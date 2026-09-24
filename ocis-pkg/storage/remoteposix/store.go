// Package remoteposix stores ordinary remote files with authoritative local SQLite metadata.
package remoteposix

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rogpeppe/go-internal/lockedfile"
)

const control = ".ocis-remoteposix"

type nodeRecord struct {
	ID, Parent, Name, Path, ETag string
	Dir                          bool
	Size, Mtime, Missing         int64
}

type store struct {
	db                       *sql.DB
	root                     *os.Root
	rootPath, state, spaceID string
	mu                       sync.Mutex
	closed                   bool
}

// Every provider instance on this host shares this lock. It covers remote I/O,
// journal recovery and SQLite commits, not just individual SQL statements.
func (s *store) lock() (func(), error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("remoteposix: store closed")
	}
	f, err := lockedfile.OpenFile(filepath.Join(s.state, "operations.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	return func() { _ = f.Close(); s.mu.Unlock() }, nil
}

func openStore(rootPath, state string) (_ *store, err error) {
	if !filepath.IsAbs(rootPath) || !filepath.IsAbs(state) {
		return nil, errors.New("root and state_dir must be absolute paths")
	}
	rootPath, err = filepath.EvalSymlinks(rootPath)
	if err != nil {
		return nil, err
	}
	// Reject obvious nesting before creating any directories on the remote tree.
	state = filepath.Clean(state)
	if r, e := filepath.Rel(rootPath, state); e == nil && (r == "." || (r != ".." && !strings.HasPrefix(r, ".."+string(filepath.Separator)))) {
		return nil, errors.New("state_dir must be outside the remote root")
	}
	if err = os.MkdirAll(state, 0700); err != nil {
		return nil, err
	}
	state, err = filepath.EvalSymlinks(state)
	if err != nil {
		return nil, err
	}
	rel, err := filepath.Rel(rootPath, state)
	if err != nil || rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
		return nil, errors.New("state_dir must be outside the remote root")
	}
	s := &store{rootPath: rootPath, state: state}
	unlock, err := s.lock()
	if err != nil {
		return nil, err
	}
	defer unlock()
	s.root, err = os.OpenRoot(rootPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = s.root.Close()
			if s.db != nil {
				_ = s.db.Close()
			}
		}
	}()
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(filepath.Join(state, "metadata.sqlite"))}
	s.db, err = sql.Open("sqlite3", u.String()+"?_journal_mode=WAL&_synchronous=FULL&_foreign_keys=on&_busy_timeout=10000")
	if err != nil {
		return nil, err
	}
	s.db.SetMaxOpenConns(1)
	_, err = s.db.Exec(`
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS nodes (id TEXT PRIMARY KEY, parent TEXT NOT NULL, name TEXT NOT NULL, path TEXT UNIQUE NOT NULL, dir INTEGER NOT NULL, size INTEGER NOT NULL, mtime INTEGER NOT NULL, etag TEXT NOT NULL, missing INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS node_parent ON nodes(parent);
CREATE TABLE IF NOT EXISTS attributes (node TEXT NOT NULL, key TEXT NOT NULL, value BLOB NOT NULL, PRIMARY KEY(node,key), FOREIGN KEY(node) REFERENCES nodes(id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, body BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS retained (id TEXT PRIMARY KEY, node TEXT NOT NULL, path TEXT NOT NULL, kind TEXT NOT NULL, size INTEGER NOT NULL, time INTEGER NOT NULL, snapshot BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, node TEXT NOT NULL, kind TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS uploads (id TEXT PRIMARY KEY, body BLOB NOT NULL);
`)
	if err != nil {
		return nil, err
	}
	var version string
	err = s.db.QueryRow("SELECT value FROM settings WHERE key='schema'").Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		// A marker binds this database to this mount, so an unavailable mount
		// cannot be mistaken for an empty directory. It contains no file metadata.
		if _, e := s.root.Stat(control); !errors.Is(e, fs.ErrNotExist) {
			return nil, errors.New("remote control directory already exists; restore its original metadata database")
		}
		s.spaceID = uuid.NewString()
		if err = s.root.Mkdir(control, 0700); err != nil {
			return nil, err
		}
		for _, d := range []string{"tmp", "trash", "versions"} {
			if err = s.root.Mkdir(path.Join(control, d), 0700); err != nil {
				return nil, err
			}
		}
		f, e := s.root.OpenFile(path.Join(control, "identity"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return nil, e
		}
		_, err = f.WriteString(s.spaceID)
		if err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
		tx, e := s.db.Begin()
		if e != nil {
			return nil, e
		}
		defer tx.Rollback()
		_, err = tx.Exec("INSERT INTO settings(key,value) VALUES ('schema','1'),('space',?),('root',?)", s.spaceID, rootPath)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec("INSERT INTO nodes VALUES (?,?,?,?,?,?,?,?,0)", s.spaceID, "", "", ".", true, 0, time.Now().UnixNano(), uuid.NewString())
		if err == nil {
			err = s.syncDirs(control, ".")
		}
		if err == nil {
			err = tx.Commit()
		}
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else {
		if version != "1" {
			return nil, fmt.Errorf("unsupported metadata schema %q", version)
		}
		if err = s.db.QueryRow("SELECT value FROM settings WHERE key='space'").Scan(&s.spaceID); err != nil {
			return nil, err
		}
		var boundRoot string
		if err = s.db.QueryRow("SELECT value FROM settings WHERE key='root'").Scan(&boundRoot); err != nil {
			return nil, err
		}
		if boundRoot != rootPath {
			return nil, errors.New("database is bound to another root")
		}
	}
	if err = s.healthy(); err != nil {
		return nil, err
	}
	if err = s.recover(context.Background()); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *store) healthy() error {
	// Open the configured pathname anew, rather than only checking the pinned
	// root handle: unmounts must not leave us serving a detached filesystem.
	r, err := os.OpenRoot(s.rootPath)
	if err != nil {
		return err
	}
	defer r.Close()
	a, err := r.Stat(".")
	if err != nil {
		return err
	}
	b, err := s.root.Stat(".")
	if err != nil {
		return err
	}
	if !os.SameFile(a, b) {
		return errors.New("remote root changed; reconciliation suspended")
	}
	f, err := r.Open(path.Join(control, "identity"))
	if err != nil {
		return fmt.Errorf("remote mount identity unavailable: %w", err)
	}
	defer f.Close()
	identity, err := io.ReadAll(io.LimitReader(f, 128))
	if err != nil {
		return err
	}
	if string(identity) != s.spaceID {
		return errors.New("remote mount identity mismatch")
	}
	return nil
}

func (s *store) syncDirs(names ...string) error {
	for _, name := range names {
		f, err := s.root.Open(name)
		if err != nil {
			return err
		}
		err = f.Sync()
		closeErr := f.Close()
		if err != nil {
			return fmt.Errorf("remote directory sync required for recovery: %w", err)
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func cleanPath(p string) (string, error) {
	if strings.ContainsAny(p, "\\\x00") {
		return "", errors.New("invalid path")
	}
	for _, part := range strings.Split(p, "/") {
		if part == ".." || part == control {
			return "", errors.New("reserved or escaping path")
		}
	}
	p = path.Clean(strings.TrimPrefix(p, "/"))
	if p == "" {
		p = "."
	}
	return p, nil
}

func (s *store) safe(p string) error {
	// os.Root confines all operations even when an external writer races a
	// symlink replacement. Reject symlinks within the tree as well.
	cur := "."
	for _, part := range strings.Split(p, "/") {
		cur = path.Join(cur, part)
		fi, err := s.root.Lstat(cur)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return errors.New("symlinks are not supported")
		}
		if !fi.IsDir() && !fi.Mode().IsRegular() {
			return errors.New("special files are not supported")
		}
	}
	return nil
}

type scanner interface{ Scan(...any) error }

func readNode(r scanner) (n nodeRecord, err error) {
	err = r.Scan(&n.ID, &n.Parent, &n.Name, &n.Path, &n.Dir, &n.Size, &n.Mtime, &n.ETag, &n.Missing)
	return
}

const nodeColumns = "id,parent,name,path,dir,size,mtime,etag,missing"

func (s *store) byPath(p string) (nodeRecord, error) {
	return readNode(s.db.QueryRow("SELECT "+nodeColumns+" FROM nodes WHERE path=?", p))
}
func (s *store) byID(id string) (nodeRecord, error) {
	return readNode(s.db.QueryRow("SELECT "+nodeColumns+" FROM nodes WHERE id=?", id))
}

func (s *store) records() ([]nodeRecord, error) {
	rows, err := s.db.Query("SELECT " + nodeColumns + " FROM nodes ORDER BY length(path),path")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []nodeRecord
	for rows.Next() {
		n, e := readNode(rows)
		if e != nil {
			return nil, e
		}
		result = append(result, n)
	}
	return result, rows.Err()
}

// scan commits only after a complete successful walk and a second mount check.
// Missing entries are retained through a grace period. A large disappearance
// suspends the entire scan instead of deleting an uncertain subtree.
func (s *store) scan(ctx context.Context, grace time.Duration) error {
	if err := s.healthy(); err != nil {
		return err
	}
	old, err := s.records()
	if err != nil {
		return err
	}
	public := old[:0]
	for _, n := range old {
		if n.Path != control && !strings.HasPrefix(n.Path, control+"/") {
			public = append(public, n)
		}
	}
	old = public
	byPath := map[string]nodeRecord{}
	for _, n := range old {
		byPath[n.Path] = n
	}
	found := map[string]nodeRecord{}
	var ordered []nodeRecord
	err = fs.WalkDir(s.root.FS(), ".", func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		if p == control {
			return fs.SkipDir
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink at %s; scan suspended", p)
		}
		fi, e := d.Info()
		if e != nil {
			return e
		}
		if !fi.IsDir() && !fi.Mode().IsRegular() {
			return fmt.Errorf("unsupported entry %s", p)
		}
		n, ok := byPath[p]
		if ok && n.Dir != fi.IsDir() {
			return fmt.Errorf("entry type changed at %s; reconciliation required", p)
		}
		if !ok || n.Missing != 0 {
			n = nodeRecord{ID: uuid.NewString(), Path: p, Name: path.Base(p), Dir: fi.IsDir(), ETag: uuid.NewString()}
		}
		if p != "." {
			n.Parent = found[path.Dir(p)].ID
		}
		size := fi.Size()
		if fi.IsDir() {
			size = 0
		}
		if n.Size != size || n.Mtime != fi.ModTime().UnixNano() {
			n.ETag = uuid.NewString()
		}
		n.Size = size
		n.Mtime = fi.ModTime().UnixNano()
		n.Missing = 0
		found[p] = n
		ordered = append(ordered, n)
		return nil
	})
	if err != nil {
		return err
	}
	if err = s.healthy(); err != nil {
		return err
	}
	missing := 0
	for _, n := range old {
		if _, ok := found[n.Path]; !ok {
			missing++
		}
	}
	if missing > 10 && missing*4 > len(old) {
		return errors.New("more than 25 percent of the tree disappeared; scan suspended")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	changed := false
	for _, n := range ordered {
		prev, ok := byPath[n.Path]
		if !ok || prev.ETag != n.ETag || prev.Missing != 0 {
			changed = true
			if ok && prev.ID != n.ID {
				if _, err = tx.ExecContext(ctx, "DELETE FROM nodes WHERE id=?", prev.ID); err != nil {
					return err
				}
			}
			_, err = tx.ExecContext(ctx, "INSERT INTO nodes VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET parent=excluded.parent,name=excluded.name,size=excluded.size,mtime=excluded.mtime,etag=excluded.etag,missing=0", n.ID, n.Parent, n.Name, n.Path, n.Dir, n.Size, n.Mtime, n.ETag, n.Missing)
			if err != nil {
				return err
			}
			kind := "file"
			if n.Dir {
				kind = "directory"
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO outbox(node,kind) VALUES (?,?)", n.ID, kind); err != nil {
				return err
			}
		}
	}
	now := time.Now().UnixNano()
	for _, n := range old {
		if _, ok := found[n.Path]; ok {
			continue
		}
		changed = true
		if n.Missing == 0 {
			_, err = tx.ExecContext(ctx, "UPDATE nodes SET missing=? WHERE id=?", now, n.ID)
		} else if time.Duration(now-n.Missing) >= grace {
			_, err = tx.ExecContext(ctx, "DELETE FROM nodes WHERE id=?", n.ID)
			if err == nil {
				_, err = tx.ExecContext(ctx, "INSERT INTO outbox(node,kind) VALUES (?,'deleted')", n.ID)
			}
		}
		if err != nil {
			return err
		}
	}
	if changed {
		if err = refreshDirectories(tx); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Directory ETags change conservatively on any committed tree change. This
// intentionally favors correctness over minimal sync invalidation in v1.
func refreshDirectories(tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE nodes SET etag=? WHERE dir=1", uuid.NewString())
	return err
}

type operation struct {
	ID, Kind, Source, Target, Digest, Previous string
	Session                                    string
	Node                                       nodeRecord
	Snapshot                                   []nodeRecord
	Time                                       int64
}

func (s *store) digest(p string) (string, error) {
	h := sha256.New()
	err := fs.WalkDir(s.root.FS(), p, func(name string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errors.New("symlink in operation")
		}
		fmt.Fprintf(h, "%d:%s:%t\n", len(strings.TrimPrefix(name, p)), strings.TrimPrefix(name, p), d.IsDir())
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return errors.New("special file in operation")
		}
		f, e := s.root.Open(name)
		if e != nil {
			return e
		}
		fi, e := f.Stat()
		if e != nil {
			f.Close()
			return e
		}
		fmt.Fprintf(h, "%d:", fi.Size())
		count, e := io.Copy(h, f)
		after, statErr := f.Stat()
		ce := f.Close()
		if e != nil {
			return e
		}
		if statErr != nil {
			return statErr
		}
		if count != fi.Size() || after.Size() != fi.Size() || !after.ModTime().Equal(fi.ModTime()) {
			return errors.New("content changed while fingerprinting")
		}
		return ce
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (s *store) apply(ctx context.Context, op operation) error {
	if err := s.healthy(); err != nil {
		return err
	}
	if err := s.safe(op.Source); err != nil {
		return err
	}
	if err := s.safe(op.Target); err != nil {
		return err
	}
	if _, err := s.root.Lstat(op.Target); err == nil && op.Kind != "replace" {
		return fmt.Errorf("destination exists: %s", op.Target)
	} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	var err error
	op.Digest, err = s.digest(op.Source)
	if err != nil {
		return err
	}
	if op.Kind == "replace" {
		op.Previous, err = s.digest(op.Target)
		if err != nil {
			return err
		}
	}
	if err = s.syncDirs(path.Dir(op.Source)); err != nil {
		return err
	}
	op.Time = time.Now().UnixNano()
	body, err := json.Marshal(op)
	if err != nil {
		return err
	}
	if _, err = s.db.ExecContext(ctx, "INSERT INTO operations VALUES (?,?)", op.ID, body); err != nil {
		return err
	}
	// After the journal is durable, recovery owns the operation even when the
	// request is canceled. Never delete the journal on an uncertain I/O error.
	return s.finish(context.Background(), op)
}

func (s *store) finish(ctx context.Context, op operation) error {
	if err := s.healthy(); err != nil {
		return err
	}
	if err := s.safe(op.Source); err != nil {
		return err
	}
	if err := s.safe(op.Target); err != nil {
		return err
	}
	if op.Kind == "replace" {
		backup := path.Join(control, "versions", op.ID)
		if _, e := s.root.Lstat(backup); errors.Is(e, fs.ErrNotExist) {
			h, e := s.digest(op.Target)
			if e != nil {
				return e
			}
			if h != op.Previous {
				return errors.New("replacement target changed; recovery stopped")
			}
			if e = s.root.Rename(op.Target, backup); e != nil {
				return e
			}
		} else if e != nil {
			return e
		}
		h, e := s.digest(backup)
		if e != nil {
			return e
		}
		if h != op.Previous {
			return errors.New("retained version changed; recovery stopped")
		}
	}
	_, srcErr := s.root.Lstat(op.Source)
	_, dstErr := s.root.Lstat(op.Target)
	if srcErr == nil && errors.Is(dstErr, fs.ErrNotExist) {
		h, err := s.digest(op.Source)
		if err != nil {
			return err
		}
		if h != op.Digest {
			return errors.New("pending operation source changed; recovery stopped")
		}
		if err = s.root.Rename(op.Source, op.Target); err != nil {
			return err
		}
	} else if !errors.Is(srcErr, fs.ErrNotExist) || dstErr != nil {
		return fmt.Errorf("ambiguous operation %s; recovery stopped", op.ID)
	}
	h, err := s.digest(op.Target)
	if err != nil {
		return err
	}
	if h != op.Digest {
		return errors.New("pending operation destination changed; recovery stopped")
	}
	if err = s.syncDirs(path.Dir(op.Source), path.Dir(op.Target), path.Join(control, "versions")); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch op.Kind {
	case "replace":
		fi, e := s.root.Stat(op.Target)
		if e != nil {
			return e
		}
		if _, err = tx.Exec("UPDATE nodes SET size=?,mtime=?,etag=?,missing=0 WHERE id=?", fi.Size(), fi.ModTime().UnixNano(), uuid.NewString(), op.Node.ID); err != nil {
			return err
		}
		body, e := json.Marshal(op.Node)
		if e != nil {
			return e
		}
		if _, err = tx.Exec("INSERT INTO retained VALUES (?,?,?,?,?,?,?)", op.ID, op.Node.ID, op.Node.Path, "version", op.Node.Size, op.Time, body); err != nil {
			return err
		}
	case "create", "move", "restore":
		if op.Kind == "restore" {
			for _, n := range op.Snapshot {
				n.Path = op.Target + strings.TrimPrefix(n.Path, op.Node.Path)
				if n.ID == op.Node.ID {
					n.Parent = op.Node.Parent
					n.Name = op.Node.Name
				}
				if _, err = tx.Exec("UPDATE nodes SET parent=?,name=?,path=?,etag=?,missing=0 WHERE id=?", n.Parent, n.Name, n.Path, uuid.NewString(), n.ID); err != nil {
					return err
				}
			}
			if _, err = tx.Exec("DELETE FROM retained WHERE id=?", op.ID); err != nil {
				return err
			}
		} else if op.Kind == "move" {
			// A prefix comparison (not LIKE) preserves names containing % or _.
			if _, err = tx.Exec("UPDATE nodes SET path=? || substr(path,?) WHERE path=? OR substr(path,1,?)=?", op.Target, utf8.RuneCountInString(op.Source)+1, op.Source, utf8.RuneCountInString(op.Source)+1, op.Source+"/"); err != nil {
				return err
			}
			if _, err = tx.Exec("UPDATE nodes SET parent=?,name=?,etag=? WHERE id=?", op.Node.Parent, op.Node.Name, uuid.NewString(), op.Node.ID); err != nil {
				return err
			}
		} else {
			fi, e := s.root.Stat(op.Target)
			if e != nil {
				return e
			}
			n := op.Node
			if _, err = tx.Exec("INSERT INTO nodes VALUES (?,?,?,?,?,?,?,?,0)", n.ID, n.Parent, n.Name, op.Target, n.Dir, fi.Size(), fi.ModTime().UnixNano(), uuid.NewString()); err != nil {
				return err
			}
		}
	case "trash":
		body, e := json.Marshal(op.Snapshot)
		if e != nil {
			return e
		}
		if _, err = tx.Exec("INSERT INTO retained VALUES (?,?,?,?,?,?,?)", op.ID, op.Node.ID, op.Node.Path, "trash", op.Node.Size, op.Time, body); err != nil {
			return err
		}
		// Keep metadata for trash entries by moving their paths into the reserved
		// namespace. Public lookups never expose these paths.
		if _, err = tx.Exec("UPDATE nodes SET path=? || substr(path,?) WHERE path=? OR substr(path,1,?)=?", op.Target, utf8.RuneCountInString(op.Source)+1, op.Source, utf8.RuneCountInString(op.Source)+1, op.Source+"/"); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown operation kind %q", op.Kind)
	}
	if err = refreshDirectories(tx); err != nil {
		return err
	}
	if op.Session != "" {
		var b []byte
		var upload session
		if err = tx.QueryRow("SELECT body FROM uploads WHERE id=?", op.Session).Scan(&b); err != nil {
			return err
		}
		if err = json.Unmarshal(b, &upload); err != nil {
			return err
		}
		upload.Result = op.Node.ID
		b, err = json.Marshal(upload)
		if err != nil {
			return err
		}
		if _, err = tx.Exec("UPDATE uploads SET body=? WHERE id=?", b, op.Session); err != nil {
			return err
		}
	}
	kind := "file"
	if op.Node.Dir {
		kind = "directory"
	}
	if op.Kind == "trash" {
		kind = "deleted"
	}
	if _, err = tx.Exec("INSERT INTO outbox(node,kind) VALUES (?,?)", op.Node.ID, kind); err != nil {
		return err
	}
	if _, err = tx.Exec("DELETE FROM operations WHERE id=?", op.ID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *store) recover(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "SELECT body FROM operations ORDER BY rowid")
	if err != nil {
		return err
	}
	var ops []operation
	for rows.Next() {
		var b []byte
		var op operation
		if err = rows.Scan(&b); err != nil {
			break
		}
		if err = json.Unmarshal(b, &op); err != nil {
			break
		}
		ops = append(ops, op)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	for _, op := range ops {
		if err = s.finish(ctx, op); err != nil {
			return err
		}
	}
	return nil
}

func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return errors.Join(s.db.Close(), s.root.Close())
}
