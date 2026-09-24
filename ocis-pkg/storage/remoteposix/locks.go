package remoteposix

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
)

// Lock files are permanent: unlinking one could create two independent locks
// for the same resource. The kernel releases locks when a process dies.
type resourceLock struct {
	Key   string
	Write bool
}
type lockRef struct {
	Ref   *provider.Reference
	Write bool
}
type heldSessionKey struct{}

func normalizeLocks(in []resourceLock) []resourceLock {
	m := map[string]bool{}
	for _, l := range in {
		m[l.Key] = m[l.Key] || l.Write
	}
	out := make([]resourceLock, 0, len(m))
	for k, w := range m {
		out = append(out, resourceLock{k, w})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func (s *store) acquire(ctx context.Context, locks []resourceLock) (func(), error) {
	leave, err := s.enter()
	if err != nil {
		return nil, err
	}
	var files []*os.File
	release := func() {
		for i := len(files) - 1; i >= 0; i-- {
			_ = files[i].Close()
		}
		leave()
	}
	if err = os.MkdirAll(filepath.Join(s.state, "locks"), 0700); err != nil {
		release()
		return nil, err
	}
	// Bound callers without deadlines, while honoring shorter request deadlines.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for _, l := range normalizeLocks(locks) {
		f, e := os.OpenFile(filepath.Join(s.state, "locks", fmt.Sprintf("%x", sha256.Sum256([]byte(l.Key)))), os.O_CREATE|os.O_RDWR, 0600)
		if e != nil {
			release()
			return nil, e
		}
		files = append(files, f)
		for {
			if e = ctx.Err(); e != nil {
				release()
				return nil, e
			}
			ok, e := tryFileLock(f, l.Write)
			if e != nil {
				release()
				return nil, e
			}
			if ok {
				break
			}
			timer := time.NewTimer(10 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				release()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return release, nil
}

// A short read snapshot supplies a coherent topology for lock planning.
// Repeating it after acquisition prevents stale UUID/path resolution after moves.
func (d *Driver) plan(ctx context.Context, refs []lockRef) ([]resourceLock, string, error) {
	tx, err := d.s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, "", err
	}
	defer tx.Rollback()
	ids := map[string]nodeRecord{}
	lookupID := func(id string) (nodeRecord, error) {
		if n, ok := ids[id]; ok {
			return n, nil
		}
		n, e := readNode(tx.QueryRowContext(ctx, "SELECT "+nodeColumns+" FROM nodes WHERE id=?", id))
		if e == nil {
			ids[id] = n
		}
		return n, e
	}
	locks := []resourceLock{}
	stamp := []string{}
	addNode := func(n nodeRecord, w bool) error { // Include stable ancestor IDs.
		seen := map[string]bool{}
		for n.ID != "" {
			if seen[n.ID] {
				return errors.New("metadata parent cycle")
			}
			seen[n.ID] = true
			locks = append(locks, resourceLock{"node/" + n.ID, w})
			stamp = append(stamp, n.ID+":"+n.Parent+":"+n.Path)
			// Retained entries are private and can outlive their original parent.
			if strings.HasPrefix(n.Path, control+"/") {
				locks = append(locks, resourceLock{"node/" + d.s.spaceID, false})
				break
			}
			if n.Parent == "" {
				break
			}
			locks = append(locks, resourceLock{"entry/" + n.Parent + "/" + n.Name, w})
			var err error
			n, err = lookupID(n.Parent)
			if err != nil {
				return err
			}
			w = false
		}
		return nil
	}
	for _, r := range refs {
		if r.Ref == nil || r.Ref.ResourceId == nil {
			return nil, "", errors.New("reference required")
		}
		id := r.Ref.ResourceId
		if (id.StorageId != "" && id.StorageId != d.c.MountID) || (id.SpaceId != "" && id.SpaceId != d.s.spaceID) {
			return nil, "", sql.ErrNoRows
		}
		base := id.OpaqueId
		if base == "" {
			base = d.s.spaceID
		}
		n, err := lookupID(base)
		if errors.Is(err, sql.ErrNoRows) {
			locks = append(locks, resourceLock{"node/" + base, r.Write}, resourceLock{"node/" + d.s.spaceID, false})
			stamp = append(stamp, "missing:"+base)
			continue
		}
		if err != nil {
			return nil, "", err
		}
		p, err := cleanPath(r.Ref.Path)
		if err != nil {
			return nil, "", err
		}
		if p == "." {
			if err = addNode(n, r.Write || n.Dir); err != nil {
				return nil, "", err
			}
			continue
		}
		if err = addNode(n, false); err != nil {
			return nil, "", err
		}
		parts := strings.Split(p, "/")
		for i, part := range parts {
			w := r.Write && i == len(parts)-1
			locks = append(locks, resourceLock{"entry/" + n.ID + "/" + part, w})
			child, err := readNode(tx.QueryRowContext(ctx, "SELECT "+nodeColumns+" FROM nodes WHERE path=?", path.Join(n.Path, part)))
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			if err != nil {
				return nil, "", err
			}
			ids[child.ID] = child
			if err = addNode(child, w || (child.Dir && i == len(parts)-1)); err != nil {
				return nil, "", err
			}
			n = child
		}
	}
	sort.Strings(stamp)
	locks = normalizeLocks(locks)
	b, _ := json.Marshal(locks)
	return locks, string(b) + strings.Join(stamp, "\x00"), nil
}

func overlap(a, b []resourceLock) bool {
	for _, x := range a {
		for _, y := range b {
			if x.Key == y.Key && (x.Write || y.Write) {
				return true
			}
		}
	}
	return false
}

func (s *store) pending(ctx context.Context) ([]operation, error) {
	rows, err := s.reader().QueryContext(ctx, "SELECT body FROM operations ORDER BY rowid")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ops []operation
	for rows.Next() {
		var b []byte
		var op operation
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &op); err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	return ops, rows.Err()
}

func (s *store) operationLocks(op operation) ([]resourceLock, error) {
	if len(op.Locks) > 0 {
		return op.Locks, nil
	}
	d := &Driver{s: s}
	refs := []lockRef{}
	for _, p := range []string{op.Source, op.Target} {
		if p != "" && p != control && !strings.HasPrefix(p, control+"/") {
			refs = append(refs, lockRef{&provider.Reference{ResourceId: &provider.ResourceId{OpaqueId: s.spaceID}, Path: p}, true})
		}
	}
	locks, _, err := d.plan(context.Background(), refs)
	// Newly created IDs are not in nodes yet, but must share the same locks
	// with ID-based lookups once recovery publishes their metadata.
	locks = append(locks, resourceLock{"node/" + op.Node.ID, true})
	return normalizeLocks(locks), err
}

func (d *Driver) beginRefs(ctx context.Context, refs ...lockRef) (func(), error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	leave, err := d.s.enter()
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			leave()
		}
	}()
	for {
		locks, stamp, err := d.plan(ctx, refs)
		if err != nil {
			return nil, mapError(err)
		}
		release, err := d.s.acquire(ctx, locks)
		if err != nil {
			return nil, err
		}
		_, next, err := d.plan(ctx, refs)
		if err != nil {
			release()
			return nil, mapError(err)
		}
		if next != stamp {
			release()
			if err = ctx.Err(); err != nil {
				return nil, err
			}
			continue
		}
		ops, err := d.s.pending(ctx)
		var pending *operation
		if err == nil {
			for _, op := range ops {
				l, e := d.s.operationLocks(op)
				if e != nil {
					err = e
					break
				}
				if overlap(locks, l) {
					pending = &op
					break
				}
			}
		}
		if err != nil {
			release()
			return nil, err
		}
		if pending != nil {
			release()
			if err = d.s.recoverOne(ctx, *pending); err != nil {
				return nil, err
			}
			continue
		}
		if err = d.s.healthy(); err != nil {
			release()
			return nil, err
		}
		success = true
		return func() { release(); leave() }, nil
	}
}

func (d *Driver) beginSession(ctx context.Context, id string) (func(), error) {
	release, err := d.s.acquire(ctx, []resourceLock{{"session/" + id, true}})
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, heldSessionKey{}, id)
	ops, err := d.s.pending(ctx)
	if err == nil {
		for _, op := range ops {
			if op.Session == id {
				if err = d.s.recoverOne(ctx, op); err != nil {
					break
				}
			}
		}
	}
	if err != nil {
		release()
		return nil, err
	}
	return release, nil
}

func (s *store) recoverOne(ctx context.Context, op operation) error {
	if op.Session != "" && ctx.Value(heldSessionKey{}) != op.Session {
		release, err := s.acquire(ctx, []resourceLock{{"session/" + op.Session, true}})
		if err != nil {
			return err
		}
		defer release()
	}
	// Only recovery takes an operation claim. Live publishers already own all
	// resource locks; recovery waits for those and rechecks the journal afterward.
	claim, err := s.acquire(ctx, []resourceLock{{"operation/" + op.ID, true}})
	if err != nil {
		return err
	}
	defer claim()
	locks, err := s.operationLocks(op)
	if err != nil {
		return err
	}
	release, err := s.acquire(ctx, locks)
	if err != nil {
		return err
	}
	defer release()
	var body []byte
	if err = s.reader().QueryRowContext(ctx, "SELECT body FROM operations WHERE id=?", op.ID).Scan(&body); errors.Is(err, sql.ErrNoRows) {
		return nil
	} else if err != nil {
		return err
	}
	if err = json.Unmarshal(body, &op); err != nil {
		return err
	}
	return s.finish(ctx, op)
}
