package remoteposix

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/google/uuid"
	"github.com/owncloud/reva/v2/pkg/storage"
)

type gatedReader struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	reader  io.Reader
}

func (r *gatedReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered); <-r.release })
	return r.reader.Read(p)
}
func waitResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("operation blocked by unrelated work")
		return nil
	}
}

func TestSlowChunkDoesNotBlockOtherSessionsOrReads(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	a := testDriver(t, root, state)
	b := testDriver(t, root, state)
	existing := uploadFile(t, a, "existing", "old")
	ids, err := a.InitiateUpload(ownerContext(), at(a, "slow"), 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	u, err := a.GetUpload(ownerContext(), ids["tus"])
	if err != nil {
		t.Fatal(err)
	}
	gate := &gatedReader{entered: make(chan struct{}), release: make(chan struct{}), reader: strings.NewReader("new")}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	defer release()
	done := make(chan error, 1)
	go func() { _, err := u.WriteChunk(ownerContext(), 0, gate); done <- err }()
	<-gate.entered
	other := make(chan error, 1)
	go func() {
		_, r, e := b.Download(ownerContext(), b.ref(existing.Id.OpaqueId), nil)
		if e == nil {
			_, e = io.ReadAll(r)
			r.Close()
		}
		if e == nil {
			_, e = b.CreateDir(ownerContext(), at(b, "parallel"))
		}
		if e == nil {
			var ids map[string]string
			ids, e = b.InitiateUpload(ownerContext(), at(b, "fast"), 4, nil)
			if e == nil {
				_, e = b.Upload(ownerContext(), storage.UploadRequest{Ref: &provider.Reference{Path: ids["simple"]}, Body: io.NopCloser(strings.NewReader("fast")), Length: 4}, nil)
			}
		}
		other <- e
	}()
	if err = waitResult(t, other); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ownerContext(), 80*time.Millisecond)
	defer cancel()
	if _, err = u.WriteChunk(ctx, 0, strings.NewReader("bad")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("same session must wait: %v", err)
	}
	release()
	if err = waitResult(t, done); err != nil {
		t.Fatal(err)
	}
	if err = u.FinishUpload(ownerContext()); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteStagingAllowsParentMoveAndRevalidates(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	a := testDriver(t, root, state)
	b := testDriver(t, root, state)
	dir, err := a.CreateDir(ownerContext(), at(a, "before"))
	if err != nil {
		t.Fatal(err)
	}
	ids, err := a.InitiateUpload(ownerContext(), at(a, "before/file"), 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := a.s.byID(dir.ResourceID.OpaqueId)
	if err != nil {
		t.Fatal(err)
	}
	gate := &gatedReader{entered: make(chan struct{}), release: make(chan struct{}), reader: strings.NewReader("new")}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	defer release()
	done := make(chan error, 1)
	go func() {
		ctx := context.WithValue(ownerContext(), heldSessionKey{}, ids["tus"])
		unlock, e := a.beginSession(ctx, ids["tus"])
		if e == nil {
			_, e = a.write(ctx, parent, "file", gate, 3, "", "", ids["tus"])
			unlock()
		}
		done <- e
	}()
	<-gate.entered
	moved := make(chan error, 1)
	go func() { _, e := b.Move(ownerContext(), at(b, "before"), at(b, "after")); moved <- e }()
	if err = waitResult(t, moved); err != nil {
		t.Fatal(err)
	}
	release()
	if err = waitResult(t, done); err != nil {
		t.Fatal(err)
	}
	if got := readContent(t, b, at(b, "after/file")); got != "new" {
		t.Fatal(got)
	}
}

func TestSharedReadersAndUnrelatedWriters(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	a := testDriver(t, root, state)
	b := testDriver(t, root, state)
	ri := uploadFile(t, a, "file", "old")
	release, err := a.beginRefs(ownerContext(), lockRef{a.ref(ri.Id.OpaqueId), false})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	done := make(chan error, 1)
	go func() {
		_, e := b.GetMD(ownerContext(), b.ref(ri.Id.OpaqueId), nil, nil)
		if e == nil {
			_, e = b.CreateDir(ownerContext(), at(b, "other"))
		}
		done <- e
	}()
	if err = waitResult(t, done); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ownerContext(), 80*time.Millisecond)
	defer cancel()
	if _, err = b.Delete(ctx, b.ref(ri.Id.OpaqueId)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("writer bypassed reader: %v", err)
	}
}

func TestOpposingDirectoryMovesDoNotDeadlock(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	a := testDriver(t, root, state)
	b := testDriver(t, root, state)
	for _, name := range []string{"a", "b"} {
		if _, err := a.CreateDir(ownerContext(), at(a, name)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(ownerContext(), 3*time.Second)
	defer cancel()
	start := make(chan struct{})
	done := make(chan error, 2)
	go func() { <-start; _, e := a.Move(ctx, at(a, "a"), at(a, "b/a")); done <- e }()
	go func() { <-start; _, e := b.Move(ctx, at(b, "b"), at(b, "a/b")); done <- e }()
	close(start)
	successes := 0
	for i := 0; i < 2; i++ {
		err := waitResult(t, done)
		if err == nil {
			successes++
		}
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("lock-order deadlock", err)
		}
	}
	if successes != 1 {
		t.Fatalf("expected one valid move, got %d", successes)
	}
}

// This helper is killed while holding real process-wide resource/session locks.
// It simulates the three durable filesystem states of replacement publication.
func TestPublicationCrashHelper(t *testing.T) {
	stage := os.Getenv("REMOTEPOSIX_CRASH_STAGE")
	if stage == "" {
		t.Skip("subprocess helper")
	}
	d := testDriver(t, os.Getenv("REMOTEPOSIX_ROOT"), os.Getenv("REMOTEPOSIX_STATE"))
	ctx := ownerContext()
	sid := os.Getenv("REMOTEPOSIX_SESSION")
	unlock, err := d.beginSession(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	release, err := d.beginRefs(ctx, lockRef{at(d, "victim"), true})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	n, err := d.s.byPath("victim")
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := d.prepare(ctx, strings.NewReader("new contents"), 12, sid)
	if err != nil {
		t.Fatal(err)
	}
	op := operation{ID: uuid.NewString(), Kind: "replace", Source: tmp, Target: n.Path, Node: n, Session: sid, Time: time.Now().UnixNano()}
	op.Digest, err = d.s.digest(tmp)
	if err != nil {
		t.Fatal(err)
	}
	op.Previous, err = d.s.digest(n.Path)
	if err != nil {
		t.Fatal(err)
	}
	op.Locks, err = d.s.operationLocks(op)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.s.syncDirs(path.Dir(tmp)); err != nil {
		t.Fatal(err)
	}
	if _, err = d.s.db.Exec("INSERT INTO operations VALUES (?,?)", op.ID, body); err != nil {
		t.Fatal(err)
	}
	if stage != "intent" {
		if err = d.s.root.Rename(n.Path, path.Join(control, "versions", op.ID)); err != nil {
			t.Fatal(err)
		}
	}
	if stage == "installed" {
		if err = d.s.root.Rename(tmp, n.Path); err != nil {
			t.Fatal(err)
		}
	}
	if err = d.s.syncDirs(".", path.Join(control, "tmp"), path.Join(control, "versions")); err != nil {
		t.Fatal(err)
	}
	fmt.Println("READY")
	for {
		time.Sleep(time.Hour)
	}
}

func TestProcessCrashRecoveryAndAtomicSessionCommit(t *testing.T) {
	for _, stage := range []string{"intent", "backup", "installed"} {
		t.Run(stage, func(t *testing.T) {
			root, state := t.TempDir(), t.TempDir()
			a := testDriver(t, root, state)
			victim := uploadFile(t, a, "victim", "old")
			other := uploadFile(t, a, "other", "independent")
			ids, err := a.InitiateUpload(ownerContext(), at(a, "victim"), 12, nil)
			if err != nil {
				t.Fatal(err)
			}
			u, err := a.GetUpload(ownerContext(), ids["tus"])
			if err != nil {
				t.Fatal(err)
			}
			if _, err = u.WriteChunk(ownerContext(), 0, strings.NewReader("new contents")); err != nil {
				t.Fatal(err)
			}
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(executable, "-test.run=^TestPublicationCrashHelper$")
			cmd.Env = append(os.Environ(), "REMOTEPOSIX_CRASH_STAGE="+stage, "REMOTEPOSIX_ROOT="+root, "REMOTEPOSIX_STATE="+state, "REMOTEPOSIX_SESSION="+ids["tus"])
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			cmd.Stderr = os.Stderr
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
			ready := make(chan error, 1)
			go func() {
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					if scanner.Text() == "READY" {
						ready <- nil
						return
					}
				}
				ready <- fmt.Errorf("helper exited before publication: %v", scanner.Err())
			}()
			if err = waitResult(t, ready); err != nil {
				t.Fatal(err)
			}
			// Startup with a pending operation is allowed, as is access to other files.
			b := testDriver(t, root, state)
			if got := readContent(t, b, b.ref(other.Id.OpaqueId)); got != "independent" {
				t.Fatal(got)
			}
			if _, err = b.CreateDir(ownerContext(), at(b, "available")); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(ownerContext(), 80*time.Millisecond)
			_, _, err = b.Download(ctx, b.ref(victim.Id.OpaqueId), nil)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("reader bypassed publishing process: %v", err)
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			if got := readContent(t, b, b.ref(victim.Id.OpaqueId)); got != "new contents" {
				t.Fatal(got)
			}
			s, err := b.session(ownerContext(), ids["tus"])
			if err != nil || s.Result != victim.Id.OpaqueId {
				t.Fatalf("session did not commit with metadata: %+v %v", s, err)
			}
			if err = u.FinishUpload(ownerContext()); err != nil {
				t.Fatalf("lost acknowledgement retry: %v", err)
			}
			versions, err := b.ListRevisions(ownerContext(), b.ref(victim.Id.OpaqueId))
			if err != nil || len(versions) != 1 {
				t.Fatalf("duplicate/missing version: %v %v", versions, err)
			}
			var pending, receipts int
			if err = b.s.db.QueryRow("SELECT count(*) FROM operations").Scan(&pending); err != nil {
				t.Fatal(err)
			}
			if err = b.s.db.QueryRow("SELECT count(*) FROM completed_operations WHERE node=? AND kind='replace'", victim.Id.OpaqueId).Scan(&receipts); err != nil {
				t.Fatal(err)
			}
			if pending != 0 || receipts != 1 {
				t.Fatalf("pending=%d receipts=%d", pending, receipts)
			}
		})
	}
}

func TestAmbiguousOperationDoesNotBlockIndependentFiles(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	uploadFile(t, d, "good", "readable")
	op := operation{ID: uuid.NewString(), Kind: "move", Source: "missing", Target: "ambiguous", Digest: "invalid", Node: nodeRecord{ID: uuid.NewString()}}
	op.Locks, _ = d.s.operationLocks(op)
	body, _ := json.Marshal(op)
	if _, err := d.s.db.Exec("INSERT INTO operations VALUES (?,?)", op.ID, body); err != nil {
		t.Fatal(err)
	}
	if got := readContent(t, d, at(d, "good")); got != "readable" {
		t.Fatal(got)
	}
	if _, err := d.GetMD(ownerContext(), at(d, "ambiguous"), nil, nil); err == nil {
		t.Fatal("ambiguous resource was served")
	}
	if _, err := d.CreateDir(ownerContext(), at(d, "still-available")); err != nil {
		t.Fatal(err)
	}
}

func TestDirectoryETagsOnlyInvalidateAncestors(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	for _, p := range []string{"a", "b"} {
		if _, err := d.CreateDir(ownerContext(), at(d, p)); err != nil {
			t.Fatal(err)
		}
	}
	before, err := d.GetMD(ownerContext(), at(d, "b"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	uploadFile(t, d, "a/file", "data")
	after, err := d.GetMD(ownerContext(), at(d, "b"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if before.Etag != after.Etag {
		t.Fatal("unrelated directory invalidated")
	}
}

func TestOpenDownloadKeepsOldVersionDuringReplacement(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	ri := uploadFile(t, d, "file", "old")
	md, reader, err := d.Download(ownerContext(), d.ref(ri.Id.OpaqueId), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	newer := uploadFile(t, d, "file", "new contents")
	old, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(old) != "old" || md.Size != 3 || md.Etag == newer.Etag {
		t.Fatal("download mixed generations")
	}
	if got := readContent(t, d, &provider.Reference{ResourceId: newer.Id}); got != "new contents" {
		t.Fatal(got)
	}
}

func TestStaleScanCannotOverwritePublication(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	ri := uploadFile(t, d, "file", "old")
	var epoch int64
	if err := d.s.reader().QueryRow("SELECT value FROM settings WHERE key='epoch'").Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	old, err := d.s.records()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]nodeRecord{}
	for _, n := range old {
		found[n.Path] = n
	}
	newer := uploadFile(t, d, "file", "new contents")
	// These observations were collected before the concurrent publication.
	if err = d.s.commitScan(ownerContext(), time.Minute, epoch, old, old, found); err != nil {
		t.Fatal(err)
	}
	after, err := d.GetMD(ownerContext(), d.ref(ri.Id.OpaqueId), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if after.Etag != newer.Etag || after.Size != newer.Size {
		t.Fatal("stale scan replaced committed metadata")
	}
	if got := readContent(t, d, d.ref(ri.Id.OpaqueId)); got != "new contents" {
		t.Fatal(got)
	}
}

func TestSameFileWritersCommitOneRevision(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	a := testDriver(t, root, state)
	b := testDriver(t, root, state)
	ri := uploadFile(t, a, "file", "old")
	idsA, err := a.InitiateUpload(ownerContext(), at(a, "file"), 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	idsB, err := b.InitiateUpload(ownerContext(), at(b, "file"), 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	ua, err := a.GetUpload(ownerContext(), idsA["tus"])
	if err != nil {
		t.Fatal(err)
	}
	ub, err := b.GetUpload(ownerContext(), idsB["tus"])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ua.WriteChunk(ownerContext(), 0, strings.NewReader("aaa")); err != nil {
		t.Fatal(err)
	}
	if _, err = ub.WriteChunk(ownerContext(), 0, strings.NewReader("bbb")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 2)
	start := make(chan struct{})
	go func() { <-start; done <- ua.FinishUpload(ownerContext()) }()
	go func() { <-start; done <- ub.FinishUpload(ownerContext()) }()
	close(start)
	successes := 0
	for i := 0; i < 2; i++ {
		if err = waitResult(t, done); err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("expected one revision winner, got %d", successes)
	}
	versions, err := a.ListRevisions(ownerContext(), a.ref(ri.Id.OpaqueId))
	if err != nil || len(versions) != 1 {
		t.Fatalf("versions: %v %v", versions, err)
	}
}

func TestSimpleUploadRetryAfterCommitDoesNotReplaceAgain(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	ids, err := d.InitiateUpload(ownerContext(), at(d, "file"), 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	req := func(body string) storage.UploadRequest {
		return storage.UploadRequest{Ref: &provider.Reference{Path: ids["simple"]}, Body: io.NopCloser(strings.NewReader(body)), Length: 3}
	}
	first, err := d.Upload(ownerContext(), req("old"), nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := d.Upload(ownerContext(), req("new"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.Etag != second.Etag || first.Id.OpaqueId != second.Id.OpaqueId {
		t.Fatal("retry was not idempotent")
	}
	if got := readContent(t, d, at(d, "file")); got != "old" {
		t.Fatal(got)
	}
}

func TestWALReadersProceedDuringWriterTransaction(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	ri := uploadFile(t, d, "file", "data")
	tx, err := d.s.db.BeginTx(ownerContext(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec("INSERT INTO settings VALUES ('uncommitted-test','value')"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, e := d.GetMD(ownerContext(), d.ref(ri.Id.OpaqueId), nil, nil); done <- e }()
	if err = waitResult(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedItemOutlivesPurgedParent(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	ctx := ownerContext()
	if _, err := d.CreateDir(ctx, at(d, "parent")); err != nil {
		t.Fatal(err)
	}
	file := uploadFile(t, d, "parent/file", "data")
	if _, err := d.Delete(ctx, d.ref(file.Id.OpaqueId)); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Delete(ctx, at(d, "parent")); err != nil {
		t.Fatal(err)
	}
	entries, err := d.retention("trash", "")
	if err != nil {
		t.Fatal(err)
	}
	var fileKey string
	for _, r := range entries {
		if r.Node == file.Id.OpaqueId {
			fileKey = r.ID
			continue
		}
		if err = d.PurgeRecycleItem(ctx, d.ref(d.s.spaceID), r.ID, ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = d.RestoreRecycleItem(ctx, d.ref(d.s.spaceID), fileKey, "", at(d, "restored")); err != nil {
		t.Fatal(err)
	}
	if got := readContent(t, d, at(d, "restored")); got != "data" {
		t.Fatal(got)
	}
}
