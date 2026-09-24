package remoteposix

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

// A replacement directory changes the root inode while retaining the original
// identity and content. This models handle invalidation without a privileged mount.
func replaceMount(t *testing.T, d *Driver) {
	t.Helper()
	gate, err := mountGate(d.s.state, true)
	if err != nil {
		t.Fatal(err)
	}
	defer gate()
	detached := filepath.Join(t.TempDir(), "detached")
	if err = os.Rename(d.s.rootPath, detached); err != nil {
		t.Fatal(err)
	}
	if err = os.CopyFS(d.s.rootPath, os.DirFS(detached)); err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		if err = fn(); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("automatic recovery did not complete: %v", err)
}

func TestAutomaticReconnectReopensAllProviders(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	a, b := testDriver(t, root, state), testDriver(t, root, state)
	file := uploadFile(t, a, "file", "original")
	replaceMount(t, a)
	// watch=false providers must reconnect too, without waiting for a request.
	eventually(t, func() error {
		for _, d := range []*Driver{a, b} {
			leave, err := d.s.enter()
			if err != nil {
				return err
			}
			generation := d.s.generation
			leave()
			if generation == 0 {
				return errors.New("old generation still active")
			}
		}
		return nil
	})
	if got := readContent(t, a, a.ref(file.Id.OpaqueId)); got != "original" {
		t.Fatal(got)
	}
	if got := readContent(t, b, b.ref(file.Id.OpaqueId)); got != "original" {
		t.Fatal(got)
	}
	uploadFile(t, b, "after-reconnect", "new")
	if got := readContent(t, a, at(a, "after-reconnect")); got != "new" {
		t.Fatal(got)
	}
}

func TestReconnectRejectsMissingAndForeignMounts(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	d := testDriver(t, root, state)
	file := uploadFile(t, d, "file", "keep")
	detached := filepath.Join(t.TempDir(), "original")
	gate, err := mountGate(state, true)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(root, detached); err != nil {
		gate()
		t.Fatal(err)
	}
	gate()
	if _, err = d.GetMD(ownerContext(), d.ref(file.Id.OpaqueId), nil, nil); !errors.Is(err, errRemoteOffline) {
		t.Fatalf("missing mount accepted: %v", err)
	}
	if err = d.s.reopen(context.Background()); err == nil {
		t.Fatal("missing root accepted")
	}
	if err = os.MkdirAll(filepath.Join(root, control), 0700); err != nil {
		t.Fatal(err)
	}
	if err = d.s.reopen(context.Background()); err == nil {
		t.Fatal("missing identity accepted")
	}
	if err = os.WriteFile(filepath.Join(root, control, "identity"), []byte(uuid.NewString()), 0600); err != nil {
		t.Fatal(err)
	}
	if err = d.s.reopen(context.Background()); err == nil {
		t.Fatal("foreign identity accepted")
	}
	if _, err = d.CreateDir(ownerContext(), at(d, "must-not-create")); err == nil {
		t.Fatal("write accepted on foreign mount")
	}
	var nodes int
	if err = d.s.reader().QueryRow("SELECT count(*) FROM nodes").Scan(&nodes); err != nil || nodes != 2 {
		t.Fatalf("metadata lost: %d %v", nodes, err)
	}
	// Move the foreign tree aside rather than deleting anything used by a probe.
	if err = os.Rename(root, filepath.Join(t.TempDir(), "foreign")); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(detached, root); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error { _, err := d.GetMD(ownerContext(), d.ref(file.Id.OpaqueId), nil, nil); return err })
	if got := readContent(t, d, d.ref(file.Id.OpaqueId)); got != "keep" {
		t.Fatal(got)
	}
}

func TestReconnectWaitsForActiveOperationsAcrossInstances(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	a, b := testDriver(t, root, state), testDriver(t, root, state)
	uploadFile(t, a, "file", "keep")
	release, err := a.beginRefs(ownerContext(), lockRef{at(a, "file"), false})
	if err != nil {
		t.Fatal(err)
	}
	// No new mount handle may be installed while another instance owns a lease.
	if err = b.s.reopen(context.Background()); !errors.Is(err, errMountBusy) {
		release()
		t.Fatalf("active operation was not fenced: %v", err)
	}
	release()
	if err = b.s.reopen(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error { _, err := a.GetMD(ownerContext(), at(a, "file"), nil, nil); return err })
}

func TestAutomaticReconnectRecoversInterruptedPublication(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	file := uploadFile(t, d, "file", "old")
	n, err := d.s.byID(file.Id.OpaqueId)
	if err != nil {
		t.Fatal(err)
	}
	release, err := d.beginRefs(ownerContext(), lockRef{d.ref(n.ID), true})
	if err != nil {
		t.Fatal(err)
	}
	tmp := path.Join(control, "tmp", uuid.NewString())
	if err = os.WriteFile(filepath.Join(d.s.rootPath, tmp), []byte("new"), 0600); err != nil {
		release()
		t.Fatal(err)
	}
	op := operation{ID: uuid.NewString(), Kind: "replace", Source: tmp, Target: n.Path, Node: n, Time: time.Now().UnixNano()}
	op.Digest, err = d.s.digest(tmp)
	if err != nil {
		release()
		t.Fatal(err)
	}
	op.Previous, err = d.s.digest(n.Path)
	if err != nil {
		release()
		t.Fatal(err)
	}
	op.Locks, err = d.s.operationLocks(op)
	if err != nil {
		release()
		t.Fatal(err)
	}
	body, err := json.Marshal(op)
	if err != nil {
		release()
		t.Fatal(err)
	}
	if _, err = d.s.db.Exec("INSERT INTO operations VALUES (?,?)", op.ID, body); err != nil {
		release()
		t.Fatal(err)
	}
	if err = d.s.root.Rename(n.Path, path.Join(control, "versions", op.ID)); err != nil {
		release()
		t.Fatal(err)
	}
	release()
	replaceMount(t, d)
	// Poll SQLite only: recovery must happen in the monitor without an API read
	// triggering the existing request-scoped recovery mechanism.
	eventually(t, func() error {
		var count int
		if err := d.s.reader().QueryRow("SELECT count(*) FROM operations").Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errors.New("intent not recovered yet")
		}
		return nil
	})
	if got := readContent(t, d, d.ref(n.ID)); got != "new" {
		t.Fatal(got)
	}
	versions, err := d.ListRevisions(ownerContext(), d.ref(n.ID))
	if err != nil || len(versions) != 1 {
		t.Fatalf("versions=%v error=%v", versions, err)
	}
}
