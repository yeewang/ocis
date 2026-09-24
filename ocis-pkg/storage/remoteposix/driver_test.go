package remoteposix

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/google/uuid"
	ctxpkg "github.com/owncloud/reva/v2/pkg/ctx"
	tusmanager "github.com/owncloud/reva/v2/pkg/rhttp/datatx/manager/tus"
	"github.com/owncloud/reva/v2/pkg/storage"
	"github.com/rs/zerolog"
)

func ownerContext() context.Context {
	return ctxpkg.ContextSetUser(context.Background(), &userpb.User{Id: &userpb.UserId{OpaqueId: "owner", Idp: "test"}})
}
func testDriver(t *testing.T, root, state string) *Driver {
	t.Helper()
	f, e := New(map[string]interface{}{"root": root, "state_dir": state, "owner_id": "owner", "owner_idp": "test", "mount_id": "storage"}, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	d := f.(*Driver)
	t.Cleanup(func() { _ = d.Shutdown(context.Background()) })
	return d
}
func at(d *Driver, p string) *provider.Reference {
	return &provider.Reference{ResourceId: d.id(d.s.spaceID), Path: p}
}
func uploadFile(t *testing.T, d *Driver, p, content string) *provider.ResourceInfo {
	t.Helper()
	ctx := ownerContext()
	ids, e := d.InitiateUpload(ctx, at(d, p), int64(len(content)), nil)
	if e != nil {
		t.Fatal(e)
	}
	ri, e := d.Upload(ctx, storage.UploadRequest{Ref: &provider.Reference{Path: ids["simple"]}, Body: io.NopCloser(strings.NewReader(content)), Length: int64(len(content))}, nil)
	if e != nil {
		t.Fatal(e)
	}
	return ri
}
func readContent(t *testing.T, d *Driver, ref *provider.Reference) string {
	t.Helper()
	_, r, e := d.Download(ownerContext(), ref, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	b, e := io.ReadAll(r)
	if e != nil {
		t.Fatal(e)
	}
	return string(b)
}

func TestTreeMetadataRestartMoveAndTrash(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	if e := os.Mkdir(filepath.Join(root, "文件%_"), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(root, "文件%_", "original.txt"), []byte("hello"), 0600); e != nil {
		t.Fatal(e)
	}
	d := testDriver(t, root, state)
	ctx := ownerContext()
	ri, e := d.GetMD(ctx, at(d, "文件%_/original.txt"), nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	id := ri.Id.OpaqueId
	if e = d.SetArbitraryMetadata(ctx, d.ref(id), &provider.ArbitraryMetadata{Metadata: map[string]string{"label": "kept"}}); e != nil {
		t.Fatal(e)
	}
	if _, e = d.Move(ctx, at(d, "文件%_"), at(d, "renamed")); e != nil {
		t.Fatal(e)
	}
	if got, e := d.GetPathByID(ctx, d.id(id)); e != nil || got != "/renamed/original.txt" {
		t.Fatalf("moved child: %q %v", got, e)
	}
	if e = d.Shutdown(ctx); e != nil {
		t.Fatal(e)
	}
	d = testDriver(t, root, state)
	if got := readContent(t, d, d.ref(id)); got != "hello" {
		t.Fatal(got)
	}
	ri, e = d.GetMD(ctx, d.ref(id), nil, nil)
	if e != nil || ri.ArbitraryMetadata.Metadata["label"] != "kept" {
		t.Fatalf("metadata lost: %v %v", ri, e)
	}
	if _, e = d.Delete(ctx, at(d, "renamed")); e != nil {
		t.Fatal(e)
	}
	if _, e = d.GetMD(ctx, d.ref(id), nil, nil); e == nil {
		t.Fatal("trashed child is accessible")
	}
	items, e := d.ListRecycle(ctx, at(d, "."), "", "")
	if e != nil || len(items) != 1 {
		t.Fatalf("trash: %v %v", items, e)
	}
	if _, e = d.ListFolder(ctx, at(d, "."), nil, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = d.RestoreRecycleItem(ctx, at(d, "."), items[0].Key, "", at(d, "restored")); e != nil {
		t.Fatal(e)
	}
	if got, e := d.GetPathByID(ctx, d.id(id)); e != nil || got != "/restored/original.txt" {
		t.Fatalf("restored identity: %q %v", got, e)
	}
	ri, e = d.GetMD(ctx, d.ref(id), nil, nil)
	if e != nil || ri.ArbitraryMetadata.Metadata["label"] != "kept" {
		t.Fatalf("restored metadata: %v %v", ri, e)
	}
}

func TestUploadsVersionsAndConflict(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	ctx := ownerContext()
	first := uploadFile(t, d, "document.txt", "first")
	ids, e := d.InitiateUpload(ctx, at(d, "document.txt"), 5, nil)
	if e != nil {
		t.Fatal(e)
	}
	second := uploadFile(t, d, "document.txt", "second")
	if first.Id.OpaqueId != second.Id.OpaqueId {
		t.Fatal("replacement changed file ID")
	}
	if _, e = d.Upload(ctx, storage.UploadRequest{Ref: &provider.Reference{Path: ids["simple"]}, Body: io.NopCloser(strings.NewReader("stale"))}, nil); e == nil {
		t.Fatal("stale upload overwrote current content")
	}
	if got := readContent(t, d, d.ref(first.Id.OpaqueId)); got != "second" {
		t.Fatal(got)
	}
	versions, e := d.ListRevisions(ctx, d.ref(first.Id.OpaqueId))
	if e != nil || len(versions) != 1 {
		t.Fatalf("versions: %v %v", versions, e)
	}
	_, r, e := d.DownloadRevision(ctx, d.ref(first.Id.OpaqueId), versions[0].Key, nil)
	if e != nil {
		t.Fatal(e)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "first" {
		t.Fatal(string(b))
	}
	if _, e = d.RestoreRevision(ctx, d.ref(first.Id.OpaqueId), versions[0].Key); e != nil {
		t.Fatal(e)
	}
	if got := readContent(t, d, d.ref(first.Id.OpaqueId)); got != "first" {
		t.Fatal(got)
	}
}

func TestUploadResumeAcrossProviderInstances(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	a := testDriver(t, root, state)
	b := testDriver(t, root, state)
	ctx := ownerContext()
	ids, e := a.InitiateUpload(ctx, at(a, "resume.txt"), 6, nil)
	if e != nil {
		t.Fatal(e)
	}
	u, e := a.GetUpload(ctx, ids["tus"])
	if e != nil {
		t.Fatal(e)
	}
	if _, e = u.WriteChunk(ctx, 0, strings.NewReader("abc")); e != nil {
		t.Fatal(e)
	}
	u, e = b.GetUpload(ctx, ids["tus"])
	if e != nil {
		t.Fatal(e)
	}
	if _, e = u.WriteChunk(ctx, 3, strings.NewReader("def")); e != nil {
		t.Fatal(e)
	}
	if e = u.FinishUpload(ctx); e != nil {
		t.Fatal(e)
	}
	if e = u.FinishUpload(ctx); e != nil {
		t.Fatal("finish is not idempotent", e)
	}
	if got := readContent(t, a, at(a, "resume.txt")); got != "abcdef" {
		t.Fatal(got)
	}
}

func TestJournalRecoveryAtRemoteRename(t *testing.T) {
	for _, renamed := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[renamed], func(t *testing.T) {
			root, state := t.TempDir(), t.TempDir()
			d := testDriver(t, root, state)
			s := d.s
			tmp := path.Join(control, "tmp", uuid.NewString())
			if e := os.WriteFile(filepath.Join(root, tmp), []byte("durable"), 0600); e != nil {
				t.Fatal(e)
			}
			op := operation{ID: uuid.NewString(), Kind: "create", Source: tmp, Target: "recovered.txt", Node: nodeRecord{ID: uuid.NewString(), Parent: s.spaceID, Name: "recovered.txt", Path: "recovered.txt"}, Time: time.Now().UnixNano()}
			op.Digest, _ = s.digest(tmp)
			body, _ := json.Marshal(op)
			if _, e := s.db.Exec("INSERT INTO operations VALUES (?,?)", op.ID, body); e != nil {
				t.Fatal(e)
			}
			if renamed {
				if e := s.root.Rename(tmp, op.Target); e != nil {
					t.Fatal(e)
				}
			}
			if e := d.Shutdown(context.Background()); e != nil {
				t.Fatal(e)
			}
			d = testDriver(t, root, state)
			if got := readContent(t, d, d.ref(op.Node.ID)); got != "durable" {
				t.Fatal(got)
			}
			var count int
			if e := d.s.db.QueryRow("SELECT count(*) FROM operations").Scan(&count); e != nil || count != 0 {
				t.Fatalf("journal not cleared: %d %v", count, e)
			}
		})
	}
}

func TestMountLossAndIncompleteScanPreserveMetadata(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	d := testDriver(t, root, state)
	ri := uploadFile(t, d, "keep.txt", "content")
	marker := filepath.Join(root, control, "identity")
	if e := os.Rename(marker, marker+".saved"); e != nil {
		t.Fatal(e)
	}
	if e := d.s.scan(context.Background(), time.Nanosecond); e == nil {
		t.Fatal("scan accepted unavailable mount")
	}
	if _, e := d.s.byID(ri.Id.OpaqueId); e != nil {
		t.Fatal("metadata removed", e)
	}
	if e := os.Rename(marker+".saved", marker); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink(t.TempDir(), filepath.Join(root, "escape")); e != nil {
		t.Fatal(e)
	}
	if e := d.s.scan(context.Background(), time.Nanosecond); e == nil {
		t.Fatal("scan followed symlink")
	}
	if _, e := d.s.byID(ri.Id.OpaqueId); e != nil {
		t.Fatal(e)
	}
}

func TestPermissionsAndPathConfinement(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	ri := uploadFile(t, d, "private.txt", "secret")
	other := ctxpkg.ContextSetUser(context.Background(), &userpb.User{Id: &userpb.UserId{OpaqueId: "other", Idp: "test"}})
	if _, _, e := d.Download(other, d.ref(ri.Id.OpaqueId), nil); e == nil {
		t.Fatal("unauthorized read")
	}
	g := &provider.Grant{Grantee: &provider.Grantee{Type: provider.GranteeType_GRANTEE_TYPE_USER, Id: &provider.Grantee_UserId{UserId: &userpb.UserId{OpaqueId: "other", Idp: "test"}}}, Permissions: &provider.ResourcePermissions{Stat: true, InitiateFileDownload: true}}
	if e := d.AddGrant(ownerContext(), d.ref(ri.Id.OpaqueId), g); e != nil {
		t.Fatal(e)
	}
	_, r, e := d.Download(other, d.ref(ri.Id.OpaqueId), nil)
	if e != nil {
		t.Fatal(e)
	}
	r.Close()
	if _, e = d.Delete(other, d.ref(ri.Id.OpaqueId)); e == nil {
		t.Fatal("read grant allowed delete")
	}
	if e = d.DenyGrant(ownerContext(), d.ref(ri.Id.OpaqueId), g.Grantee); e != nil {
		t.Fatal(e)
	}
	if _, _, e = d.Download(other, d.ref(ri.Id.OpaqueId), nil); e == nil {
		t.Fatal("deny ignored")
	}
	for _, p := range []string{"../escape", control + "/identity", "a/../../escape", "a\\b"} {
		if _, e = d.CreateDir(ownerContext(), at(d, p)); e == nil {
			t.Fatalf("accepted unsafe path %s", p)
		}
	}
}

func TestRejectRemoteStateDirectory(t *testing.T) {
	root := t.TempDir()
	if s, e := openStore(root, filepath.Join(root, "metadata")); e == nil {
		s.close()
		t.Fatal("accepted state inside remote root")
	}
}

func TestExternalDeletionGrace(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	ri := uploadFile(t, d, "removed.txt", "gone")
	if e := d.s.root.Remove("removed.txt"); e != nil {
		t.Fatal(e)
	}
	if e := d.s.scan(context.Background(), time.Hour); e != nil {
		t.Fatal(e)
	}
	n, e := d.s.byID(ri.Id.OpaqueId)
	if e != nil || n.Missing == 0 {
		t.Fatalf("missing grace not recorded: %v %v", n, e)
	}
	if e = d.s.scan(context.Background(), time.Nanosecond); e != nil {
		t.Fatal(e)
	}
	if _, e = d.s.byID(ri.Id.OpaqueId); e == nil {
		t.Fatal("expired missing node remains")
	}
}

func TestAmbiguousRecoveryStops(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	op := operation{ID: uuid.NewString(), Kind: "move", Source: "missing", Target: "also-missing", Digest: "invalid"}
	b, _ := json.Marshal(op)
	if _, e := d.s.db.Exec("INSERT INTO operations VALUES (?,?)", op.ID, b); e != nil {
		t.Fatal(e)
	}
	if e := d.s.recover(context.Background()); e == nil {
		t.Fatal("ambiguous recovery succeeded")
	}
	var count int
	if e := d.s.db.QueryRow("SELECT count(*) FROM operations").Scan(&count); e != nil || count != 1 {
		t.Fatalf("journal lost: %d %v", count, e)
	}
}

func TestReplacementRecoveryAfterBackupAndAfterInstall(t *testing.T) {
	for _, installed := range []bool{false, true} {
		t.Run(map[bool]string{false: "backup", true: "installed"}[installed], func(t *testing.T) {
			root, state := t.TempDir(), t.TempDir()
			d := testDriver(t, root, state)
			ri := uploadFile(t, d, "replace.txt", "old")
			n, e := d.s.byID(ri.Id.OpaqueId)
			if e != nil {
				t.Fatal(e)
			}
			tmp := path.Join(control, "tmp", uuid.NewString())
			if e = os.WriteFile(filepath.Join(root, tmp), []byte("new"), 0600); e != nil {
				t.Fatal(e)
			}
			op := operation{ID: uuid.NewString(), Kind: "replace", Source: tmp, Target: n.Path, Node: n, Time: time.Now().UnixNano()}
			op.Digest, _ = d.s.digest(tmp)
			op.Previous, _ = d.s.digest(n.Path)
			b, _ := json.Marshal(op)
			if _, e = d.s.db.Exec("INSERT INTO operations VALUES (?,?)", op.ID, b); e != nil {
				t.Fatal(e)
			}
			if e = d.s.root.Rename(n.Path, path.Join(control, "versions", op.ID)); e != nil {
				t.Fatal(e)
			}
			if installed {
				if e = d.s.root.Rename(tmp, n.Path); e != nil {
					t.Fatal(e)
				}
			}
			d.Shutdown(context.Background())
			d = testDriver(t, root, state)
			if got := readContent(t, d, d.ref(n.ID)); got != "new" {
				t.Fatal(got)
			}
			v, e := d.ListRevisions(ownerContext(), d.ref(n.ID))
			if e != nil || len(v) != 1 {
				t.Fatalf("retained backup: %v %v", v, e)
			}
		})
	}
}

func TestConcurrentProviderWritesAndTUSCompletionMetadata(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	a := testDriver(t, root, state)
	b := testDriver(t, root, state)
	ctx := ownerContext()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i, d := range []*Driver{a, b} {
		wg.Add(1)
		go func(i int, d *Driver) {
			defer wg.Done()
			name := []string{"a.txt", "b.txt"}[i]
			ids, e := d.InitiateUpload(ctx, at(d, name), 1, nil)
			if e != nil {
				errs <- e
				return
			}
			u, e := d.GetUpload(ctx, ids["tus"])
			if e != nil {
				errs <- e
				return
			}
			info, e := u.GetInfo(ctx)
			if e != nil {
				errs <- e
				return
			}
			if info.Storage["NodeId"] == "" || info.Storage["SpaceRoot"] != d.s.spaceID {
				t.Error("TUS resource headers lack identity")
			}
			if _, e = u.WriteChunk(ctx, 0, strings.NewReader("x")); e != nil {
				errs <- e
				return
			}
			if e = u.FinishUpload(ctx); e != nil {
				errs <- e
				return
			}
			sessions, e := d.ListUploadSessions(context.Background(), storage.UploadSessionFilter{ID: &info.ID})
			if e != nil {
				errs <- e
				return
			}
			if len(sessions) != 1 || sessions[0].Reference().ResourceId.GetOpaqueId() != info.Storage["NodeId"] {
				t.Error("completion event identity mismatch")
			}
		}(i, d)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

func TestMassDisappearanceDoesNotCommit(t *testing.T) {
	root := t.TempDir()
	for i := range 12 {
		if e := os.WriteFile(filepath.Join(root, string(rune('a'+i))), nil, 0600); e != nil {
			t.Fatal(e)
		}
	}
	d := testDriver(t, root, t.TempDir())
	for i := range 12 {
		if e := os.Remove(filepath.Join(root, string(rune('a'+i)))); e != nil {
			t.Fatal(e)
		}
	}
	if e := d.s.scan(context.Background(), time.Nanosecond); e == nil {
		t.Fatal("mass disappearance was accepted")
	}
	records, e := d.s.records()
	if e != nil || len(records) != 13 {
		t.Fatalf("metadata changed: %d %v", len(records), e)
	}
	for _, n := range records {
		if n.Missing != 0 {
			t.Fatal("failed scan committed missing entries")
		}
	}
}

func TestRecreatedMissingFileGetsNewIdentity(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	ri := uploadFile(t, d, "recreated", "old")
	if e := d.s.root.Remove("recreated"); e != nil {
		t.Fatal(e)
	}
	if e := d.s.scan(context.Background(), time.Hour); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(d.s.rootPath, "recreated"), []byte("new"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := d.s.scan(context.Background(), time.Hour); e != nil {
		t.Fatal(e)
	}
	n, e := d.s.byPath("recreated")
	if e != nil {
		t.Fatal(e)
	}
	if n.ID == ri.Id.OpaqueId {
		t.Fatal("new object inherited old identity")
	}
}

func TestTUSHTTPUpload(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	ctx := ownerContext()
	ids, e := d.InitiateUpload(ctx, at(d, "http.txt"), 4, nil)
	if e != nil {
		t.Fatal(e)
	}
	log := zerolog.Nop()
	manager, e := tusmanager.New(nil, nil, &log)
	if e != nil {
		t.Fatal(e)
	}
	handler, e := manager.Handler(d)
	if e != nil {
		t.Fatal(e)
	}
	req := httptest.NewRequest(http.MethodPatch, "/"+ids["tus"], strings.NewReader("test")).WithContext(ctx)
	req.Header.Set("Tus-Resumable", "1.0.0")
	req.Header.Set("Upload-Offset", "0")
	req.Header.Set("Content-Type", "application/offset+octet-stream")
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { handler.ServeHTTP(rec, req); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("TUS completion blocked")
	}
	if rec.Code != http.StatusNoContent {
		t.Fatalf("TUS status %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("OC-FileId") == "" {
		t.Fatal("missing OC-FileId")
	}
	if got := readContent(t, d, at(d, "http.txt")); got != "test" {
		t.Fatal(got)
	}
}
