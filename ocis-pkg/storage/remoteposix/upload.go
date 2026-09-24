package remoteposix

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/google/uuid"
	"github.com/owncloud/reva/v2/pkg/errtypes"
	"github.com/owncloud/reva/v2/pkg/storage"
	tusd "github.com/tus/tusd/v2/pkg/handler"
)

type session struct {
	Info                                                      tusd.FileInfo
	User, IDP, Parent, Name, ExpectedID, ExpectedETag, Result string
	Created                                                   int64
}
type upload struct {
	d  *Driver
	id string
}

func (d *Driver) session(ctx context.Context, id string) (session, error) {
	var s session
	if _, e := uuid.Parse(id); e != nil {
		return s, tusd.ErrNotFound
	}
	var b []byte
	if e := d.s.db.QueryRow("SELECT body FROM uploads WHERE id=?", id).Scan(&b); e != nil {
		return s, mapError(e)
	}
	if e := json.Unmarshal(b, &s); e != nil {
		return s, e
	}
	u, e := executant(ctx)
	if e != nil {
		return s, e
	}
	if u.Id.OpaqueId != s.User || u.Id.Idp != s.IDP {
		return s, errtypes.PermissionDenied("upload owner")
	}
	if time.Since(time.Unix(0, s.Created)) > 24*time.Hour {
		return s, errtypes.PreconditionFailed("upload expired")
	}
	return s, nil
}
func (d *Driver) saveSession(s session) error {
	b, e := json.Marshal(s)
	if e != nil {
		return e
	}
	_, e = d.s.db.Exec("INSERT INTO uploads VALUES (?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body", s.Info.ID, b)
	return e
}
func (d *Driver) stage(id string) string { return filepath.Join(d.s.state, "staging", id) }

func (d *Driver) start(ctx context.Context, ref *provider.Reference, info tusd.FileInfo) (tusd.Upload, error) {
	u, e := d.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer u()
	parent, p, e := d.target(ref)
	if e != nil {
		return nil, e
	}
	if e = d.allow(ctx, parent, "upload"); e != nil {
		return nil, e
	}
	if info.IsPartial || info.IsFinal {
		return nil, errtypes.NotSupported("upload concatenation")
	}
	if !info.SizeIsDeferred && info.Size < 0 {
		return nil, errtypes.BadRequest("negative upload size")
	}
	if mtime := info.MetaData["mtime"]; mtime != "" {
		if _, err := strconv.ParseInt(mtime, 10, 64); err != nil {
			return nil, errtypes.BadRequest("invalid mtime")
		}
	}
	metadata := tusd.MetaData{}
	for k, v := range info.MetaData {
		metadata[k] = v
	}
	info.MetaData = metadata
	user, e := executant(ctx)
	if e != nil {
		return nil, e
	}
	info.ID = uuid.NewString()
	info.Offset = 0
	info.Storage = map[string]string{"SpaceRoot": d.s.spaceID, "NodeId": uuid.NewString()}
	if info.MetaData == nil {
		info.MetaData = tusd.MetaData{}
	}
	info.MetaData["providerID"] = d.c.MountID
	info.MetaData["expires"] = time.Now().Add(24 * time.Hour).Format(time.RFC3339)
	s := session{Info: info, User: user.Id.OpaqueId, IDP: user.Id.Idp, Parent: parent.ID, Name: path.Base(p), Created: time.Now().UnixNano()}
	if n, e := d.s.byPath(p); e == nil {
		if n.Dir {
			return nil, errtypes.AlreadyExists(p)
		}
		if e = d.allow(ctx, n, "upload"); e != nil {
			return nil, e
		}
		s.ExpectedID = n.ID
		s.ExpectedETag = n.ETag
		s.Info.Storage["NodeId"] = n.ID
	} else if !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	if e = os.MkdirAll(filepath.Join(d.s.state, "staging"), 0700); e != nil {
		return nil, e
	}
	f, e := os.OpenFile(d.stage(info.ID), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	if e = f.Close(); e != nil {
		return nil, e
	}
	if e = d.saveSession(s); e != nil {
		_ = os.Remove(d.stage(info.ID))
		return nil, e
	}
	return &upload{d: d, id: info.ID}, nil
}

func (d *Driver) InitiateUpload(ctx context.Context, ref *provider.Reference, length int64, metadata map[string]string) (map[string]string, error) {
	info := tusd.FileInfo{Size: length, MetaData: tusd.MetaData(metadata)}
	_, info.SizeIsDeferred = metadata["sizedeferred"]
	if length < 0 {
		info.SizeIsDeferred = true
	}
	u, e := d.start(ctx, ref, info)
	if e != nil {
		return nil, e
	}
	i, e := u.GetInfo(ctx)
	if e != nil {
		return nil, e
	}
	return map[string]string{"simple": i.ID, "tus": i.ID}, nil
}
func (d *Driver) NewUpload(ctx context.Context, info tusd.FileInfo) (tusd.Upload, error) {
	return d.start(ctx, &provider.Reference{ResourceId: d.id(d.s.spaceID), Path: path.Join(info.MetaData["dir"], info.MetaData["filename"])}, info)
}
func (d *Driver) GetUpload(ctx context.Context, id string) (tusd.Upload, error) {
	u, e := d.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer u()
	if _, e = d.session(ctx, id); e != nil {
		return nil, e
	}
	return &upload{d: d, id: id}, nil
}
func (d *Driver) UseIn(c *tusd.StoreComposer) {
	c.UseCore(d)
	c.UseTerminater(d)
	c.UseLengthDeferrer(d)
}
func (d *Driver) AsTerminatableUpload(u tusd.Upload) tusd.TerminatableUpload { return u.(*upload) }
func (d *Driver) AsLengthDeclarableUpload(u tusd.Upload) tusd.LengthDeclarableUpload {
	return u.(*upload)
}

func (u *upload) GetInfo(ctx context.Context) (tusd.FileInfo, error) {
	unlock, e := u.d.begin(ctx)
	if e != nil {
		return tusd.FileInfo{}, e
	}
	defer unlock()
	s, e := u.d.session(ctx, u.id)
	return s.Info, e
}
func (u *upload) WriteChunk(ctx context.Context, offset int64, r io.Reader) (int64, error) {
	unlock, e := u.d.begin(ctx)
	if e != nil {
		return 0, e
	}
	defer unlock()
	s, e := u.d.session(ctx, u.id)
	if e != nil {
		return 0, e
	}
	if s.Result != "" || offset != s.Info.Offset {
		return 0, errtypes.PreconditionFailed("upload offset or state mismatch")
	}
	f, e := os.OpenFile(u.d.stage(u.id), os.O_WRONLY, 0600)
	if e != nil {
		return 0, e
	}
	defer f.Close()
	if e = f.Truncate(offset); e != nil {
		return 0, e
	}
	if _, e = f.Seek(offset, io.SeekStart); e != nil {
		return 0, e
	}
	if !s.Info.SizeIsDeferred {
		r = io.LimitReader(r, s.Info.Size-offset+1)
	}
	n, e := io.Copy(f, r)
	if e != nil {
		return n, e
	}
	if !s.Info.SizeIsDeferred && offset+n > s.Info.Size {
		_ = f.Truncate(offset)
		return 0, errtypes.BadRequest("upload exceeds declared size")
	}
	if e = f.Sync(); e != nil {
		return n, e
	}
	s.Info.Offset += n
	return n, u.d.saveSession(s)
}
func (u *upload) GetReader(ctx context.Context) (io.ReadCloser, error) {
	unlock, e := u.d.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer unlock()
	if _, e = u.d.session(ctx, u.id); e != nil {
		return nil, e
	}
	return os.Open(u.d.stage(u.id))
}
func (u *upload) DeclareLength(ctx context.Context, size int64) error {
	unlock, e := u.d.begin(ctx)
	if e != nil {
		return e
	}
	defer unlock()
	s, e := u.d.session(ctx, u.id)
	if e != nil {
		return e
	}
	if !s.Info.SizeIsDeferred || size < s.Info.Offset {
		return errtypes.BadRequest("invalid upload length")
	}
	s.Info.Size = size
	s.Info.SizeIsDeferred = false
	return u.d.saveSession(s)
}
func (u *upload) Terminate(ctx context.Context) error {
	unlock, e := u.d.begin(ctx)
	if e != nil {
		return e
	}
	defer unlock()
	if _, e = u.d.session(ctx, u.id); e != nil {
		return e
	}
	if e = os.Remove(u.d.stage(u.id)); e != nil && !errors.Is(e, os.ErrNotExist) {
		return e
	}
	_, e = u.d.s.db.Exec("DELETE FROM uploads WHERE id=?", u.id)
	return e
}

func (d *Driver) write(ctx context.Context, parent nodeRecord, name string, r io.Reader, length int64, expectedID, expectedETag string, sessionIDs ...string) (nodeRecord, error) {
	if length < 0 || length == int64(^uint64(0)>>1) {
		return nodeRecord{}, errtypes.BadRequest("invalid upload length")
	}
	if e := d.allow(ctx, parent, "upload"); e != nil {
		return nodeRecord{}, e
	}
	p := path.Join(parent.Path, name)
	n, e := d.s.byPath(p)
	kind := "create"
	if e == nil {
		if n.Dir || n.ID != expectedID || n.ETag != expectedETag {
			return n, errtypes.PreconditionFailed("destination changed since upload started")
		}
		fi, err := d.s.root.Stat(p)
		if err != nil {
			return n, mapError(err)
		}
		if fi.Size() != n.Size || fi.ModTime().UnixNano() != n.Mtime {
			return n, errtypes.PreconditionFailed("remote file changed outside oCIS")
		}
		if e = d.allow(ctx, n, "upload"); e != nil {
			return n, e
		}
		kind = "replace"
	} else {
		if !errors.Is(e, sql.ErrNoRows) {
			return n, e
		}
		if expectedID != "" {
			return n, errtypes.PreconditionFailed("destination disappeared")
		}
		n = nodeRecord{ID: uuid.NewString(), Parent: parent.ID, Name: name, Path: p}
		if len(sessionIDs) > 0 && sessionIDs[0] != "" {
			upload, err := d.session(ctx, sessionIDs[0])
			if err != nil {
				return n, err
			}
			n.ID = upload.Info.Storage["NodeId"]
		}
	}
	// Temporary files are on the same mounted filesystem as the destination.
	tmp := path.Join(control, "tmp", uuid.NewString())
	f, e := d.s.root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return n, e
	}
	count, e := io.Copy(f, io.LimitReader(r, length+1))
	if e == nil && count != length {
		e = errtypes.BadRequest("upload size mismatch")
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		_ = d.s.root.Remove(tmp)
		return n, e
	}
	if len(sessionIDs) > 0 && sessionIDs[0] != "" {
		upload, err := d.session(ctx, sessionIDs[0])
		if err != nil {
			return n, err
		}
		if value := upload.Info.MetaData["mtime"]; value != "" {
			seconds, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return n, err
			}
			mtime := time.Unix(seconds, 0)
			if err = d.s.root.Chtimes(tmp, mtime, mtime); err != nil {
				return n, err
			}
		}
	}
	sessionID := ""
	if len(sessionIDs) > 0 {
		sessionID = sessionIDs[0]
	}
	e = d.s.apply(ctx, operation{ID: uuid.NewString(), Kind: kind, Source: tmp, Target: p, Node: n, Session: sessionID})
	if e != nil {
		return n, e
	}
	return d.s.byID(n.ID)
}
func (u *upload) FinishUpload(ctx context.Context) error {
	d := u.d
	unlock, e := d.begin(ctx)
	if e != nil {
		return e
	}
	defer unlock()
	s, e := d.session(ctx, u.id)
	if e != nil {
		return e
	}
	if s.Result != "" {
		return nil
	}
	if s.Info.SizeIsDeferred || s.Info.Offset != s.Info.Size {
		return errtypes.PreconditionFailed("upload incomplete")
	}
	parent, e := d.s.byID(s.Parent)
	if e != nil || !public(parent) {
		return errtypes.NotFound("upload parent")
	}
	f, e := os.Open(d.stage(u.id))
	if e != nil {
		return e
	}
	defer f.Close()
	_, e = d.write(ctx, parent, s.Name, f, s.Info.Size, s.ExpectedID, s.ExpectedETag, u.id)
	if e != nil {
		return e
	}
	return nil
}
func (d *Driver) Upload(ctx context.Context, req storage.UploadRequest, finished storage.UploadFinishedFunc) (*provider.ResourceInfo, error) {
	u, e := d.GetUpload(ctx, strings.TrimPrefix(req.Ref.GetPath(), "/"))
	if e != nil {
		return nil, e
	}
	i, e := u.GetInfo(ctx)
	if e != nil {
		return nil, e
	}
	if i.Offset != 0 {
		return nil, errtypes.PreconditionFailed("simple upload already started")
	}
	if _, e = u.WriteChunk(ctx, 0, req.Body); e != nil {
		return nil, e
	}
	i, e = u.GetInfo(ctx)
	if e != nil {
		return nil, e
	}
	if i.SizeIsDeferred {
		if e = u.(*upload).DeclareLength(ctx, i.Offset); e != nil {
			return nil, e
		}
	}
	if e = u.FinishUpload(ctx); e != nil {
		return nil, e
	}
	unlock, e := d.begin(ctx)
	if e != nil {
		return nil, e
	}
	s, e := d.session(ctx, u.(*upload).id)
	unlock()
	if e != nil {
		return nil, e
	}
	ri, e := d.GetMD(ctx, d.ref(s.Result), nil, nil)
	if e == nil && finished != nil {
		user, _ := executant(ctx)
		finished(d.owner(), user.Id, d.ref(s.Result))
	}
	return ri, e
}
func (d *Driver) TouchFile(ctx context.Context, ref *provider.Reference, _ bool, _ string) (*storage.TouchFileResult, error) {
	u, e := d.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer u()
	n, e := d.resolve(ref)
	if e == nil {
		if e = d.allow(ctx, n, "upload"); e != nil {
			return nil, e
		}
		return &storage.TouchFileResult{SpaceOwner: d.owner(), SpaceID: d.s.spaceID, ResourceID: d.id(n.ID)}, nil
	}
	parent, p, e := d.target(ref)
	if e != nil {
		return nil, e
	}
	n, e = d.write(ctx, parent, path.Base(p), bytes.NewReader(nil), 0, "", "")
	if e != nil {
		return nil, e
	}
	return &storage.TouchFileResult{SpaceOwner: d.owner(), SpaceID: d.s.spaceID, ResourceID: d.id(n.ID)}, nil
}
func (d *Driver) MarkProcessing(context.Context, *provider.Reference, bool, string) error {
	return errtypes.NotSupported("remoteposix uses synchronous upload sessions")
}
func (d *Driver) PrepareUpload(context.Context, *provider.Reference, string, storage.UploadInfo) (*storage.PrepareUploadResult, error) {
	return nil, errtypes.NotSupported("remoteposix uses synchronous upload sessions")
}
func (d *Driver) RollbackUpload(context.Context, *provider.Reference, string, storage.RollbackInfo) error {
	return errtypes.NotSupported("remoteposix uses synchronous upload sessions")
}
func (d *Driver) CommitUpload(context.Context, *provider.Reference, string, storage.UploadSource) error {
	return errtypes.NotSupported("remoteposix uses synchronous upload sessions")
}

// ListUploadSessions is used by the TUS completion event consumer and the
// storage-users upload maintenance command; it is not a public filesystem API.
func (d *Driver) ListUploadSessions(ctx context.Context, filter storage.UploadSessionFilter) ([]storage.UploadSession, error) {
	u, e := d.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer u()
	rows, e := d.s.db.Query("SELECT body FROM uploads")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []storage.UploadSession{}
	for rows.Next() {
		var b []byte
		var s session
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(b, &s); e != nil {
			return nil, e
		}
		if filter.ID != nil && s.Info.ID != *filter.ID {
			continue
		}
		if filter.Processing != nil && *filter.Processing {
			continue
		}
		if filter.HasVirus != nil && *filter.HasVirus {
			continue
		}
		expired := time.Since(time.Unix(0, s.Created)) > 24*time.Hour
		if filter.Expired != nil && *filter.Expired != expired {
			continue
		}
		out = append(out, &sessionView{d: d, s: s})
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}
	rows.Close()
	if filter.Orphaned != nil {
		filtered := out[:0]
		for _, v := range out {
			sv := v.(*sessionView)
			_, e := d.s.byID(sv.s.Info.Storage["NodeId"])
			orphaned := e != nil && sv.s.Result != ""
			if orphaned == *filter.Orphaned {
				filtered = append(filtered, v)
			}
		}
		out = filtered
	}
	return out, nil
}

type sessionView struct {
	d *Driver
	s session
}

func (v *sessionView) ID() string                    { return v.s.Info.ID }
func (v *sessionView) Filename() string              { return v.s.Name }
func (v *sessionView) Size() int64                   { return v.s.Info.Size }
func (v *sessionView) Offset() int64                 { return v.s.Info.Offset }
func (v *sessionView) Reference() provider.Reference { return *v.d.ref(v.s.Info.Storage["NodeId"]) }
func (v *sessionView) Executant() userpb.UserId {
	return userpb.UserId{OpaqueId: v.s.User, Idp: v.s.IDP}
}
func (v *sessionView) SpaceOwner() *userpb.UserId    { return v.d.owner() }
func (v *sessionView) Expires() time.Time            { return time.Unix(0, v.s.Created).Add(24 * time.Hour) }
func (v *sessionView) IsProcessing() bool            { return false }
func (v *sessionView) ScanData() (string, time.Time) { return "", time.Time{} }
func (v *sessionView) Purge(ctx context.Context) {
	u, e := v.d.begin(ctx)
	if e != nil {
		return
	}
	defer u()
	if e = os.Remove(v.d.stage(v.s.Info.ID)); e != nil && !errors.Is(e, os.ErrNotExist) {
		return
	}
	_, _ = v.d.s.db.Exec("DELETE FROM uploads WHERE id=?", v.s.Info.ID)
}
