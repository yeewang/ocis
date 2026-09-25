package remoteposix

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"mime"
	"os"
	"path"
	"strings"
	"time"
	"unicode/utf8"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	ctxpkg "github.com/owncloud/reva/v2/pkg/ctx"
	"github.com/owncloud/reva/v2/pkg/errtypes"
	"github.com/owncloud/reva/v2/pkg/events"
	"github.com/owncloud/reva/v2/pkg/storage"
	"github.com/owncloud/reva/v2/pkg/storage/fs/registry"
	"github.com/owncloud/reva/v2/pkg/storagespace"
	"github.com/owncloud/reva/v2/pkg/utils"
	"github.com/rs/zerolog"
)

func init() { registry.Register("remoteposix", New) }

type Config struct {
	Root           string        `mapstructure:"root"`
	StateDir       string        `mapstructure:"state_dir"`
	OwnerID        string        `mapstructure:"owner_id"`
	OwnerIDP       string        `mapstructure:"owner_idp"`
	SpaceName      string        `mapstructure:"space_name"`
	MountID        string        `mapstructure:"mount_id"`
	TransferSecret string        `mapstructure:"transfer_secret"`
	DataServerURL  string        `mapstructure:"data_server_url"`
	ScanInterval   time.Duration `mapstructure:"scan_interval"`
	MissingGrace   time.Duration `mapstructure:"missing_grace_period"`
	Watch          bool          `mapstructure:"watch"`
}

type Driver struct {
	s      *store
	c      Config
	stream events.Stream
	log    zerolog.Logger
	cancel context.CancelFunc
	done   chan struct{}
}

var _ storage.FS = (*Driver)(nil)

func New(m map[string]interface{}, stream events.Stream, log *zerolog.Logger) (storage.FS, error) {
	c := Config{ScanInterval: time.Minute, MissingGrace: 10 * time.Minute, SpaceName: "Remote storage"}
	if err := mapstructure.Decode(m, &c); err != nil {
		return nil, err
	}
	if c.OwnerID == "" || c.OwnerIDP == "" || c.MountID == "" {
		return nil, errors.New("remoteposix requires owner_id, owner_idp and mount_id")
	}
	if c.ScanInterval <= 0 || c.MissingGrace <= 0 {
		return nil, errors.New("scan interval and missing grace must be positive")
	}
	s, err := openStore(c.Root, c.StateDir)
	if err != nil {
		return nil, err
	}
	d := &Driver{s: s, c: c, stream: stream, log: zerolog.Nop(), done: make(chan struct{})}
	if log != nil {
		d.log = *log
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	// Reclaim local payloads before a potentially long remote-tree scan.
	if err = d.cleanupUploads(ctx); err != nil {
		d.log.Warn().Err(err).Msg("upload staging cleanup deferred")
	}
	err = s.scan(ctx, c.MissingGrace)
	if err != nil {
		s.close()
		cancel()
		return nil, err
	}
	go d.maintain(ctx)
	return d, nil
}

func (d *Driver) owner() *userpb.UserId {
	return &userpb.UserId{OpaqueId: d.c.OwnerID, Idp: d.c.OwnerIDP}
}
func (d *Driver) id(id string) *provider.ResourceId {
	return &provider.ResourceId{StorageId: d.c.MountID, SpaceId: d.s.spaceID, OpaqueId: id}
}
func (d *Driver) ref(id string) *provider.Reference { return &provider.Reference{ResourceId: d.id(id)} }
func (d *Driver) Capabilities(context.Context) storage.Capabilities {
	return storage.Capabilities{Upload: true, CreateContainer: true, Move: true, Delete: true, Trash: true, Versioning: true, Sharing: true, ArbitraryMetadata: true}
}
func (d *Driver) Shutdown(context.Context) error { d.cancel(); <-d.done; return d.s.close() }

// Whole-tree maintenance remains exclusive. Ordinary requests name resources.
func (d *Driver) begin(ctx context.Context) (func(), error) {
	return d.beginRefs(ctx, lockRef{d.ref(d.s.spaceID), true})
}

func public(n nodeRecord) bool {
	return n.Missing == 0 && n.Path != control && !strings.HasPrefix(n.Path, control+"/")
}
func (d *Driver) resolve(ref *provider.Reference) (nodeRecord, error) {
	if ref == nil {
		return nodeRecord{}, errtypes.BadRequest("reference required")
	}
	id := ref.GetResourceId()
	if id == nil || (id.StorageId != "" && id.StorageId != d.c.MountID) || (id.SpaceId != "" && id.SpaceId != d.s.spaceID) {
		return nodeRecord{}, errtypes.NotFound("space")
	}
	base := id.OpaqueId
	if base == "" {
		base = d.s.spaceID
	}
	n, err := d.s.byID(base)
	if err != nil {
		return n, mapError(err)
	}
	if !public(n) {
		return n, errtypes.NotFound(base)
	}
	if ref.Path != "" && ref.Path != "." && ref.Path != "/" {
		p, e := cleanPath(ref.Path)
		if e != nil {
			return n, errtypes.BadRequest(e.Error())
		}
		if !n.Dir {
			return n, errtypes.BadRequest("not a directory")
		}
		n, err = d.s.byPath(path.Join(n.Path, p))
		if err != nil {
			return n, mapError(err)
		}
	}
	if !public(n) {
		return n, errtypes.NotFound(ref.Path)
	}
	if err = d.s.safe(n.Path); err != nil {
		return n, err
	}
	return n, nil
}

func (d *Driver) target(ref *provider.Reference) (nodeRecord, string, error) {
	p, err := cleanPath(ref.GetPath())
	if err != nil || p == "." {
		return nodeRecord{}, "", errtypes.BadRequest("destination filename required")
	}
	parent, err := d.resolve(&provider.Reference{ResourceId: ref.GetResourceId(), Path: path.Dir(p)})
	if err != nil {
		return parent, "", err
	}
	if !parent.Dir {
		return parent, "", errtypes.BadRequest("parent is not a directory")
	}
	return parent, path.Join(parent.Path, path.Base(p)), nil
}

func mapError(err error) error {
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, os.ErrNotExist) {
		return errtypes.NotFound("resource")
	}
	if errors.Is(err, os.ErrExist) {
		return errtypes.AlreadyExists("resource")
	}
	return err
}

func (d *Driver) info(ctx context.Context, n nodeRecord) (*provider.ResourceInfo, error) {
	p, err := d.permissions(ctx, n)
	if err != nil {
		return nil, err
	}
	if !p.Stat {
		return nil, errtypes.NotFound(n.Name)
	}
	ri := &provider.ResourceInfo{Id: d.id(n.ID), Name: n.Name, Path: "/" + strings.TrimPrefix(n.Path, "./"), Etag: n.ETag, Mtime: utils.TimeToTS(time.Unix(0, n.Mtime)), Size: uint64(max(n.Size, 0)), Owner: d.owner(), PermissionSet: p, Space: &provider.StorageSpace{
		Id:   &provider.StorageSpaceId{OpaqueId: storagespace.FormatStorageID(d.c.MountID, d.s.spaceID)},
		Root: d.id(d.s.spaceID), Name: d.c.SpaceName, SpaceType: "project", Owner: &userpb.User{Id: d.owner()},
	}}
	if n.Parent != "" {
		ri.ParentId = d.id(n.Parent)
	} else {
		ri.Path = "/"
		ri.Name = d.c.SpaceName
	}
	if !d.isOwner(ctx) && n.Parent != "" {
		visible := n.Name
		parentID := n.Parent
		for parentID != "" {
			parent, e := d.s.byID(parentID)
			if e != nil {
				return nil, e
			}
			permissions, e := d.permissions(ctx, parent)
			if e != nil {
				return nil, e
			}
			if !permissions.Stat {
				break
			}
			if parent.Name != "" {
				visible = path.Join(parent.Name, visible)
			}
			parentID = parent.Parent
		}
		ri.Path = "/" + visible
	}
	if n.Dir {
		ri.Type = provider.ResourceType_RESOURCE_TYPE_CONTAINER
		ri.MimeType = "httpd/unix-directory"
		ri.Size = d.used(n.Path)
	} else {
		ri.Type = provider.ResourceType_RESOURCE_TYPE_FILE
		ri.MimeType = mime.TypeByExtension(path.Ext(n.Name))
		if ri.MimeType == "" {
			ri.MimeType = "application/octet-stream"
		}
	}
	attrs, err := d.attributes(n.ID, "user:")
	if err != nil {
		return nil, err
	}
	ri.ArbitraryMetadata = &provider.ArbitraryMetadata{Metadata: map[string]string{}}
	for k, v := range attrs {
		ri.ArbitraryMetadata.Metadata[strings.TrimPrefix(k, "user:")] = string(v)
	}
	return ri, nil
}

func (d *Driver) used(p string) uint64 {
	var size int64
	if p == "." {
		_ = d.s.reader().QueryRow("SELECT coalesce(sum(size),0) FROM nodes WHERE dir=0 AND missing=0 AND substr(path,1,?)!=?", len(control)+1, control+"/").Scan(&size)
	} else {
		_ = d.s.reader().QueryRow("SELECT coalesce(sum(size),0) FROM nodes WHERE dir=0 AND missing=0 AND (path=? OR substr(path,1,?)=?)", p, utf8.RuneCountInString(p)+1, p+"/").Scan(&size)
	}
	return uint64(max(size, 0))
}

func (d *Driver) GetMD(ctx context.Context, ref *provider.Reference, _, _ []string) (*provider.ResourceInfo, error) {
	u, e := d.beginRefs(ctx, lockRef{ref, false})
	if e != nil {
		return nil, e
	}
	defer u()
	n, e := d.resolve(ref)
	if e != nil {
		return nil, e
	}
	if _, e = d.s.root.Stat(n.Path); e != nil {
		return nil, mapError(e)
	}
	return d.info(ctx, n)
}
func (d *Driver) ListFolder(ctx context.Context, ref *provider.Reference, _, _ []string) ([]*provider.ResourceInfo, error) {
	u, e := d.beginRefs(ctx, lockRef{ref, true})
	if e != nil {
		return nil, e
	}
	defer u()
	n, e := d.resolve(ref)
	if e != nil {
		return nil, e
	}
	if e = d.allow(ctx, n, "list"); e != nil {
		return nil, e
	}
	if !n.Dir {
		return nil, errtypes.BadRequest("not a directory")
	}
	all, e := d.s.records()
	if e != nil {
		return nil, e
	}
	out := []*provider.ResourceInfo{}
	for _, child := range all {
		if child.Parent != n.ID || !public(child) {
			continue
		}
		ri, e := d.info(ctx, child)
		if e != nil {
			var nf errtypes.NotFound
			if errors.As(e, &nf) {
				continue
			}
			return nil, e
		}
		ri.Path = ri.Name
		out = append(out, ri)
	}
	return out, nil
}
func (d *Driver) Download(ctx context.Context, ref *provider.Reference, open func(*provider.ResourceInfo) bool) (*provider.ResourceInfo, io.ReadCloser, error) {
	u, e := d.beginRefs(ctx, lockRef{ref, false})
	if e != nil {
		return nil, nil, e
	}
	defer u()
	n, e := d.resolve(ref)
	if e != nil {
		return nil, nil, e
	}
	if e = d.allow(ctx, n, "download"); e != nil {
		return nil, nil, e
	}
	if n.Dir {
		return nil, nil, errtypes.BadRequest("cannot download directory")
	}
	ri, e := d.info(ctx, n)
	if e != nil {
		return nil, nil, e
	}
	if open != nil && !open(ri) {
		return ri, nil, nil
	}
	f, e := d.s.root.Open(n.Path)
	return ri, f, mapError(e)
}
func (d *Driver) GetPathByID(ctx context.Context, id *provider.ResourceId) (string, error) {
	ri, e := d.GetMD(ctx, &provider.Reference{ResourceId: id}, nil, nil)
	if e != nil {
		return "", e
	}
	if !ri.PermissionSet.GetPath {
		return "", errtypes.PermissionDenied("get path")
	}
	return ri.Path, nil
}
func (d *Driver) GetQuota(ctx context.Context, ref *provider.Reference) (uint64, uint64, uint64, error) {
	u, e := d.beginRefs(ctx, lockRef{d.ref(d.s.spaceID), false})
	if e != nil {
		return 0, 0, 0, e
	}
	defer u()
	n, e := d.resolve(ref)
	if e == nil {
		e = d.allow(ctx, n, "quota")
	}
	if e != nil {
		return 0, 0, 0, e
	}
	return d.filesystemQuota(ctx)
}

func (d *Driver) ListStorageSpaces(ctx context.Context, filters []*provider.ListStorageSpacesRequest_Filter, _ bool) ([]*provider.StorageSpace, error) {
	u, e := d.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer u()
	n, e := d.s.byID(d.s.spaceID)
	if e != nil {
		return nil, e
	}
	ri, e := d.info(ctx, n)
	if e != nil {
		var nf errtypes.NotFound
		if errors.As(e, &nf) {
			return []*provider.StorageSpace{}, nil
		}
		return nil, e
	}
	sp := &provider.StorageSpace{Id: &provider.StorageSpaceId{OpaqueId: d.s.spaceID}, Name: d.c.SpaceName, SpaceType: "project", Root: d.id(n.ID), RootInfo: ri, Owner: &userpb.User{Id: d.owner()}, Mtime: ri.Mtime, Opaque: utils.AppendPlainToOpaque(nil, "spaceAlias", "project/"+d.s.spaceID)}
	if e := d.addSpaceMembership(sp); e != nil {
		return nil, e
	}
	for _, f := range filters {
		if id := f.GetId(); id != nil {
			parsed, err := storagespace.ParseID(id.OpaqueId)
			if err != nil || parsed.SpaceId != d.s.spaceID || parsed.StorageId != "" && parsed.StorageId != d.c.MountID || parsed.OpaqueId != "" && parsed.OpaqueId != d.s.spaceID {
				return []*provider.StorageSpace{}, nil
			}
		}
		// These registry modifiers include grants/mountpoints in addition to
		// the actual space; they are not exclusive space-type filters.
		if typ := f.GetSpaceType(); typ != "" && typ != "project" && typ != "+grant" && typ != "+mountpoint" {
			return []*provider.StorageSpace{}, nil
		}
	}
	return []*provider.StorageSpace{sp}, nil
}

func (d *Driver) CreateDir(ctx context.Context, ref *provider.Reference) (*storage.CreateDirResult, error) {
	u, e := d.beginRefs(ctx, lockRef{ref, true})
	if e != nil {
		return nil, e
	}
	defer u()
	parent, p, e := d.target(ref)
	if e != nil {
		return nil, e
	}
	if e = d.allow(ctx, parent, "mkdir"); e != nil {
		return nil, e
	}
	n := nodeRecord{ID: uuid.NewString(), Parent: parent.ID, Name: path.Base(p), Path: p, Dir: true}
	tmp := path.Join(control, "tmp", uuid.NewString())
	if e = d.s.root.Mkdir(tmp, 0700); e != nil {
		return nil, e
	}
	e = d.s.apply(ctx, operation{ID: uuid.NewString(), Kind: "create", Source: tmp, Target: p, Node: n})
	if e != nil {
		return nil, e
	}
	return &storage.CreateDirResult{SpaceOwner: d.owner(), SpaceID: d.s.spaceID, ResourceID: d.id(n.ID)}, nil
}
func (d *Driver) Move(ctx context.Context, src, dst *provider.Reference) (*storage.MoveResult, error) {
	u, e := d.beginRefs(ctx, lockRef{src, true}, lockRef{dst, true})
	if e != nil {
		return nil, e
	}
	defer u()
	n, e := d.resolve(src)
	if e != nil {
		return nil, e
	}
	if n.ID == d.s.spaceID {
		return nil, errtypes.BadRequest("cannot move root")
	}
	if e = d.allow(ctx, n, "move"); e != nil {
		return nil, e
	}
	parent, p, e := d.target(dst)
	if e != nil {
		return nil, e
	}
	if e = d.allow(ctx, parent, "upload"); e != nil {
		return nil, e
	}
	if p == n.Path {
		return &storage.MoveResult{SpaceOwner: d.owner(), OldReference: src, NewReference: dst}, nil
	}
	if strings.HasPrefix(p, n.Path+"/") {
		return nil, errtypes.BadRequest("cannot move directory into itself")
	}
	n.Parent = parent.ID
	n.Name = path.Base(p)
	e = d.s.apply(ctx, operation{ID: uuid.NewString(), Kind: "move", Source: n.Path, Target: p, Node: n})
	if e != nil {
		return nil, e
	}
	return &storage.MoveResult{SpaceOwner: d.owner(), OldReference: src, NewReference: dst}, nil
}
func (d *Driver) Delete(ctx context.Context, ref *provider.Reference) (*storage.DeleteResult, error) {
	u, e := d.beginRefs(ctx, lockRef{ref, true})
	if e != nil {
		return nil, e
	}
	defer u()
	n, e := d.resolve(ref)
	if e != nil {
		return nil, e
	}
	if n.ID == d.s.spaceID {
		return nil, errtypes.BadRequest("cannot delete root")
	}
	if e = d.allow(ctx, n, "delete"); e != nil {
		return nil, e
	}
	all, e := d.s.records()
	if e != nil {
		return nil, e
	}
	snapshot := []nodeRecord{}
	for _, child := range all {
		if child.Path == n.Path || strings.HasPrefix(child.Path, n.Path+"/") {
			snapshot = append(snapshot, child)
		}
	}
	id := uuid.NewString()
	e = d.s.apply(ctx, operation{ID: id, Kind: "trash", Source: n.Path, Target: path.Join(control, "trash", id), Node: n, Snapshot: snapshot})
	if e != nil {
		return nil, e
	}
	return &storage.DeleteResult{SpaceOwner: d.owner(), ResourceId: d.id(n.ID)}, nil
}

func (d *Driver) publish(ctx context.Context) error {
	release, err := d.s.acquire(ctx, []resourceLock{{"outbox", true}})
	if err != nil {
		return err
	}
	defer release()
	if d.stream == nil {
		return nil
	}
	rows, e := d.s.reader().Query("SELECT id,node,kind FROM outbox ORDER BY id LIMIT 100")
	if e != nil {
		return e
	}
	type event struct {
		id         int64
		node, kind string
	}
	var batch []event
	for rows.Next() {
		var ev event
		if e = rows.Scan(&ev.id, &ev.node, &ev.kind); e != nil {
			break
		}
		batch = append(batch, ev)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	for _, ev := range batch {
		var payload any
		switch ev.kind {
		case "deleted":
			payload = events.ItemTrashed{SpaceOwner: d.owner(), Executant: d.owner(), Owner: d.owner(), ID: d.id(ev.node), Ref: d.ref(ev.node), Timestamp: utils.TSNow()}
		case "directory":
			payload = events.ContainerCreated{SpaceOwner: d.owner(), Executant: d.owner(), Owner: d.owner(), Ref: d.ref(ev.node), Timestamp: utils.TSNow()}
		default:
			payload = events.FileTouched{SpaceOwner: d.owner(), Executant: d.owner(), Ref: d.ref(ev.node), Timestamp: utils.TSNow()}
		}
		if e = events.Publish(ctx, d.stream, payload); e != nil {
			return e
		}
		if _, e = d.s.db.Exec("DELETE FROM outbox WHERE id=?", ev.id); e != nil {
			return e
		}
	}
	return nil
}

func executant(ctx context.Context) (*userpb.User, error) {
	u, ok := ctxpkg.ContextGetUser(ctx)
	if !ok || u.GetId().GetOpaqueId() == "" {
		return nil, errtypes.PermissionDenied("authenticated user required")
	}
	return u, nil
}
