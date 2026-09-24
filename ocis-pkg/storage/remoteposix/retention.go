package remoteposix

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path"
	"strings"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/owncloud/reva/v2/pkg/errtypes"
	"github.com/owncloud/reva/v2/pkg/storage"
	"github.com/owncloud/reva/v2/pkg/utils"
)

type retained struct {
	ID, Node, Path, Kind string
	Size, Time           int64
	Snapshot             []byte
}

func (d *Driver) retained(key string) (r retained, e error) {
	e = d.s.reader().QueryRow("SELECT id,node,path,kind,size,time,snapshot FROM retained WHERE id=?", key).Scan(&r.ID, &r.Node, &r.Path, &r.Kind, &r.Size, &r.Time, &r.Snapshot)
	return r, mapError(e)
}
func (d *Driver) retention(kind, node string) ([]retained, error) {
	rows, e := d.s.reader().Query("SELECT id,node,path,kind,size,time,snapshot FROM retained WHERE kind=? AND (?='' OR node=?) ORDER BY time DESC", kind, node, node)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []retained{}
	for rows.Next() {
		var r retained
		if e = rows.Scan(&r.ID, &r.Node, &r.Path, &r.Kind, &r.Size, &r.Time, &r.Snapshot); e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (d *Driver) ListRevisions(ctx context.Context, ref *provider.Reference) ([]*provider.FileVersion, error) {
	u, e := d.beginRefs(ctx, lockRef{ref, false})
	if e != nil {
		return nil, e
	}
	defer u()
	n, e := d.resolve(ref)
	if e != nil {
		return nil, e
	}
	if e = d.allow(ctx, n, "versions"); e != nil {
		return nil, e
	}
	items, e := d.retention("version", n.ID)
	if e != nil {
		return nil, e
	}
	out := []*provider.FileVersion{}
	for _, r := range items {
		out = append(out, &provider.FileVersion{Key: r.ID, Size: uint64(r.Size), Mtime: uint64(time.Unix(0, r.Time).Unix()), Etag: r.ID})
	}
	return out, nil
}
func (d *Driver) DownloadRevision(ctx context.Context, ref *provider.Reference, key string, open func(*provider.ResourceInfo) bool) (*provider.ResourceInfo, io.ReadCloser, error) {
	u, e := d.beginRefs(ctx, lockRef{ref, false})
	if e != nil {
		return nil, nil, e
	}
	defer u()
	n, e := d.resolve(ref)
	if e != nil {
		return nil, nil, e
	}
	if e = d.allow(ctx, n, "versions"); e != nil {
		return nil, nil, e
	}
	if e = d.allow(ctx, n, "download"); e != nil {
		return nil, nil, e
	}
	r, e := d.retained(key)
	if e != nil {
		return nil, nil, e
	}
	if r.Node != n.ID || r.Kind != "version" {
		return nil, nil, errtypes.NotFound(key)
	}
	ri, e := d.info(ctx, n)
	if e != nil {
		return nil, nil, e
	}
	ri.Etag = r.ID
	ri.Size = uint64(r.Size)
	ri.Mtime = utils.TimeToTS(time.Unix(0, r.Time))
	if open != nil && !open(ri) {
		return ri, nil, nil
	}
	f, e := d.s.root.Open(path.Join(control, "versions", r.ID))
	return ri, f, e
}
func (d *Driver) RestoreRevision(ctx context.Context, ref *provider.Reference, key string) (*storage.RestoreRevisionResult, error) {
	u, e := d.beginRefs(ctx, lockRef{ref, true})
	if e != nil {
		return nil, e
	}
	defer u()
	n, e := d.resolve(ref)
	if e != nil {
		return nil, e
	}
	if e = d.allow(ctx, n, "restoreversion"); e != nil {
		return nil, e
	}
	r, e := d.retained(key)
	if e != nil {
		return nil, e
	}
	if r.Node != n.ID || r.Kind != "version" {
		return nil, errtypes.NotFound(key)
	}
	parent, e := d.s.byID(n.Parent)
	if e != nil {
		return nil, e
	}
	f, e := d.s.root.Open(path.Join(control, "versions", r.ID))
	if e != nil {
		return nil, e
	}
	defer f.Close()
	_, e = d.write(ctx, parent, n.Name, f, r.Size, n.ID, n.ETag)
	if e != nil {
		return nil, e
	}
	return &storage.RestoreRevisionResult{SpaceOwner: d.owner()}, nil
}

// Trash administration is restricted to the root owner. File-level grants do
// not confer access to other users' deleted resources.
func (d *Driver) trashAccess(ctx context.Context, ref *provider.Reference) error {
	if !d.isOwner(ctx) {
		return errtypes.PermissionDenied("trash owner required")
	}
	n, e := d.resolve(ref)
	if e != nil {
		return e
	}
	if n.ID != d.s.spaceID {
		return errtypes.BadRequest("trash reference must be the space root")
	}
	return nil
}
func (d *Driver) ListRecycle(ctx context.Context, ref *provider.Reference, key, relative string) ([]*provider.RecycleItem, error) {
	u, e := d.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer u()
	if e = d.trashAccess(ctx, ref); e != nil {
		return nil, e
	}
	items, e := d.retention("trash", "")
	if e != nil {
		return nil, e
	}
	out := []*provider.RecycleItem{}
	if key != "" || relative != "" {
		return nil, errtypes.NotSupported("list trash roots; restore or purge a complete item")
	}
	for _, r := range items {
		var nodes []nodeRecord
		if e = json.Unmarshal(r.Snapshot, &nodes); e != nil {
			return nil, e
		}
		typ := provider.ResourceType_RESOURCE_TYPE_FILE
		for _, n := range nodes {
			if n.ID == r.Node && n.Dir {
				typ = provider.ResourceType_RESOURCE_TYPE_CONTAINER
			}
		}
		out = append(out, &provider.RecycleItem{Key: r.ID, Type: typ, Ref: &provider.Reference{ResourceId: d.id(d.s.spaceID), Path: r.Path}, Size: uint64(max(r.Size, 0)), DeletionTime: utils.TimeToTS(time.Unix(0, r.Time))})
	}
	return out, nil
}

// Lock retained content by stable ID; an old parent may already be purged.
func (d *Driver) beginTrash(ctx context.Context, key string, dst *provider.Reference, restore bool) (func(), retained, *provider.Reference, error) {
	leave, err := d.s.enter()
	if err != nil {
		return nil, retained{}, dst, err
	}
	r, err := d.retained(key)
	if err != nil {
		leave()
		return nil, r, dst, err
	}
	refs := []lockRef{{d.ref(r.Node), true}}
	if restore {
		if dst == nil {
			dst = &provider.Reference{ResourceId: d.id(d.s.spaceID), Path: r.Path}
		}
		refs = append(refs, lockRef{dst, true})
	}
	release, err := d.beginRefs(ctx, refs...)
	if err != nil {
		leave()
		return nil, r, dst, err
	}
	r, err = d.retained(key)
	if err != nil {
		release()
		leave()
		return nil, r, dst, err
	}
	return func() { release(); leave() }, r, dst, nil
}

func (d *Driver) RestoreRecycleItem(ctx context.Context, ref *provider.Reference, key, relative string, dst *provider.Reference) (*storage.RestoreRecycleItemResult, error) {
	u, r, destination, e := d.beginTrash(ctx, key, dst, true)
	dst = destination
	if e != nil {
		return nil, e
	}
	defer u()
	if e = d.trashAccess(ctx, ref); e != nil {
		return nil, e
	}
	if relative != "" {
		return nil, errtypes.NotSupported("restore a complete trash item")
	}
	if r.Kind != "trash" {
		return nil, errtypes.NotFound(key)
	}
	parent, p, e := d.target(dst)
	if e != nil {
		return nil, e
	}
	var snapshot []nodeRecord
	if e = json.Unmarshal(r.Snapshot, &snapshot); e != nil {
		return nil, e
	}
	var n nodeRecord
	for _, v := range snapshot {
		if v.ID == r.Node {
			n = v
		}
	}
	n.Parent = parent.ID
	n.Name = path.Base(p)
	e = d.s.apply(ctx, operation{ID: r.ID, Kind: "restore", Source: path.Join(control, "trash", r.ID), Target: p, Node: n, Snapshot: snapshot})
	if e != nil {
		return nil, e
	}
	return &storage.RestoreRecycleItemResult{SpaceOwner: d.owner()}, nil
}
func (d *Driver) purge(ctx context.Context, key string) error {
	r, e := d.retained(key)
	if e != nil {
		return e
	}
	if r.Kind != "trash" {
		return errtypes.NotFound(key)
	}
	var snapshot []nodeRecord
	if e = json.Unmarshal(r.Snapshot, &snapshot); e != nil {
		return e
	}
	var versionIDs []string
	for _, n := range snapshot {
		versions, err := d.retention("version", n.ID)
		if err != nil {
			return err
		}
		for _, v := range versions {
			if err = d.s.root.Remove(path.Join(control, "versions", v.ID)); err != nil && !os.IsNotExist(err) {
				return err
			}
			versionIDs = append(versionIDs, v.ID)
		}
	}
	// Deletion is idempotent. Retain the metadata until all content is gone, so
	// a crash or partial remote failure can be retried using the same trash key.
	p := path.Join(control, "trash", r.ID)
	if e = d.s.root.RemoveAll(p); e != nil {
		return e
	}
	if e = d.s.syncDirs(path.Join(control, "trash"), path.Join(control, "versions")); e != nil {
		return e
	}
	tx, e := d.s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, id := range versionIDs {
		if _, e = tx.Exec("DELETE FROM retained WHERE id=?", id); e != nil {
			return e
		}
	}
	if _, e = tx.Exec("DELETE FROM nodes WHERE path=? OR substr(path,1,?)=?", p, len(p)+1, p+"/"); e != nil {
		return e
	}
	if _, e = tx.Exec("DELETE FROM retained WHERE id=?", key); e != nil {
		return e
	}
	return tx.Commit()
}
func (d *Driver) PurgeRecycleItem(ctx context.Context, ref *provider.Reference, key, relative string) error {
	u, _, _, e := d.beginTrash(ctx, key, nil, false)
	if e != nil {
		return e
	}
	defer u()
	if e = d.trashAccess(ctx, ref); e != nil {
		return e
	}
	if strings.Trim(relative, "/.") != "" {
		return errtypes.NotSupported("purge a complete trash item")
	}
	return d.purge(ctx, key)
}
func (d *Driver) EmptyRecycle(ctx context.Context, ref *provider.Reference) error {
	u, e := d.begin(ctx)
	if e != nil {
		return e
	}
	defer u()
	if e = d.trashAccess(ctx, ref); e != nil {
		return e
	}
	items, e := d.retention("trash", "")
	if e != nil {
		return e
	}
	for _, r := range items {
		if e = d.purge(ctx, r.ID); e != nil {
			return e
		}
	}
	return nil
}
