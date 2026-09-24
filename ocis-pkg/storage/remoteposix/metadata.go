package remoteposix

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/google/uuid"
	"github.com/owncloud/reva/v2/pkg/errtypes"
	"google.golang.org/protobuf/proto"
)

func (d *Driver) attributes(id, prefix string) (map[string][]byte, error) {
	rows, e := d.s.reader().Query("SELECT key,value FROM attributes WHERE node=? AND substr(key,1,?)=?", id, len(prefix), prefix)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	result := map[string][]byte{}
	for rows.Next() {
		var k string
		var v []byte
		if e = rows.Scan(&k, &v); e != nil {
			return nil, e
		}
		result[k] = v
	}
	return result, rows.Err()
}

func fullPermissions() *provider.ResourcePermissions {
	return &provider.ResourcePermissions{Stat: true, GetPath: true, GetQuota: true, ListContainer: true, InitiateFileDownload: true, InitiateFileUpload: true, CreateContainer: true, Move: true, Delete: true, ListFileVersions: true, RestoreFileVersion: true, ListRecycle: true, RestoreRecycleItem: true, PurgeRecycle: true, AddGrant: true, RemoveGrant: true, UpdateGrant: true, ListGrants: true, DenyGrant: true}
}

func (d *Driver) isOwner(ctx context.Context) bool {
	u, e := executant(ctx)
	return e == nil && u.Id.OpaqueId == d.c.OwnerID && u.Id.Idp == d.c.OwnerIDP
}

func (d *Driver) permissions(ctx context.Context, n nodeRecord) (*provider.ResourcePermissions, error) {
	u, e := executant(ctx)
	if e != nil {
		return nil, e
	}
	if d.isOwner(ctx) {
		return fullPermissions(), nil
	}
	p := &provider.ResourcePermissions{}
	seen := map[string]bool{}
	for {
		if seen[n.ID] {
			return nil, errors.New("metadata parent cycle")
		}
		seen[n.ID] = true
		attrs, e := d.attributes(n.ID, "grant:")
		if e != nil {
			return nil, e
		}
		for _, b := range attrs {
			g := &provider.Grant{}
			if e = proto.Unmarshal(b, g); e != nil {
				return nil, e
			}
			match := false
			if id := g.Grantee.GetUserId(); id != nil {
				match = id.OpaqueId == u.Id.OpaqueId && id.Idp == u.Id.Idp
			}
			if id := g.Grantee.GetGroupId(); id != nil && id.Idp == u.Id.Idp {
				for _, group := range u.Groups {
					if group == id.OpaqueId {
						match = true
					}
				}
			}
			if !match {
				continue
			}
			if g.Permissions == nil {
				return &provider.ResourcePermissions{}, nil
			}
			// Boolean permissions are additive; a deny anywhere on the chain wins.
			proto.Merge(p, g.Permissions)
		}
		if n.Parent == "" {
			break
		}
		n, e = d.s.byID(n.Parent)
		if e != nil {
			return nil, e
		}
	}
	return p, nil
}

func (d *Driver) allow(ctx context.Context, n nodeRecord, action string) error {
	p, e := d.permissions(ctx, n)
	if e != nil {
		return e
	}
	allowed := false
	switch action {
	case "stat":
		allowed = p.Stat
	case "path":
		allowed = p.GetPath
	case "quota":
		allowed = p.GetQuota
	case "list":
		allowed = p.ListContainer
	case "download":
		allowed = p.InitiateFileDownload
	case "upload":
		allowed = p.InitiateFileUpload
	case "mkdir":
		allowed = p.CreateContainer
	case "move":
		allowed = p.Move
	case "delete":
		allowed = p.Delete
	case "versions":
		allowed = p.ListFileVersions
	case "restoreversion":
		allowed = p.RestoreFileVersion
	case "trash":
		allowed = p.ListRecycle
	case "restore":
		allowed = p.RestoreRecycleItem
	case "purge":
		allowed = p.PurgeRecycle
	case "grants":
		allowed = p.ListGrants
	case "metadata":
		allowed = p.InitiateFileUpload
	}
	if !allowed {
		return errtypes.PermissionDenied(action)
	}
	return nil
}

func grantKey(g *provider.Grantee) (string, error) {
	if g == nil {
		return "", errtypes.BadRequest("grantee required")
	}
	if g.GetUserId() == nil && g.GetGroupId() == nil {
		return "", errtypes.BadRequest("user or group grantee required")
	}
	b, e := proto.MarshalOptions{Deterministic: true}.Marshal(g)
	return "grant:" + base64.RawURLEncoding.EncodeToString(b), e
}
func (d *Driver) changeGrant(ctx context.Context, ref *provider.Reference, g *provider.Grant, remove bool) error {
	u, e := d.beginRefs(ctx, lockRef{ref, true})
	if e != nil {
		return e
	}
	defer u()
	if !d.isOwner(ctx) {
		return errtypes.PermissionDenied("only the configured owner may change grants")
	}
	n, e := d.resolve(ref)
	if e != nil {
		return e
	}
	if g == nil {
		return errtypes.BadRequest("grant required")
	}
	k, e := grantKey(g.Grantee)
	if e != nil {
		return e
	}
	if remove {
		_, e = d.s.db.Exec("DELETE FROM attributes WHERE node=? AND key=?", n.ID, k)
	} else {
		b, err := proto.Marshal(g)
		if err != nil {
			return err
		}
		_, e = d.s.db.Exec("INSERT INTO attributes VALUES (?,?,?) ON CONFLICT(node,key) DO UPDATE SET value=excluded.value", n.ID, k, b)
	}
	return e
}
func (d *Driver) AddGrant(ctx context.Context, r *provider.Reference, g *provider.Grant) error {
	return d.changeGrant(ctx, r, g, false)
}
func (d *Driver) UpdateGrant(ctx context.Context, r *provider.Reference, g *provider.Grant) error {
	return d.changeGrant(ctx, r, g, false)
}
func (d *Driver) RemoveGrant(ctx context.Context, r *provider.Reference, g *provider.Grant) error {
	return d.changeGrant(ctx, r, g, true)
}
func (d *Driver) DenyGrant(ctx context.Context, r *provider.Reference, g *provider.Grantee) error {
	return d.changeGrant(ctx, r, &provider.Grant{Grantee: g}, false)
}
func (d *Driver) ListGrants(ctx context.Context, r *provider.Reference) ([]*provider.Grant, error) {
	u, e := d.beginRefs(ctx, lockRef{r, false})
	if e != nil {
		return nil, e
	}
	defer u()
	n, e := d.resolve(r)
	if e != nil {
		return nil, e
	}
	if e = d.allow(ctx, n, "grants"); e != nil {
		return nil, e
	}
	attrs, e := d.attributes(n.ID, "grant:")
	if e != nil {
		return nil, e
	}
	out := []*provider.Grant{}
	for _, b := range attrs {
		g := &provider.Grant{}
		if e = proto.Unmarshal(b, g); e != nil {
			return nil, e
		}
		out = append(out, g)
	}
	return out, nil
}

func (d *Driver) editMetadata(ctx context.Context, r *provider.Reference, set map[string]string, remove []string) error {
	u, e := d.beginRefs(ctx, lockRef{r, true})
	if e != nil {
		return e
	}
	defer u()
	n, e := d.resolve(r)
	if e != nil {
		return e
	}
	if e = d.allow(ctx, n, "metadata"); e != nil {
		return e
	}
	tx, e := d.s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for k, v := range set {
		if k == "" || strings.ContainsRune(k, 0) {
			return errtypes.BadRequest("invalid metadata key")
		}
		if _, e = tx.Exec("INSERT INTO attributes VALUES (?,?,?) ON CONFLICT(node,key) DO UPDATE SET value=excluded.value", n.ID, "user:"+k, []byte(v)); e != nil {
			return e
		}
	}
	for _, k := range remove {
		if _, e = tx.Exec("DELETE FROM attributes WHERE node=? AND key=?", n.ID, "user:"+k); e != nil {
			return e
		}
	}
	if _, e = tx.Exec("UPDATE nodes SET etag=? WHERE id=?", uuid.NewString(), n.ID); e != nil {
		return e
	}
	if e = refreshDirectories(tx, n.Path); e != nil {
		return e
	}
	return tx.Commit()
}
func (d *Driver) SetArbitraryMetadata(ctx context.Context, r *provider.Reference, md *provider.ArbitraryMetadata) error {
	return d.editMetadata(ctx, r, md.GetMetadata(), nil)
}
func (d *Driver) UnsetArbitraryMetadata(ctx context.Context, r *provider.Reference, keys []string) error {
	return d.editMetadata(ctx, r, nil, keys)
}
