package remoteposix

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/google/uuid"
	"github.com/owncloud/reva/v2/pkg/errtypes"
	"github.com/owncloud/reva/v2/pkg/utils"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// addSpaceMembership exposes the root grants consumed by Graph and Web when
// deciding which actions a project-space member can perform.
func (d *Driver) addSpaceMembership(sp *provider.StorageSpace) error {
	attrs, err := d.attributes(d.s.spaceID, "grant:")
	if err != nil {
		return err
	}
	permissions := map[string]*provider.ResourcePermissions{}
	groups := map[string]struct{}{}
	for _, data := range attrs {
		grant := &provider.Grant{}
		if err := proto.Unmarshal(data, grant); err != nil {
			return err
		}
		var id string
		if user := grant.Grantee.GetUserId(); user != nil && user.Idp == d.c.OwnerIDP {
			id = user.OpaqueId
		} else if group := grant.Grantee.GetGroupId(); group != nil && group.Idp == d.c.OwnerIDP {
			id = group.OpaqueId
			groups[id] = struct{}{}
		}
		if id == "" {
			continue
		}
		permissions[id] = grant.Permissions
		if permissions[id] == nil {
			permissions[id] = &provider.ResourcePermissions{}
		}
	}
	permissions[d.c.OwnerID] = fullPermissions()
	for key, value := range map[string]interface{}{"grants": permissions, "groups": groups} {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		sp.Opaque = utils.AppendPlainToOpaque(sp.Opaque, key, string(data))
	}
	return nil
}

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
func (d *Driver) changeGrant(ctx context.Context, ref *provider.Reference, g *provider.Grant, action string) error {
	u, e := d.beginRefs(ctx, lockRef{ref, true})
	if e != nil {
		return e
	}
	defer u()
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
	if !d.isOwner(ctx) {
		permissions, err := d.permissions(ctx, n)
		if err != nil {
			return err
		}
		allowed := map[string]bool{"add": permissions.AddGrant, "update": permissions.UpdateGrant, "remove": permissions.RemoveGrant, "deny": permissions.DenyGrant}
		if !allowed[action] {
			return errtypes.PermissionDenied("grant " + action)
		}
		if action != "remove" && (g.Permissions == nil && !permissions.DenyGrant || !permissionsWithin(g.Permissions, permissions)) {
			return errtypes.PermissionDenied("cannot grant permissions beyond your own")
		}
		// AddGrant is an upsert. Replacing an existing grant also requires
		// update permission, and delegated editors cannot alter stronger grants.
		var previous []byte
		err = d.s.reader().QueryRowContext(ctx, "SELECT value FROM attributes WHERE node=? AND key=?", n.ID, k).Scan(&previous)
		if err == nil {
			old := &provider.Grant{}
			if err = proto.Unmarshal(previous, old); err != nil {
				return err
			}
			if action == "add" && !permissions.UpdateGrant || old.Permissions == nil && !permissions.DenyGrant || !permissionsWithin(old.Permissions, permissions) {
				return errtypes.PermissionDenied("cannot change this grant")
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		} else if action == "update" && !permissions.AddGrant {
			return errtypes.PermissionDenied("creating a grant requires add permission")
		}
	}
	if action == "remove" {
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

func permissionsWithin(requested, available *provider.ResourcePermissions) bool {
	if requested == nil {
		return true
	}
	if len(requested.ProtoReflect().GetUnknown()) != 0 {
		return false
	}
	allowed := true
	requested.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		allowed = field.Kind() == protoreflect.BoolKind && (!value.Bool() || available.ProtoReflect().Get(field).Bool())
		return allowed
	})
	return allowed
}
func (d *Driver) AddGrant(ctx context.Context, r *provider.Reference, g *provider.Grant) error {
	return d.changeGrant(ctx, r, g, "add")
}
func (d *Driver) UpdateGrant(ctx context.Context, r *provider.Reference, g *provider.Grant) error {
	return d.changeGrant(ctx, r, g, "update")
}
func (d *Driver) RemoveGrant(ctx context.Context, r *provider.Reference, g *provider.Grant) error {
	return d.changeGrant(ctx, r, g, "remove")
}
func (d *Driver) DenyGrant(ctx context.Context, r *provider.Reference, g *provider.Grantee) error {
	return d.changeGrant(ctx, r, &provider.Grant{Grantee: g}, "deny")
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
