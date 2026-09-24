package remoteposix

import (
	"context"
	"testing"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	ctxpkg "github.com/owncloud/reva/v2/pkg/ctx"
)

func sharingGrant(name string, permissions *provider.ResourcePermissions) *provider.Grant {
	return &provider.Grant{Grantee: &provider.Grantee{Type: provider.GranteeType_GRANTEE_TYPE_USER, Id: &provider.Grantee_UserId{UserId: &userpb.UserId{OpaqueId: name, Idp: "test"}}}, Permissions: permissions}
}

func TestDelegatedSharing(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	root := d.ref(d.s.spaceID)
	file := uploadFile(t, d, "shared.txt", "hello")
	ref := d.ref(file.Id.OpaqueId)
	member := ctxpkg.ContextSetUser(context.Background(), &userpb.User{Id: &userpb.UserId{OpaqueId: "member", Idp: "test"}})
	reader := sharingGrant("reader", &provider.ResourcePermissions{Stat: true, InitiateFileDownload: true})
	if err := d.AddGrant(member, ref, reader); err == nil {
		t.Fatal("unprivileged member shared a file")
	}
	permissions := &provider.ResourcePermissions{Stat: true, InitiateFileDownload: true, ListGrants: true, AddGrant: true, UpdateGrant: true, RemoveGrant: true}
	if err := d.AddGrant(ownerContext(), root, sharingGrant("member", permissions)); err != nil {
		t.Fatal(err)
	}
	for _, target := range []*provider.Reference{root, ref} {
		if err := d.AddGrant(member, target, reader); err != nil {
			t.Fatal(err)
		}
		if err := d.UpdateGrant(member, target, sharingGrant("reader", &provider.ResourcePermissions{Stat: true})); err != nil {
			t.Fatal(err)
		}
		if err := d.RemoveGrant(member, target, reader); err != nil {
			t.Fatal(err)
		}
	}
	strong := sharingGrant("privileged", &provider.ResourcePermissions{Stat: true, PurgeRecycle: true})
	if err := d.AddGrant(member, ref, strong); err == nil {
		t.Fatal("member delegated a permission they lack")
	}
	if err := d.AddGrant(ownerContext(), ref, strong); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateGrant(member, ref, sharingGrant("privileged", reader.Permissions)); err == nil {
		t.Fatal("member downgraded a stronger grant")
	}
	if err := d.RemoveGrant(member, ref, strong); err == nil {
		t.Fatal("member removed a stronger grant")
	}
	if err := d.DenyGrant(member, ref, reader.Grantee); err == nil {
		t.Fatal("member created a deny without DenyGrant")
	}
	if err := d.AddGrant(member, ref, sharingGrant("reader", nil)); err == nil {
		t.Fatal("member bypassed deny check through AddGrant")
	}
	if err := d.DenyGrant(ownerContext(), ref, reader.Grantee); err != nil {
		t.Fatal(err)
	}
	if err := d.AddGrant(member, ref, reader); err == nil {
		t.Fatal("member replaced an owner deny")
	}
	if err := d.RemoveGrant(member, ref, reader); err == nil {
		t.Fatal("member removed an owner deny")
	}
}

func TestSharingRequiresSpecificPermission(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	root := d.ref(d.s.spaceID)
	member := ctxpkg.ContextSetUser(context.Background(), &userpb.User{Id: &userpb.UserId{OpaqueId: "member", Idp: "test"}})
	reader := sharingGrant("reader", &provider.ResourcePermissions{Stat: true})
	if err := d.AddGrant(ownerContext(), root, sharingGrant("member", &provider.ResourcePermissions{Stat: true, AddGrant: true})); err != nil {
		t.Fatal(err)
	}
	if err := d.AddGrant(member, root, reader); err != nil {
		t.Fatal(err)
	}
	if err := d.AddGrant(member, root, reader); err == nil {
		t.Fatal("AddGrant replaced a grant without UpdateGrant")
	}
	if err := d.UpdateGrant(member, root, reader); err == nil {
		t.Fatal("UpdateGrant allowed without permission")
	}
	if err := d.RemoveGrant(member, root, reader); err == nil {
		t.Fatal("RemoveGrant allowed without permission")
	}
	if err := d.AddGrant(ownerContext(), root, sharingGrant("member", &provider.ResourcePermissions{Stat: true, UpdateGrant: true})); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateGrant(member, root, sharingGrant("new-reader", reader.Permissions)); err == nil {
		t.Fatal("UpdateGrant created a grant without AddGrant")
	}
}

func TestSharingSpaceIDLookup(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	for id, want := range map[string]int{
		d.s.spaceID:                     1,
		d.c.MountID + "$" + d.s.spaceID: 1,
		d.c.MountID + "$" + d.s.spaceID + "!" + d.s.spaceID: 1,
		"another-provider$" + d.s.spaceID:                   0,
		d.c.MountID + "$another-space":                      0,
		d.c.MountID + "$" + d.s.spaceID + "!another-file":   0,
	} {
		spaces, err := d.ListStorageSpaces(ownerContext(), []*provider.ListStorageSpacesRequest_Filter{{
			Type: provider.ListStorageSpacesRequest_Filter_TYPE_ID,
			Term: &provider.ListStorageSpacesRequest_Filter_Id{Id: &provider.StorageSpaceId{OpaqueId: id}},
		}}, false)
		if err != nil || len(spaces) != want {
			t.Fatalf("lookup %q: %d spaces, error %v", id, len(spaces), err)
		}
	}
	for _, modifier := range []string{"+grant", "+mountpoint"} {
		spaces, err := d.ListStorageSpaces(ownerContext(), []*provider.ListStorageSpacesRequest_Filter{{
			Type: provider.ListStorageSpacesRequest_Filter_TYPE_SPACE_TYPE,
			Term: &provider.ListStorageSpacesRequest_Filter_SpaceType{SpaceType: modifier},
		}}, false)
		if err != nil || len(spaces) != 1 {
			t.Fatalf("registry modifier %q: %d spaces, error %v", modifier, len(spaces), err)
		}
	}
	info, err := d.GetMD(ownerContext(), d.ref(d.s.spaceID), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if info.GetSpace().GetId().GetOpaqueId() != d.c.MountID+"$"+d.s.spaceID || info.GetSpace().GetSpaceType() != "project" {
		t.Fatal("resource metadata lacks the qualified project space identity required by Graph sharing")
	}
}
