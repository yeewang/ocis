package remoteposix

import (
	"context"
	"encoding/json"
	"testing"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	ctxpkg "github.com/owncloud/reva/v2/pkg/ctx"
)

func TestSpaceListingIncludesUploadMembership(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	member := &userpb.UserId{OpaqueId: "editor", Idp: "test"}
	ctx := ctxpkg.ContextSetUser(context.Background(), &userpb.User{Id: member})
	grant := &provider.Grant{
		Grantee:     &provider.Grantee{Type: provider.GranteeType_GRANTEE_TYPE_USER, Id: &provider.Grantee_UserId{UserId: member}},
		Permissions: &provider.ResourcePermissions{Stat: true, GetPath: true, ListContainer: true, InitiateFileUpload: true, CreateContainer: true},
	}
	ref := d.ref(d.s.spaceID)
	if err := d.AddGrant(ownerContext(), ref, grant); err != nil {
		t.Fatal(err)
	}
	check := func(ctx context.Context, wantUpload bool) {
		t.Helper()
		spaces, err := d.ListStorageSpaces(ctx, nil, false)
		if err != nil || len(spaces) != 1 {
			t.Fatalf("space listing: %v, %d", err, len(spaces))
		}
		var grants map[string]*provider.ResourcePermissions
		if err := json.Unmarshal(spaces[0].Opaque.Map["grants"].Value, &grants); err != nil {
			t.Fatal(err)
		}
		if grants["editor"].GetInitiateFileUpload() != wantUpload {
			t.Fatalf("Graph membership upload permission: %v", grants["editor"])
		}
		if !grants["owner"].GetInitiateFileUpload() || !grants["owner"].GetAddGrant() {
			t.Fatal("owner membership missing")
		}
		if u, _ := executant(ctx); u.Id.OpaqueId == "editor" && spaces[0].RootInfo.PermissionSet.InitiateFileUpload != wantUpload {
			t.Fatal("root permissions disagree with membership")
		}
	}
	check(ctx, true)
	check(ownerContext(), true)
	grant.Permissions.InitiateFileUpload = false
	if err := d.UpdateGrant(ownerContext(), ref, grant); err != nil {
		t.Fatal(err)
	}
	check(ctx, false)
	if err := d.RemoveGrant(ownerContext(), ref, grant); err != nil {
		t.Fatal(err)
	}
	check(ownerContext(), false)
}
