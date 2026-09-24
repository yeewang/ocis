package svc

import (
	"context"
	"errors"
	authpb "github.com/cs3org/go-cs3apis/cs3/auth/provider/v1beta1"
	"net/http"
	"net/http/httptest"
	"testing"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	types "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	"github.com/owncloud/reva/v2/pkg/token/manager/jwt"
)

func TestInstallerRequiresSignedServerAdmin(t *testing.T) {
	tokens, err := jwt.New(map[string]interface{}{"secret": "test-installer-secret", "expires": int64(60)})
	if err != nil {
		t.Fatal(err)
	}
	s := appInstaller{tokens: tokens}
	for _, tc := range []struct {
		roles   string
		allowed bool
		authErr error
		want    int
	}{
		{`[]`, true, nil, 204},
		{`["71881883-1768-46bd-a24d-a356a2afdf7f"]`, false, nil, 403},
		{`[]`, false, nil, 403}, {`["312c0871-5ef7-4b3a-85b6-0e4074c64049"]`, false, nil, 403},
		{`[]`, false, errors.New("unavailable"), 503},
	} {
		s.authorize = func(ctx context.Context, id *userpb.UserId) (bool, error) {
			if id.GetOpaqueId() != "user" {
				t.Fatal("wrong authorization subject")
			}
			return tc.allowed, tc.authErr
		}
		u := &userpb.User{Id: &userpb.UserId{OpaqueId: "user", Idp: "test"}, Opaque: &types.Opaque{Map: map[string]*types.OpaqueEntry{"roles": {Decoder: "plain", Value: []byte(tc.roles)}}}}
		token, err := tokens.MintToken(context.Background(), u, map[string]*authpb.Scope{"user": {Role: authpb.Role_ROLE_OWNER}})
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("POST", "/api/app-store/install", nil)
		r.Header.Set("x-access-token", token)
		w := httptest.NewRecorder()
		s.requireAdmin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })).ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("roles=%s HTTP %d: %s", tc.roles, w.Code, w.Body.String())
		}
	}
	for _, token := range []string{"", "forged-token"} {
		r := httptest.NewRequest("POST", "/api/app-store/install", nil)
		r.Header.Set("x-access-token", token)
		w := httptest.NewRecorder()
		s.requireAdmin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Fatal("unauthorized handler ran") })).ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatal(w.Code)
		}
	}
}
