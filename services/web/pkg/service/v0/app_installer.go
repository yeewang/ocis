package svc

import (
	"context"
	"encoding/json"
	"errors"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	permissionsapi "github.com/cs3org/go-cs3apis/cs3/permissions/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	revactx "github.com/owncloud/reva/v2/pkg/ctx"
	"google.golang.org/grpc/metadata"
	"net/http"

	"github.com/owncloud/ocis/v2/ocis-pkg/version"
	"github.com/owncloud/ocis/v2/services/web/pkg/apps"
	"github.com/owncloud/reva/v2/pkg/auth/scope"
	"github.com/owncloud/reva/v2/pkg/token"
	"github.com/owncloud/reva/v2/pkg/token/manager/jwt"
)

type appInstaller struct {
	manager   *apps.Installer
	tokens    token.Manager
	authorize func(context.Context, *userpb.UserId) (bool, error)
}

func newAppInstaller(o Options) *appInstaller {
	tokens, _ := jwt.New(map[string]interface{}{"secret": o.Config.TokenManager.JWTSecret, "expires": int64(86400)})
	return &appInstaller{manager: apps.NewInstaller(o.Config.Asset.AppsPath, o.AppFS, version.GetString()), tokens: tokens, authorize: func(ctx context.Context, id *userpb.UserId) (bool, error) {
		client, err := o.GatewaySelector.Next()
		if err != nil {
			return false, err
		}
		response, err := client.CheckPermission(ctx, &permissionsapi.CheckPermissionRequest{
			Permission: "Roles.ReadWrite", SubjectRef: &permissionsapi.SubjectReference{Spec: &permissionsapi.SubjectReference_UserId{UserId: id}},
		})
		if err != nil {
			return false, err
		}
		return response != nil && response.Status != nil && response.Status.Code == rpc.Code_CODE_OK, nil
	}}
}

func (s *appInstaller) requireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if s.tokens == nil || r.Header.Get("x-access-token") == "" {
			http.Error(w, "Authentication required", http.StatusUnauthorized)
			return
		}
		user, scopes, err := s.tokens.DismantleToken(r.Context(), r.Header.Get("x-access-token"))
		if err != nil {
			http.Error(w, "Invalid authentication", http.StatusUnauthorized)
			return
		}
		if ok, err := scope.VerifyScope(r.Context(), scopes, r.URL.Path); err != nil || !ok {
			http.Error(w, "Token scope does not permit app installation", http.StatusForbidden)
			return
		}
		if user.GetId().GetOpaqueId() == "" || s.authorize == nil {
			http.Error(w, "Only server administrators can install applications", http.StatusForbidden)
			return
		}
		ctx := revactx.ContextSetUser(r.Context(), user)
		ctx = metadata.AppendToOutgoingContext(ctx, "x-access-token", r.Header.Get("x-access-token"))
		allowed, err := s.authorize(ctx, user.GetId())
		if err != nil {
			http.Error(w, "Cannot check administrator permissions", http.StatusServiceUnavailable)
			return
		}
		if !allowed {
			http.Error(w, "Only server administrators can install applications", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *appInstaller) list(w http.ResponseWriter, r *http.Request) {
	installed, err := s.manager.Installed()
	if err != nil {
		http.Error(w, "Cannot read installed applications", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"installed": installed})
}

func (s *appInstaller) install(w http.ResponseWriter, r *http.Request) {
	var request struct {
		ID      string `json:"id"`
		Version string `json:"version"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "Invalid installation request", http.StatusBadRequest)
		return
	}
	installed, err := s.manager.Install(r.Context(), request.ID, request.Version)
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, apps.ErrInstalled) || errors.Is(err, apps.ErrInstallBusy) {
			code = http.StatusConflict
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]string{"message": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(installed)
}
