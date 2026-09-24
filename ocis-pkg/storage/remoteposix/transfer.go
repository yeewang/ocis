package remoteposix

import (
	"net/http"
	"strings"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	ctxpkg "github.com/owncloud/reva/v2/pkg/ctx"
	"github.com/owncloud/reva/v2/pkg/errtypes"
)

type uploadTransferClaims struct {
	jwt.RegisteredClaims
	Target string `json:"target"`
}

// AuthorizeUploadRequest restores the session identity only for a valid transfer
// capability signed by the gateway for this exact provider and upload session.
// A session UUID alone never grants access. Ordinary authenticated requests still
// go through the datastore's session-owner and resource-permission checks.
func (d *Driver) AuthorizeUploadRequest(r *http.Request) (*http.Request, error) {
	if _, err := executant(r.Context()); err == nil || r.Method == http.MethodOptions {
		return r, nil
	}
	denied := errtypes.PermissionDenied("valid upload transfer required")
	method := r.Method
	if override := r.Header.Get("X-HTTP-Method-Override"); override != "" {
		if method != http.MethodPost || override != http.MethodPatch {
			return nil, denied
		}
		method = override
	}
	if method != http.MethodPatch && method != http.MethodHead && method != http.MethodDelete {
		return nil, denied
	}
	id := strings.TrimPrefix(r.URL.Path, "/")
	if _, err := uuid.Parse(id); err != nil || r.URL.Path != "/"+id {
		return nil, denied
	}
	if d.c.TransferSecret == "" || d.c.DataServerURL == "" {
		return nil, denied
	}
	claims := &uploadTransferClaims{}
	token, err := jwt.ParseWithClaims(r.Header.Get("X-Reva-Transfer"), claims, func(*jwt.Token) (interface{}, error) {
		return []byte(d.c.TransferSecret), nil
	}, jwt.WithValidMethods([]string{"HS256"}), jwt.WithAudience("reva"), jwt.WithExpirationRequired())
	if err != nil || !token.Valid || claims.Target != strings.TrimRight(d.c.DataServerURL, "/")+"/tus/"+id {
		return nil, denied
	}
	s, err := d.loadSession(id)
	if err != nil || s.User == "" || s.IDP == "" {
		return nil, denied
	}
	ctx := ctxpkg.ContextSetUser(r.Context(), &userpb.User{
		Id: &userpb.UserId{OpaqueId: s.User, Idp: s.IDP}, Groups: s.Groups,
	})
	return r.WithContext(ctx), nil
}
