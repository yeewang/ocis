package remoteposix

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	ctxpkg "github.com/owncloud/reva/v2/pkg/ctx"
)

func TestUploadTransferAuthorization(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	d.c.TransferSecret = "test-transfer-secret"
	d.c.DataServerURL = "http://localhost:9258/data"
	ctx := ctxpkg.ContextSetUser(context.Background(), &userpb.User{Id: d.owner(), Groups: []string{"team"}})
	ids, err := d.InitiateUpload(ctx, at(d, "browser.txt"), 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := ids["tus"]
	target := d.c.DataServerURL + "/tus/" + id
	sign := func(target, secret, audience string, expires time.Time) string {
		t.Helper()
		claims := uploadTransferClaims{Target: target, RegisteredClaims: jwt.RegisteredClaims{Audience: jwt.ClaimStrings{audience}}}
		if !expires.IsZero() {
			claims.ExpiresAt = jwt.NewNumericDate(expires)
		}
		s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	valid := sign(target, d.c.TransferSecret, "reva", time.Now().Add(time.Hour))
	for _, tt := range []struct {
		name, method, path, token string
		allowed                   bool
	}{
		{"patch", "PATCH", "/" + id, valid, true},
		{"resume-head", "HEAD", "/" + id, valid, true},
		{"terminate", "DELETE", "/" + id, valid, true},
		{"missing-token", "PATCH", "/" + id, "", false},
		{"wrong-session", "PATCH", "/" + uuid.NewString(), valid, false},
		{"read-stage", "GET", "/" + id, valid, false},
		{"wrong-provider", "PATCH", "/" + id, sign("http://elsewhere/data/tus/"+id, d.c.TransferSecret, "reva", time.Now().Add(time.Hour)), false},
		{"wrong-secret", "PATCH", "/" + id, sign(target, "other", "reva", time.Now().Add(time.Hour)), false},
		{"expired", "PATCH", "/" + id, sign(target, d.c.TransferSecret, "reva", time.Now().Add(-time.Hour)), false},
		{"no-expiration", "PATCH", "/" + id, sign(target, d.c.TransferSecret, "reva", time.Time{}), false},
		{"wrong-audience", "PATCH", "/" + id, sign(target, d.c.TransferSecret, "other", time.Now().Add(time.Hour)), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, tt.path, nil)
			r.Header.Set("X-Reva-Transfer", tt.token)
			got, err := d.AuthorizeUploadRequest(r)
			if (err == nil) != tt.allowed {
				t.Fatalf("allowed=%v, err=%v", tt.allowed, err)
			}
			if tt.allowed {
				u, err := executant(got.Context())
				if err != nil || u.Id.OpaqueId != d.c.OwnerID || len(u.Groups) != 1 {
					t.Fatal("session identity not restored", err)
				}
			}
		})
	}
	if _, err := d.GetUpload(context.Background(), id); err == nil {
		t.Fatal("anonymous session UUID granted access")
	}
	r := httptest.NewRequest(http.MethodPatch, "/"+id, nil)
	r.Header.Set("X-Reva-Transfer", valid)
	r, err = d.AuthorizeUploadRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	u, err := d.GetUpload(r.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = u.WriteChunk(r.Context(), 0, strings.NewReader("abc")); err != nil {
		t.Fatal(err)
	}
	if err = u.FinishUpload(r.Context()); err != nil {
		t.Fatal(err)
	}
	if got := readContent(t, d, at(d, "browser.txt")); got != "abc" {
		t.Fatalf("content=%q", got)
	}
}
