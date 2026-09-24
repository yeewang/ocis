package apps

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type installTransport func(*http.Request) (*http.Response, error)

func (f installTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func bundleForTest(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var b bytes.Buffer
	w := zip.NewWriter(&b)
	for name, content := range files {
		f, err := w.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func testInstaller(t *testing.T, bundle []byte, minimum, download string) *Installer {
	t.Helper()
	i := NewInstaller(filepath.Join(t.TempDir(), "apps"), nil, "8.2.0-remoteposix.10-alpine")
	i.Client.Transport = installTransport(func(r *http.Request) (*http.Response, error) {
		body := bundle
		if r.URL.String() == CatalogURL {
			body, _ = json.Marshal(map[string]any{"apps": []any{map[string]any{"id": "org.example.demo", "versions": []any{map[string]any{"version": "1.0.0", "url": download, "minOCIS": minimum}}}}})
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})
	return i
}
func TestInstallPersistsAndDoesNotOverwrite(t *testing.T) {
	b := bundleForTest(t, map[string]string{"demo/manifest.json": `{"entrypoint":"main.js"}`, "demo/main.js": "export default {}"})
	i := testInstaller(t, b, "7.0.0", "https://github.com/owncloud/marketplace/releases/download/demo/demo.zip")
	r, err := i.Install(context.Background(), "org.example.demo", "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if r.Directory != "demo" || len(r.SHA256) != 64 {
		t.Fatal(r)
	}
	second := NewInstaller(i.Root, nil, "8.2.0")
	list, err := second.Installed()
	if err != nil || len(list) != 1 || list[0].Version != "1.0.0" {
		t.Fatalf("%v %v", list, err)
	}
	if _, err = i.Install(context.Background(), "org.example.demo", "1.0.0"); !errors.Is(err, ErrInstalled) {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(i.Root, "demo/main.js"))
	if err != nil || string(got) != "export default {}" {
		t.Fatal("existing app changed")
	}
}
func TestRejectUntrustedOrIncompatibleInstall(t *testing.T) {
	for _, tc := range []struct{ minimum, endpoint, id string }{
		{"99.0.0", "https://github.com/owncloud/marketplace/releases/download/demo/demo.zip", "org.example.demo"},
		{"7.0.0", "http://127.0.0.1/private", "org.example.demo"},
		{"7.0.0", "https://github.com/evil/repo/releases/download/x/demo.zip", "org.example.demo"},
		{"7.0.0", "https://github.com/owncloud/marketplace/releases/download/demo/demo.zip", "unknown.app"},
	} {
		i := testInstaller(t, nil, tc.minimum, tc.endpoint)
		if _, err := i.Install(context.Background(), tc.id, "1.0.0"); err == nil {
			t.Fatal("unsafe install accepted")
		}
		if _, err := os.Stat(i.Root); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("failed install published files")
		}
	}
}
func TestRejectUnsafeBundles(t *testing.T) {
	for _, files := range []map[string]string{
		{"../escape": "bad"}, {"/absolute": "bad"}, {"demo/../../escape": "bad"}, {`demo\escape`: "bad"},
		{"demo/main.js": "no manifest"}, {"demo/manifest.json": `{"entrypoint":"../outside.js"}`},
		{"demo/manifest.json": `{"entrypoint":"main.js"}`},
		{"demo/manifest.json": `{"entrypoint":"main.js"}`, "demo/main.js": "ok", "other/main.js": "bad"},
	} {
		if _, err := unpackApp(bundleForTest(t, files), t.TempDir()); err == nil {
			t.Fatalf("accepted %v", files)
		}
	}
	var b bytes.Buffer
	z := zip.NewWriter(&b)
	h := &zip.FileHeader{Name: "demo/link"}
	h.SetMode(os.ModeSymlink | 0777)
	f, _ := z.CreateHeader(h)
	_, _ = f.Write([]byte("/etc/passwd"))
	_ = z.Close()
	if _, err := unpackApp(b.Bytes(), t.TempDir()); err == nil {
		t.Fatal("accepted symlink")
	}
}
func TestDownloadRedirectPolicy(t *testing.T) {
	for _, raw := range []string{"http://github.com/owncloud/marketplace/releases/download/a/b", "https://127.0.0.1/x", "https://github.com.evil.test/x", "https://user:password@github.com/owncloud/marketplace/releases/download/a/b"} {
		u, _ := url.Parse(raw)
		if trustedDownload(u, true) {
			t.Fatal(raw)
		}
	}
	i := NewInstaller(t.TempDir(), nil, "8.2.0")
	i.Client.Transport = installTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("too large")), Header: make(http.Header)}, nil
	})
	if _, err := i.get(context.Background(), CatalogURL, 3); err == nil {
		t.Fatal("download bound ignored")
	}
}
