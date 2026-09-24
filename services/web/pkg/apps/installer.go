package apps

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/rogpeppe/go-internal/lockedfile"
)

const CatalogURL = "https://marketplace.owncloud.com/api/ocis/v1/apps.json"
const installRecord = ".ocis-install.json"
const maxBundle = 100 << 20
const maxExpanded = 500 << 20

var ErrInstalled = errors.New("application is already installed")
var ErrInstallBusy = errors.New("another installation is in progress")
var appDirectory = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,99}$`)

type InstalledApp struct {
	ID        string `json:"id"`
	Directory string `json:"directory"`
	Version   string `json:"version,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
}

type Installer struct {
	Root          string
	Existing      fs.FS
	ServerVersion string
	Client        *http.Client
	mu            sync.Mutex
}

func NewInstaller(root string, existing fs.FS, version string) *Installer {
	return &Installer{Root: root, Existing: existing, ServerVersion: version, Client: &http.Client{
		Timeout: 2 * time.Minute,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 || !trustedDownload(req.URL, true) {
				return errors.New("untrusted download redirect")
			}
			return nil
		},
	}}
}

func trustedDownload(u *url.URL, redirect bool) bool {
	if u.Scheme != "https" || u.User != nil || u.Port() != "" {
		return false
	}
	if u.Host == "github.com" && strings.HasPrefix(u.Path, "/owncloud/marketplace/releases/download/") {
		return true
	}
	return redirect && (u.Host == "release-assets.githubusercontent.com" || u.Host == "objects.githubusercontent.com")
}

func (i *Installer) get(ctx context.Context, endpoint string, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := i.Client.Do(req)
	if err != nil {
		return nil, errors.New("marketplace download failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("marketplace returned HTTP %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if int64(len(b)) > limit {
		return nil, errors.New("download exceeds size limit")
	}
	return b, err
}

func (i *Installer) Installed() ([]InstalledApp, error) {
	entries, err := os.ReadDir(i.Root)
	if errors.Is(err, os.ErrNotExist) {
		return []InstalledApp{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := []InstalledApp{}
	for _, entry := range entries {
		if !entry.IsDir() || !appDirectory.MatchString(entry.Name()) {
			continue
		}
		if _, err := os.Stat(filepath.Join(i.Root, entry.Name(), "manifest.json")); err != nil {
			continue
		}
		record := InstalledApp{Directory: entry.Name()}
		if b, err := os.ReadFile(filepath.Join(i.Root, entry.Name(), installRecord)); err == nil {
			if err := json.Unmarshal(b, &record); err != nil {
				return nil, err
			}
		}
		result = append(result, record)
	}
	return result, nil
}

func (i *Installer) Install(ctx context.Context, id, version string) (InstalledApp, error) {
	if !i.mu.TryLock() {
		return InstalledApp{}, ErrInstallBusy
	}
	defer i.mu.Unlock()
	if id == "" || version == "" {
		return InstalledApp{}, errors.New("application ID and version are required")
	}
	b, err := i.get(ctx, CatalogURL, 8<<20)
	if err != nil {
		return InstalledApp{}, err
	}
	var catalog struct {
		Apps []struct {
			ID       string `json:"id"`
			Versions []struct {
				Version string `json:"version"`
				URL     string `json:"url"`
				Minimum string `json:"minOCIS"`
			} `json:"versions"`
		} `json:"apps"`
	}
	if err := json.Unmarshal(b, &catalog); err != nil {
		return InstalledApp{}, errors.New("invalid marketplace catalog")
	}
	endpoint := ""
	for _, app := range catalog.Apps {
		if app.ID != id {
			continue
		}
		for _, release := range app.Versions {
			if release.Version != version {
				continue
			}
			if release.Minimum != "" {
				minimum, e1 := semver.NewVersion(release.Minimum)
				current, e2 := semver.NewVersion(strings.SplitN(i.ServerVersion, "-", 2)[0])
				if e1 != nil || e2 != nil || current.LessThan(minimum) {
					return InstalledApp{}, fmt.Errorf("this app requires oCIS %s or newer", release.Minimum)
				}
			}
			endpoint = release.URL
		}
	}
	u, err := url.Parse(endpoint)
	if err != nil || !trustedDownload(u, false) {
		return InstalledApp{}, errors.New("release is not available from the trusted marketplace")
	}
	bundle, err := i.get(ctx, endpoint, maxBundle)
	if err != nil {
		return InstalledApp{}, err
	}
	if err = os.MkdirAll(i.Root, 0755); err != nil {
		return InstalledApp{}, err
	}
	// Outside the served app directory: incomplete and failed installs are never visible.
	staging := filepath.Join(filepath.Dir(i.Root), ".app-install-staging")
	if err = os.MkdirAll(staging, 0700); err != nil {
		return InstalledApp{}, err
	}
	lock, err := lockedfile.OpenFile(filepath.Join(staging, "install.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return InstalledApp{}, err
	}
	defer lock.Close()
	tmp, err := os.MkdirTemp(staging, "install-")
	if err != nil {
		return InstalledApp{}, err
	}
	defer os.RemoveAll(tmp)
	name, err := unpackApp(bundle, tmp)
	if err != nil {
		return InstalledApp{}, err
	}
	if i.Existing != nil {
		if _, err := fs.Stat(i.Existing, name+"/manifest.json"); err == nil {
			return InstalledApp{}, ErrInstalled
		}
	}
	if _, err = os.Lstat(filepath.Join(i.Root, name)); !errors.Is(err, os.ErrNotExist) {
		return InstalledApp{}, ErrInstalled
	}
	sum := sha256.Sum256(bundle)
	record := InstalledApp{ID: id, Directory: name, Version: version, SHA256: hex.EncodeToString(sum[:])}
	data, _ := json.Marshal(record)
	if err = os.WriteFile(filepath.Join(tmp, name, installRecord), data, 0644); err != nil {
		return InstalledApp{}, err
	}
	if err = ctx.Err(); err != nil {
		return InstalledApp{}, err
	}
	if err = os.Rename(filepath.Join(tmp, name), filepath.Join(i.Root, name)); err != nil {
		return InstalledApp{}, err
	}
	return record, nil
}

func unpackApp(bundle []byte, destination string) (string, error) {
	z, err := zip.NewReader(bytes.NewReader(bundle), int64(len(bundle)))
	if err != nil {
		return "", errors.New("invalid app ZIP")
	}
	if len(z.File) > 10000 {
		return "", errors.New("too many files in app ZIP")
	}
	name := ""
	var expanded uint64
	seen := map[string]bool{}
	for _, f := range z.File {
		p := strings.TrimSuffix(f.Name, "/")
		parts := strings.Split(p, "/")
		if !fs.ValidPath(p) || strings.ContainsAny(p, "\\:\x00") || !appDirectory.MatchString(parts[0]) || seen[p] || f.Mode()&(os.ModeSymlink|os.ModeDevice|os.ModeNamedPipe|os.ModeSocket) != 0 {
			return "", errors.New("unsafe path or file type in app ZIP")
		}
		seen[p] = true
		if name == "" {
			name = parts[0]
		}
		if parts[0] != name {
			return "", errors.New("app ZIP must contain exactly one application directory")
		}
		if f.UncompressedSize64 > maxExpanded-expanded {
			return "", errors.New("app ZIP exceeds expanded size limit")
		}
		expanded += f.UncompressedSize64
		dest := filepath.Join(destination, filepath.FromSlash(p))
		if f.FileInfo().IsDir() {
			if err = os.MkdirAll(dest, 0755); err != nil {
				return "", err
			}
			continue
		}
		if len(parts) < 2 {
			return "", errors.New("app files must be inside an application directory")
		}
		if err = os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
			return "", err
		}
		r, err := f.Open()
		if err != nil {
			return "", err
		}
		out, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			r.Close()
			return "", err
		}
		n, copyErr := io.Copy(out, io.LimitReader(r, int64(f.UncompressedSize64)+1))
		closeErr := out.Close()
		r.Close()
		if copyErr != nil || closeErr != nil || uint64(n) != f.UncompressedSize64 {
			return "", errors.New("app file extraction failed")
		}
	}
	manifest, err := os.ReadFile(filepath.Join(destination, name, "manifest.json"))
	if err != nil {
		return "", errors.New("app manifest missing")
	}
	var app Application
	if json.Unmarshal(manifest, &app) != nil || !fs.ValidPath(app.Entrypoint) || strings.ContainsAny(app.Entrypoint, "\\:") || (path.Ext(app.Entrypoint) != ".js" && path.Ext(app.Entrypoint) != ".mjs") {
		return "", errors.New("invalid app entrypoint")
	}
	entry, err := os.Stat(filepath.Join(destination, name, filepath.FromSlash(app.Entrypoint)))
	if err != nil || !entry.Mode().IsRegular() {
		return "", errors.New("app entrypoint missing")
	}
	return name, nil
}
