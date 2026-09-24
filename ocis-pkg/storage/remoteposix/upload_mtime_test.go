package remoteposix

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestUploadMtime(t *testing.T) {
	for value, want := range map[string]time.Time{
		"1727000000":           time.Unix(1727000000, 0),
		"1727000000.123":       time.Unix(1727000000, 123000000),
		"1727000000.001":       time.Unix(1727000000, 1000000),
		"1727000000.123456789": time.Unix(1727000000, 123456789),
		"-0.5":                 time.Unix(-1, 500000000),
	} {
		t.Run(value, func(t *testing.T) {
			root := t.TempDir()
			d := testDriver(t, root, t.TempDir())
			ctx := ownerContext()
			ids, err := d.InitiateUpload(ctx, at(d, "web.txt"), 6, map[string]string{"mtime": value})
			if err != nil {
				t.Fatal(err)
			}
			u, err := d.GetUpload(ctx, ids["tus"])
			if err != nil {
				t.Fatal(err)
			}
			if _, err = u.WriteChunk(ctx, 0, strings.NewReader("abc")); err != nil {
				t.Fatal(err)
			}
			if _, err = u.WriteChunk(ctx, 3, strings.NewReader("def")); err != nil {
				t.Fatal(err)
			}
			if err = u.FinishUpload(ctx); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(filepath.Join(root, "web.txt"))
			if err != nil {
				t.Fatal(err)
			}
			if !info.ModTime().Equal(want) {
				t.Fatalf("mtime = %v, want %v", info.ModTime(), want)
			}
			if got := readContent(t, d, at(d, "web.txt")); got != "abcdef" {
				t.Fatalf("content = %q", got)
			}
		})
	}
}

func TestRejectInvalidUploadMtime(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	for _, value := range []string{"NaN", "Infinity", "1727000000.", "1727000000.-1", "1727000000.1.2", "1727000000.1234567890", "9223372036854775807", "-9223372036854775808", "1e9"} {
		t.Run(value, func(t *testing.T) {
			if _, err := d.InitiateUpload(ownerContext(), at(d, "bad.txt"), 1, map[string]string{"mtime": value}); err == nil {
				t.Fatal("accepted invalid mtime")
			}
		})
	}
}
