//go:build unix && !aix

package remoteposix

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestFilesystemQuota(t *testing.T) {
	root := t.TempDir()
	d := testDriver(t, root, t.TempDir())
	uploadFile(t, d, "visible.txt", "abc")
	// Control files occupy disk but must not count as live Team file usage.
	if err := os.WriteFile(filepath.Join(root, control, "quota-test"), []byte("private data"), 0600); err != nil {
		t.Fatal(err)
	}
	var before, after unix.Statfs_t
	if err := unix.Statfs(root, &before); err != nil {
		t.Fatal(err)
	}
	total, used, remaining, err := d.GetQuota(ownerContext(), at(d, "."))
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Statfs(root, &after); err != nil {
		t.Fatal(err)
	}
	a, b := uint64(before.Bavail)*uint64(before.Bsize), uint64(after.Bavail)*uint64(after.Bsize)
	// Allow a little concurrent filesystem activity during the system calls.
	const tolerance = 16 << 20
	if used != 3 || total != used+remaining || total > math.MaxInt64 || remaining+tolerance < min(a, b) || remaining > max(a, b)+tolerance {
		t.Fatalf("quota total=%d used=%d remaining=%d; filesystem availability before=%d after=%d", total, used, remaining, a, b)
	}
	if _, _, _, err := d.GetQuota(context.Background(), at(d, ".")); err == nil {
		t.Fatal("anonymous quota access succeeded")
	}
	// An uncovered mountpoint must not report the local filesystem's capacity.
	if err := os.Rename(root, root+"-detached"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(root); _ = os.Rename(root+"-detached", root) })
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := d.GetQuota(ownerContext(), at(d, ".")); err == nil {
		t.Fatal("reported quota for a missing remote mount")
	}
}

func TestQuotaByteBounds(t *testing.T) {
	for _, tc := range []struct{ used, blocks, size, total, remaining uint64 }{
		{3, 10, 4096, 40963, 40960},
		{3, 0, 4096, 3, 0},
		{3, math.MaxUint64, 4096, math.MaxInt64, math.MaxInt64 - 3},
		{math.MaxUint64, 1, 4096, math.MaxInt64, 0},
		{3, 10, 0, 3, 0},
	} {
		total, used, remaining := quotaBytes(tc.used, tc.blocks, tc.size)
		if total != tc.total || remaining != tc.remaining || used+remaining != total {
			t.Fatalf("quotaBytes(%d,%d,%d) = %d,%d,%d", tc.used, tc.blocks, tc.size, total, used, remaining)
		}
	}
}
