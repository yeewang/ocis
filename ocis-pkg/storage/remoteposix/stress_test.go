package remoteposix

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/owncloud/reva/v2/pkg/storage"
)

// Opt in because this performs thousands of durable filesystem/SQLite writes.
// REMOTEPOSIX_STRESS=1 go test -race -v -count=1 -timeout=10m -run '^TestThousandFilesWriteRead$' ./ocis-pkg/storage/remoteposix
func TestThousandFilesWriteRead(t *testing.T) {
	if os.Getenv("REMOTEPOSIX_STRESS") != "1" {
		t.Skip("set REMOTEPOSIX_STRESS=1 to run the 1000-file workload")
	}
	const files, size, workers = 1000, 64 * 1024, 32
	root, state := t.TempDir(), t.TempDir()
	drivers := []*Driver{testDriver(t, root, state), testDriver(t, root, state)}
	ctx, cancel := context.WithTimeout(ownerContext(), 9*time.Minute)
	defer cancel()
	ids := make([]*provider.ResourceId, files)
	hashes := make([][32]byte, files)
	payload := func(i int) []byte {
		data := make([]byte, size)
		for j := range data {
			data[j] = byte((i*31 + j*17 + j/251) % 256)
		}
		copy(data, fmt.Sprintf("file-%04d\n", i))
		return data
	}
	run := func(name string, fn func(int, int) error) {
		t.Helper()
		start := time.Now()
		jobs := make(chan int, files)
		errs := make(chan error, files)
		var completed atomic.Int64
		for i := 0; i < files; i++ {
			jobs <- i
		}
		close(jobs)
		var wg sync.WaitGroup
		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for i := range jobs {
					if err := fn(i, worker); err != nil {
						errs <- fmt.Errorf("%s file %d: %w", name, i, err)
					}
					if n := completed.Add(1); n%250 == 0 {
						t.Logf("%s: %d/%d complete", name, n, files)
					}
				}
			}(worker)
		}
		wg.Wait()
		close(errs)
		failures := 0
		for err := range errs {
			failures++
			if failures <= 10 {
				t.Log(err)
			}
		}
		elapsed := time.Since(start)
		t.Logf("%s: files=%d bytes/file=%d workers=%d elapsed=%s files/s=%.1f MiB/s=%.2f failures=%d", name, files, size, workers, elapsed, float64(files)/elapsed.Seconds(), float64(files*size)/(1<<20)/elapsed.Seconds(), failures)
		if failures != 0 {
			t.Fatalf("%s failed for %d files", name, failures)
		}
	}
	run("write", func(i, worker int) error {
		d := drivers[worker%len(drivers)]
		data := payload(i)
		hashes[i] = sha256.Sum256(data)
		session, err := d.InitiateUpload(ctx, at(d, fmt.Sprintf("file-%04d.bin", i)), size, nil)
		if err != nil {
			return err
		}
		info, err := d.Upload(ctx, storage.UploadRequest{Ref: &provider.Reference{Path: session["simple"]}, Body: io.NopCloser(bytes.NewReader(data)), Length: size}, nil)
		if err != nil {
			return err
		}
		if info.Size != size {
			return fmt.Errorf("wrong metadata size: %d", info.Size)
		}
		ids[i] = info.Id
		return nil
	})
	seen := make(map[string]bool, files)
	for _, id := range ids {
		if seen[id.OpaqueId] {
			t.Fatalf("duplicate file ID: %s", id.OpaqueId)
		}
		seen[id.OpaqueId] = true
	}
	read := func(i, worker int) error {
		d := drivers[(worker+1)%len(drivers)]
		info, r, err := d.Download(ctx, &provider.Reference{ResourceId: ids[i]}, nil)
		if err != nil {
			return err
		}
		h := sha256.New()
		n, err := io.Copy(h, r)
		closeErr := r.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if n != size || info.Size != size || !bytes.Equal(h.Sum(nil), hashes[i][:]) {
			return fmt.Errorf("content or size mismatch: bytes=%d metadata=%d", n, info.Size)
		}
		return nil
	}
	run("read-verify", read)
	for _, d := range drivers {
		if err := d.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	drivers = []*Driver{testDriver(t, root, state), testDriver(t, root, state)}
	run("read-after-restart", read)
	for query, want := range map[string]int{
		"SELECT count(*) FROM nodes WHERE dir=0 AND missing=0":                   files,
		"SELECT count(*) FROM operations":                                        0,
		"SELECT count(*) FROM completed_operations WHERE kind='create'":          files,
		"SELECT count(*) FROM uploads WHERE json_extract(body,'$.Result') != ''": files,
	} {
		var got int
		if err := drivers[0].s.reader().QueryRowContext(ctx, query).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s: got %d, want %d", query, got, want)
		}
	}
	t.Log("verified 1000 unique files, committed upload results, completion receipts, and an empty operation journal")
}
