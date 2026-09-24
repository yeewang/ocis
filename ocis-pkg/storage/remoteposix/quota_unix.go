//go:build unix && !aix

package remoteposix

import (
	"context"
	"math"

	"golang.org/x/sys/unix"
)

// The space has no configured quota. Its effective capacity is its live file
// usage plus the bytes available to the mount user. Other directories, versions
// and trash consume filesystem capacity without counting as live space usage.
func (d *Driver) filesystemQuota(ctx context.Context) (uint64, uint64, uint64, error) {
	f, err := d.s.root.Open(".")
	if err != nil {
		return 0, 0, 0, mapError(err)
	}
	defer f.Close()
	var stat unix.Statfs_t
	if err = unix.Fstatfs(int(f.Fd()), &stat); err != nil {
		return 0, 0, 0, mapError(err)
	}
	// Recheck the configured mount after statfs so an unmount does not report
	// capacity from a detached root or the local mountpoint filesystem.
	if err = d.s.healthy(); err != nil {
		return 0, 0, 0, mapError(err)
	}
	var used int64
	err = d.s.reader().QueryRowContext(ctx, "SELECT coalesce(sum(size),0) FROM nodes WHERE dir=0 AND missing=0 AND substr(path,1,?)!=?", len(control)+1, control+"/").Scan(&used)
	if err != nil {
		return 0, 0, 0, mapError(err)
	}
	// Some platforms report negative availability when over quota.
	var blocks uint64
	if stat.Bavail > 0 {
		blocks = uint64(stat.Bavail)
	}
	var blockSize uint64
	if stat.Bsize > 0 {
		blockSize = uint64(stat.Bsize)
	}
	total, usage, remaining := quotaBytes(uint64(max(used, 0)), blocks, blockSize)
	return total, usage, remaining, nil
}

func quotaBytes(used, blocks, blockSize uint64) (uint64, uint64, uint64) {
	// Graph represents quota as int64. Saturate before multiplication/addition.
	used = min(used, uint64(math.MaxInt64))
	limit := uint64(math.MaxInt64) - used
	remaining := limit
	if blockSize == 0 || blocks <= limit/blockSize {
		remaining = blocks * blockSize
	}
	return used + remaining, used, remaining
}
