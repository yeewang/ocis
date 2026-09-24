//go:build unix && !aix

package remoteposix

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
)

func tryFileLock(f *os.File, write bool) (bool, error) {
	mode := unix.LOCK_SH
	if write {
		mode = unix.LOCK_EX
	}
	err := unix.Flock(int(f.Fd()), mode|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return false, nil
	}
	return err == nil, err
}
