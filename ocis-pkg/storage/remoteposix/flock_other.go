//go:build !unix || aix

package remoteposix

import (
	"errors"
	"os"
)

func tryFileLock(*os.File, bool) (bool, error) {
	return false, errors.New("remoteposix requires POSIX flock support")
}
