//go:build !unix || aix

package remoteposix

import (
	"context"

	"github.com/owncloud/reva/v2/pkg/errtypes"
)

func (d *Driver) filesystemQuota(context.Context) (uint64, uint64, uint64, error) {
	return 0, 0, 0, errtypes.NotSupported("remote filesystem capacity unavailable on this platform")
}
