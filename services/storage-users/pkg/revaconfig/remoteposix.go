package revaconfig

import (
	"github.com/owncloud/ocis/v2/services/storage-users/pkg/config"
	"time"
)

// RemotePosix maps both provider instances to the same local state directory.
func RemotePosix(cfg *config.Config, watch bool) map[string]interface{} {
	c := cfg.Drivers.RemotePosix
	if c.ScanInterval == 0 {
		c.ScanInterval = time.Minute
	}
	if c.MissingGrace == 0 {
		c.MissingGrace = 10 * time.Minute
	}
	if c.SpaceName == "" {
		c.SpaceName = "Remote storage"
	}
	return map[string]interface{}{
		"root": c.Root, "state_dir": c.StateDir,
		"owner_id": c.OwnerID, "owner_idp": c.OwnerIDP,
		"space_name": c.SpaceName, "mount_id": cfg.MountID,
		"transfer_secret": cfg.TransferSecret, "data_server_url": cfg.DataServerURL,
		"scan_interval": c.ScanInterval, "missing_grace_period": c.MissingGrace,
		"watch": watch,
	}
}
