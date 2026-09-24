package revaconfig

import (
	"github.com/owncloud/ocis/v2/ocis-pkg/shared"
	"testing"
	"time"

	"github.com/owncloud/ocis/v2/services/storage-users/pkg/config"
	"github.com/owncloud/ocis/v2/services/storage-users/pkg/config/defaults"
	"github.com/owncloud/reva/v2/pkg/storage/fs/registry"
)

func TestRemotePosixProviderConfiguration(t *testing.T) {
	c := defaults.FullDefaultConfig()
	c.Commons = &shared.Commons{GRPCClientTLS: &shared.GRPCClientTLS{}}
	c.MountID = "mount"
	c.Drivers.RemotePosix = config.RemotePosixDriver{Root: "/remote", StateDir: "/local", OwnerID: "owner", OwnerIDP: "idp"}
	metadata := StorageProviderDrivers(c)["remoteposix"].(map[string]interface{})
	data := DataProviderDrivers(c)["remoteposix"].(map[string]interface{})
	if metadata["state_dir"] != "/local" || data["state_dir"] != metadata["state_dir"] {
		t.Fatal("providers must share local SQLite state")
	}
	if metadata["watch"] != true || data["watch"] != false {
		t.Fatal("only the metadata provider should poll")
	}
	if metadata["scan_interval"] != time.Minute || metadata["missing_grace_period"] != 10*time.Minute {
		t.Fatal("invalid defaults")
	}
	if registry.NewFuncs["remoteposix"] == nil {
		t.Fatal("driver not registered")
	}
}
