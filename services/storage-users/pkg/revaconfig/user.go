package revaconfig

import (
	_ "github.com/owncloud/ocis/v2/ocis-pkg/storage/remoteposix"
	"github.com/owncloud/ocis/v2/services/storage-users/pkg/config"
)

// StorageProviderDrivers are the drivers for the storage provider
func StorageProviderDrivers(cfg *config.Config) map[string]interface{} {
	return map[string]interface{}{
		"remoteposix": RemotePosix(cfg, true),
		"eos":         EOS(cfg),
		"eoshome":     EOSHome(cfg),
		"eosgrpc":     EOSGRPC(cfg),
		"local":       Local(cfg),
		"localhome":   LocalHome(cfg),
		"owncloudsql": OwnCloudSQL(cfg),
		"ocis":        OcisNoEvents(cfg),
		"s3":          S3(cfg),
		"s3ng":        S3NGNoEvents(cfg),
		"posix":       Posix(cfg, true),
	}
}

// DataProviderDrivers are the drivers for the storage provider
func DataProviderDrivers(cfg *config.Config) map[string]interface{} {
	return map[string]interface{}{
		"remoteposix": RemotePosix(cfg, false),
		"eos":         EOS(cfg),
		"eoshome":     EOSHome(cfg),
		"eosgrpc":     EOSGRPC(cfg),
		"local":       Local(cfg),
		"localhome":   LocalHome(cfg),
		"owncloudsql": OwnCloudSQL(cfg),
		"ocis":        Ocis(cfg),
		"s3":          S3(cfg),
		"s3ng":        S3NG(cfg),
		"posix":       Posix(cfg, false),
	}
}
