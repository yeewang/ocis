package remoteposix

import (
	"context"
	"net/url"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/owncloud/reva/v2/pkg/errtypes"
	"github.com/owncloud/reva/v2/pkg/storage"
)

func (d *Driver) CreateReference(context.Context, string, *url.URL) error {
	return errtypes.NotSupported("remote reference mounts")
}
func (d *Driver) CreateStorageSpace(context.Context, *provider.CreateStorageSpaceRequest) (*provider.CreateStorageSpaceResponse, error) {
	return nil, errtypes.NotSupported("one configured project space per root")
}
func (d *Driver) UpdateStorageSpace(context.Context, *provider.UpdateStorageSpaceRequest) (*provider.UpdateStorageSpaceResponse, error) {
	return nil, errtypes.NotSupported("configure the project space through storage-users configuration")
}
func (d *Driver) DeleteStorageSpace(context.Context, *provider.DeleteStorageSpaceRequest) (*storage.DeleteStorageSpaceResult, error) {
	return nil, errtypes.NotSupported("configured remote root cannot be deleted")
}
func (d *Driver) CreateHome(context.Context) error {
	return errtypes.NotSupported("project spaces only")
}
func (d *Driver) GetHome(context.Context) (string, error) {
	return "", errtypes.NotSupported("project spaces only")
}
func (d *Driver) GetLock(context.Context, *provider.Reference) (*provider.Lock, error) {
	return nil, nil
}
func (d *Driver) SetLock(context.Context, *provider.Reference, *provider.Lock) (*storage.SetLockResult, error) {
	return nil, errtypes.NotSupported("WebDAV locks")
}
func (d *Driver) RefreshLock(context.Context, *provider.Reference, *provider.Lock, string) error {
	return errtypes.NotSupported("WebDAV locks")
}
func (d *Driver) Unlock(context.Context, *provider.Reference, *provider.Lock) (*storage.UnlockResult, error) {
	return nil, errtypes.NotSupported("WebDAV locks")
}
