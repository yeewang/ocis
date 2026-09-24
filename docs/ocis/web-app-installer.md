# Installing Web apps from the App Store

Set `WEB_APP_INSTALLER_ENABLED=true` on the main oCIS/web service to enable
installation from the official marketplace. It is disabled by default. Sign in
with the server Administrator role, open App Store, click **Install**, and then
**Reload to use app**. A Team/space Manager cannot install server applications.

The backend accepts only an app ID and release version. It resolves the release
against `https://marketplace.owncloud.com/api/ocis/v1/apps.json`, checks the minimum
oCIS version, and downloads only official marketplace GitHub release assets.
Custom catalog apps remain available for manual download and installation.

Bundles are limited to 100 MiB compressed, 500 MiB expanded, and 10,000 entries.
Traversal, symlinks, multiple app directories, and missing/invalid entrypoints
are rejected. Installation stages outside the served directory, then atomically
publishes the complete app in `WEB_ASSET_APPS_PATH`. ID, version and SHA-256 are
recorded in `.ocis-install.json` alongside the manifest. This records the fetched
bundle digest; it is not a publisher signature verification mechanism.

Existing applications, including bundled and manually installed apps, are never
overwritten. Updating and uninstalling remain administrator filesystem tasks.
App configuration is discovered on each config request, so installation requires
only a browser reload, not an oCIS restart. Preserve the app asset volume when
recreating containers. Each web replica must have access to that shared app volume.

Extensions run in the browser with the user's session; installation therefore
requires server administration privileges. Installing an extension does not
automatically relax CSP or configure external services that the extension needs.
