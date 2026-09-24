---
title: "Remote POSIX tree with local SQLite metadata"
---

`remoteposix` is an experimental storage-users driver for an existing, mounted
directory tree. It imports that directory as one project space. Current file
contents stay at their ordinary paths. File IDs, grants, arbitrary metadata,
upload sessions, trash/version indexes and recovery records live in a local
SQLite database. No extended attributes, hard links, symlinks or inotify are
required. Existing `ocis`, `s3ng` and `posix` drivers are unchanged.

## Build

Build on Linux with the Go version specified in `go.mod` and a C compiler.
Optional oCIS build tags may require additional native libraries. SQLite uses the repository's
vendored `github.com/mattn/go-sqlite3`, so CGO must be enabled:

```sh
CGO_ENABLED=1 go build -o ./ocis/bin/ocis-remoteposix-linux-amd64 ./ocis/cmd/ocis
go test -race ./ocis-pkg/storage/remoteposix ./services/storage-users/pkg/revaconfig
```

## Configure

### Docker image

From the repository root, build the complete Web UI and the Linux amd64 binary
with CGO SQLite and VIPS enabled, then package the runtime:

```sh
docker build --platform linux/amd64 \
  -f ocis/docker/Dockerfile.remoteposix.build \
  --build-arg VERSION=8.2.0-remoteposix.3-alpine \
  --build-arg REVISION="$(git rev-parse HEAD)" \
  --output type=local,dest=ocis/dist/binaries .
docker build --platform linux/amd64 \
  -f ocis/docker/Dockerfile.remoteposix \
  --build-arg VERSION=8.2.0-remoteposix.3-alpine \
  --build-arg REVISION="$(git rev-parse HEAD)" \
  -t ocis-remoteposix:8.2.0-remoteposix.3-alpine ocis
docker run --rm ocis-remoteposix:8.2.0-remoteposix.3-alpine version --skip-services
```

The runtime Dockerfile is an exact copy of `ocis/docker/Dockerfile.linux.amd64`:
`amd64/alpine:3.24.2`, all original packages (including `vips=8.18.2-r0`), labels,
UID/GID 1000, permissions, volumes, working directory, port and entrypoint are
preserved. The binary is compiled against Alpine musl with the release build's
tags and default configuration/data paths. The embedded Web UI, login assets,
translations and standard oCIS services are included. Its default command is
`server`. `Dockerfile.remoteposix.dockerignore` limits the runtime build context;
the original runtime Dockerfile can also be used with the same binary.
The driver is included but is selected through the configuration below; ordinary
oCIS initialization and deployment configuration are still required.

For a Linux deployment, bind-mount the already mounted remote tree at
`/mnt/remote`, and configure that path as the driver root. Persist `/etc/ocis`
and `/var/lib/ocis` on local storage. Set the driver state directory to a local
path such as `/var/lib/ocis/remoteposix/team`; never place it inside the remote
mount. Grant UID/GID 1000 access to these paths before starting the container.
Providers for the same root must share the same local state directory and mount
namespace view of the remote root. The image does not mount NFS/SMB itself.

The remote provider must share the gateway's `transfer_secret` (or
`OCIS_TRANSFER_SECRET`), inherited from the common oCIS configuration. Browser
TUS requests use a signed transfer URL rather than an internal user token. The
provider validates its signature, audience, expiration and exact session target
before restoring the upload owner's identity; knowing a session UUID alone does
not authorize an upload. Keep `STORAGE_USERS_DATA_SERVER_URL` consistent with the
URL advertised to the gateway.

An exported Docker archive can be imported with:

```sh
docker load -i ocis-remoteposix-8.2.0-remoteposix.3-alpine-linux-amd64.docker.tar
```

### Driver configuration

Use an additional storage-users instance for this project space if the deployment
also needs personal spaces. The normal oCIS gateway, authentication and storage
registry configuration is still required; the example below only selects the
driver. Give the additional provider its own mount ID and service addresses.

```yaml
storage_users:
  driver: remoteposix
  mount_id: 38b14792-24bf-46de-98a8-94c1ae948f2d
  drivers:
    remoteposix:
      root: /mnt/remote/team
      state_dir: /var/lib/ocis/remoteposix/team
      owner_id: REPLACE_WITH_EXISTING_OCIS_USER_ID
      owner_idp: https://cloud.example.com
      space_name: Team
      scan_interval: 1m
      missing_grace_period: 10m
```

For a standalone service configuration file, omit the outer `storage_users` key.
Equivalent environment variables:

```sh
export STORAGE_USERS_DRIVER=remoteposix
export STORAGE_USERS_MOUNT_ID=38b14792-24bf-46de-98a8-94c1ae948f2d
export STORAGE_USERS_REMOTEPOSIX_ROOT=/mnt/remote/team
export STORAGE_USERS_REMOTEPOSIX_STATE_DIR=/var/lib/ocis/remoteposix/team
export STORAGE_USERS_REMOTEPOSIX_OWNER_ID=REPLACE_WITH_EXISTING_OCIS_USER_ID
export STORAGE_USERS_REMOTEPOSIX_OWNER_IDP=https://cloud.example.com
export STORAGE_USERS_REMOTEPOSIX_SPACE_NAME=Team
export STORAGE_USERS_REMOTEPOSIX_SCAN_INTERVAL=1m
export STORAGE_USERS_REMOTEPOSIX_MISSING_GRACE_PERIOD=10m
```

The configured owner ID and identity provider must match the user's actual CS3
identity, not their display name. Initially only that owner can access the space.
The owner can add user/group grants; grants inherit down the tree, with explicit
denies taking precedence. Only the owner can change grants or administer trash.
Filesystem credentials remain separate: API permissions do not restrict users
who can directly access the remote mount.

## Required storage behavior

- Existing writable, case-sensitive mounted root with ordinary stat, readdir,
  read/write, mkdir, rename, remove, and file/directory sync operations.
- Renames between the root and its reserved control directory must be supported;
  do not place nested mounts in the exported tree.
- Persistent **local** state directory outside the remote root. Keep the SQLite
  database, WAL, shared-memory file, lock directory, `mount.lock` and staging files together.
  Do not put this directory on NFS, SMB or another network mount.
- All instances serving this root must run on one host and use the same state
  directory. Provider instances coordinate through local POSIX advisory locks.
  Independent databases on multiple hosts are unsupported.
- Direct filesystem editors are trusted. Schedule their writes separately from
  oCIS mutations: ordinary POSIX rename cannot provide a distributed transaction
  or prevent uncoordinated external writers from racing a checked destination.

The driver reserves `.ocis-remoteposix` under the selected root. It contains a
small mount-identity marker and ordinary files/directories for temporary content,
trash and previous versions. Per-file metadata is stored only in SQLite. Do not
edit, rename or delete this reserved directory. Symlinks and special files in the
exported tree are rejected; a scan encountering one stops without committing.

## Behavior and recovery

- UUIDs remain stable across oCIS moves, including directory moves and their
  descendants, replacements, restarts, and trash/restore.
- Simple and resumable TUS uploads use local staging. Completion copies to a
  temporary remote file, verifies its length, syncs it, and journals the rename.
  Upload sessions expire after 24 hours and can be purged with storage-users
  upload maintenance commands.
- Overwriting a file retains its previous contents under the control directory.
  Trash and version metadata remain in SQLite. Restore and purge operate on whole
  trash items; partial trash browsing/restoration is not implemented.
- An operation journal bridges remote file changes and SQLite commits. Recovery
  compares SHA-256 tree/content fingerprints before finishing a pending rename.
  A mismatch or ambiguous source/destination stops recovery and preserves the
  journal for investigation. It does not guess which resource should be deleted.
- An outbox retains file-change notifications until publication succeeds. Delivery
  is at least once. Only changed directories and their ancestors receive new
  ETags. Unrelated directories retain their ETags.
- Polling imports external additions and size/mtime changes. Directory listing
  reads committed metadata; it does not trigger a remote-tree walk. Failed walks
  or failed mount-identity checks do not commit scan results. Missing paths are
  hidden while their metadata is held through the grace period. A scan losing
  more than ten entries and more than 25%
  of known entries stops for administrator investigation.
- External renames are delete-plus-create and receive new IDs. A file observed
  missing and subsequently recreated also receives a new ID. Content edits that
  preserve both size and mtime are not detected by polling. Content-hash polling
  and an external-rename identity service are not implemented.

## Concurrency and commit protocol

- File reads take shared UUID and directory-entry locks. Writes take exclusive
  locks on their file and entry, with shared locks on ancestors. Directory moves,
  deletion and grant changes exclude conflicting descendant requests. Every
  resource lock set is sorted, merged and acquired without lock upgrades; paths
  and parent IDs are revalidated after acquisition. Lock waits respect request
  cancellation and are capped at 30 seconds.
- Multiple provider processes on the same host can transfer different files
  concurrently. A TUS chunk holds only its session lock. Upload completion copies
  and syncs a temporary remote file before acquiring destination locks, then
  rechecks the parent ID, permissions and expected file ETag. Competing writers
  against the same ETag produce one winner and a precondition failure.
- Downloads open the file and obtain matching metadata under shared locks, then
  release locks before streaming. Already open downloads continue reading the
  old retained file when a replacement is published. Upload-stage readers see
  only the durable offset captured when opened.
- SQLite has a query-only reader pool and a separate writer connection using
  immediate transactions. WAL permits metadata readers during a write transaction;
  SQLite still serializes writers. Remote copying, hashing, renaming and syncing
  happen outside runtime database transactions.
- Publication commits a durable intent with a resource-lock manifest, performs
  and syncs the remote rename, then atomically commits file metadata, retained
  versions, the upload result, a completion receipt and outbox events while
  deleting the intent. The request succeeds only after that final commit.
  **This is a recovery protocol, not a transaction spanning SQLite and POSIX.**
- Requests check overlapping pending intents while holding resource locks. Before
  recovery they release those locks, acquire the session (if applicable), an
  operation claim and the sorted resource locks, then recheck the journal. Live
  publishers own the same resources, so recovery cannot interfere with them.
  Process exit releases kernel locks. Completed TUS and simple-upload retries
  return their existing session result rather than publishing another version.
- A pending or ambiguous operation blocks its affected resources and ancestor
  directory views. Independent file operations remain available, including after
  restart. Recovery attempts other operations even if one fails. Polling waits
  until pending operations are resolved before reconciling external changes.
- Scan enumeration takes no tree lock. A database mutation epoch rejects stale
  observations; only the scan's final metadata commit takes the root lock. A busy
  tree can delay external-change import until a later polling pass. Directory
  metadata/listing requests briefly exclude writes within their subtree to give
  consistent aggregate metadata. Root-wide queries and emptying the entire trash
  remain root-wide operations; individual trash restore/purge locks its item.

Lock files are permanent identifiers. Do not unlink the `locks` directory while
any provider is running. Completion receipts and lock files have no automatic
retention policy in this experimental version.

## Disconnect and automatic reconnect

Every provider runs a reconnect monitor, including data providers with polling
(`watch`) disabled. Healthy probes run once per second. Failed reconnect or
recovery attempts back off to at most 30 seconds. The operating system remains
responsible for mounting the remote filesystem; the driver does not run mount
commands or create a replacement empty root.

A detected mount failure rejects new requests and preserves local staging,
metadata and durable operation intents. Once the configured path is accessible,
the monitor validates the original space identity and existing control directories.
Missing or foreign identities are rejected without creating any remote state.

Every operation holds a shared `mount.lock` lease before taking resource locks.
Reopening takes an exclusive lease across all provider processes, so active
operations must finish first. A persisted mount generation forces every instance
to reopen its own root handle before accepting more work. Pending publication
intents are recovered automatically, with continued retries when recovery remains
incomplete. An ambiguous intent still blocks its affected resources; independent
resources remain usable. Polling resumes once pending intents are resolved.

Existing upload sessions can be retried after reconnect. Already open download
streams retain their file descriptor; they may continue or return an I/O error
and are not transparently restarted. Initial provider startup still requires an
accessible, valid mount.

Filesystem calls themselves are synchronous and cannot be bounded by the lock
or context timeout. A kernel-blocked filesystem call retains its mount/resource
leases and can delay reconnect and shutdown. The monitor deliberately does not
release those leases or launch replacement writers while that call may complete.
Reconnect tests simulate handle invalidation with replacement local directories;
real NFS/SMB/FUSE disconnection and server recovery still require deployment tests.

## Upgrade

Stop **all** providers using this root before upgrading from an earlier
implementation (including schema 2). Back up the local state and remote tree together.
The driver upgrades local metadata to schema 3 and can recover older journal
records without lock manifests. Do not run the old and new binaries together:
they use different runtime lock protocols. The older binaries reject schema 3 on
startup; reverting requires restoring the matching backup.

After a recovery error, stop the provider, preserve the local state and remote
tree, and inspect the `operations` table together with the recorded source,
destination and fingerprints. Restore a consistent pair from backup if the
intended state cannot be established. Do not delete the journal or reinitialize
the database to make the error disappear. A large-deletion scan resumes after
the missing tree is restored; bulk external deletion approval is not automated.

## Current limits

This is an opt-in implementation, not a claim of production qualification on a
particular NFS/SMB/FUSE server. Test the actual mount's rename and durability
behavior before deployment. The automated tests use local Linux directories and
fault injection, not a real remote storage server.

This driver provides a configured project space, not automatic personal-space
creation or space deletion. It does not implement WebDAV locks, reference mounts,
TUS concatenation, configurable quota limits, or asynchronous upload postprocessing/antivirus. It
does not use the decomposedfs postprocessing coordinator; do not select it for a
deployment that requires that pipeline. Grants and arbitrary metadata are
supported; the broader sharing/UI flows still need deployment acceptance tests.

Members can manage sharing when explicitly granted `AddGrant`, `UpdateGrant`,
`RemoveGrant`, and `ListGrants`. Root grants inherit to descendants. Delegated
sharing cannot grant permissions the caller lacks or change stronger existing
grants; replacing a grant requires update permission even through AddGrant.
Deny grants require `DenyGrant`, including replacing or removing an existing deny.
The configured owner retains full grant management.
For managing whole-space membership through Web/Graph, assign the standard
Manager role: the gateway rejects partial grant-management roles on space roots.
That role includes file, version, and recycle permissions as well as sharing;
the current remote driver still restricts recycle management to its configured
owner. A space Manager is not a server administrator.

Quota reporting uses the remote mount's space available to its filesystem user.
Used bytes count live files indexed in this Team space; effective total is used
plus available bytes, not the entire disk size. Files outside the space, trash,
and versions reduce available capacity without inflating live-file usage. Values
are bounded to the Graph API's signed 64-bit range. Capacity errors or a missing
mount return an error rather than reporting unlimited space or local disk space.

Migration of an existing `posix`/`ocis` metadata layout is not automatic: importing
an existing plain tree creates new IDs. No xattr metadata or old share IDs are
silently adopted. There is no automatic version retention policy; monitor remote
capacity. A rescan cannot recreate lost grants, IDs, or version/trash associations.

## Backup

Stop all provider instances and all external writers, then back up the entire
local state directory and the entire remote root (including the reserved control
directory) as one consistent set. Restore both to the same configured locations.
Do not copy only a live `metadata.sqlite`: committed transactions may still be in
its WAL. SQLite is authoritative metadata and must be included in backups.
