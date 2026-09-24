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
  database, WAL, shared-memory file, operation lock and staging files together.
  Do not put this directory on NFS, SMB or another network mount.
- All instances serving this root must run on one host and use the same state
  directory. Provider instances serialize operations through a local file lock.
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
  is at least once. Directory ETags currently invalidate conservatively on any
  committed tree change. This implementation prioritizes correctness over large
  tree throughput; scans and file operations serialize per root.
- Polling imports external additions and size/mtime changes. Directory listing
  also triggers reconciliation. Failed walks or failed mount-identity checks do
  not commit scan results. Missing paths are hidden while their metadata is held
  through the grace period. A scan losing more than ten entries and more than 25%
  of known entries stops for administrator investigation.
- External renames are delete-plus-create and receive new IDs. A file observed
  missing and subsequently recreated also receives a new ID. Content edits that
  preserve both size and mtime are not detected by polling. Content-hash polling
  and an external-rename identity service are not implemented.

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
TUS concatenation, quotas, or asynchronous upload postprocessing/antivirus. It
does not use the decomposedfs postprocessing coordinator; do not select it for a
deployment that requires that pipeline. Grants and arbitrary metadata are
supported; the broader sharing/UI flows still need deployment acceptance tests.

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
