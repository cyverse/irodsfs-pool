# iRODS FUSE Pool

A shared middleware server that pools iRODS connections, caches data blocks in memory, and stages writes to local disk for multiple FUSE client instances.

## Features

- **Session multiplexing** — clients sharing the same iRODS account reuse a single session and connection pool.
- **Block cache** — 4MB blocks cached in memory (Ristretto) with configurable capacity and TTL, shared across all sessions.
- **Local staging** — writes are stored on local disk immediately and synced to iRODS in the background.
- **Packed directories** — directories full of small files (`.venv`, `.git`, …) are held locally and stored in iRODS as a single archive, turning tens of thousands of round trips into one transfer.
- **Session recovery** — active-session lifecycle metadata is persisted without credentials so crashes and failed releases can be detected and recovered later.
- **Monitoring** — built-in HTTP dashboard (`/monitor`), Prometheus metrics (`/metrics`), and an administrative REST API (`/api`) on a single port.
- **Resource checks** — warns on startup and in the dashboard when memory or disk is insufficient.

## Build

```bash
go build -o irodsfs-pool ./cmd
```

## Install the latest Linux release

Install the latest release for the current Linux architecture and register it
as a systemd service with:

```sh
curl -fsSL https://raw.githubusercontent.com/cyverse/irodsfs-pool/main/install.sh | bash
```

The installer downloads the matching GitHub Release archive, installs the
service, and enables and starts it. If `recovery_encryption_key` is empty, the
installer generates a base64-encoded 32-byte key in
`/etc/irodsfs-pool/config.yaml`; back up that file because the key is needed
to decrypt persisted recovery credentials.

## Configuration

Create a YAML config file (see `packaging/systemd/config.yaml` for a full example):

```yaml
service_endpoint: tcp://0.0.0.0:12020
data_root_path: /irodsfs_pool
max_data_mem_cache_size: 107374182400  # 100GB
data_mem_cache_ttl: 6h
staging_root_path: /irodsfs_pool/staging
management_service_endpoint: 0.0.0.0:12021  # `http://` is optional
log_root_path: /var/log/irodsfs-pool
```

### Packed directories

Uploading a directory of many small files costs one round trip per file, which
dominates the transfer for trees like `.venv` or `.git`. Packing stores such a
directory in iRODS as a single archive data object — `.venv` becomes
`.venv.mount.tar` — so it crosses the wire once instead of once per file.

It is on by default for the tool directories listed below. Removing a name is
how a deployment keeps that directory a normal iRODS collection.

```yaml
packed_directories:
  enabled: true
  names: [".git", ".venv", ".claude", ".codex", ".copilot", ".ansible", ".cache", ".docker", ".vscode", ".vscode-shared"]
  suffix: ".mount.tar"
  compression: none              # none | gzip | zstd
  max_packed_dir_size: 5368709120  # 5GB
  snapshot_interval: 30m
  concurrent_pack_limit: 2
```

How it behaves:

- **First access** downloads the archive and extracts it under
  `{staging}/{sessionID}-packed`. Everything after that — listing, stat, read,
  write, rename, delete — is served from local disk with no iRODS round trip.
  That directory is deliberately a sibling of `{staging}/{sessionID}`, which
  staging deletes once its own uploads have synced.
- **Every `snapshot_interval`** a directory with unsaved changes is packed and
  uploaded, which bounds how much work a crash can lose. A negative value
  uploads at session release only.
- **Session release** packs each directory one last time and removes the local
  tree, skipping the upload when nothing changed since the last pack. The
  archive is uploaded under a temporary name and renamed into place, so a
  failed transfer never destroys the previous archive. A tree whose archive did
  not reach iRODS is kept on disk rather than deleted with the session.
- **Existing collections** are migrated on first access: the collection is
  downloaded, and the first successful pack replaces it with the archive.

Things to know about it:

- A packed directory is **not browsable in iRODS** on its own — `ils` shows the
  archive. Pack only directories that are not read directly there.
- The extracted tree occupies staging disk for the whole session and is never
  evicted, so a full staging area makes writes fail rather than silently
  falling back to slow per-file uploads. It shares `max_staging_data_size` with
  the staged files, so size that for both.
- A single packed directory may not exceed `max_packed_dir_size`. A write that
  would cross the line fails, which keeps the directory readable; letting it
  grow past the limit would make the next session refuse to mount it.
- `compression: none` is the default on purpose: these directories hold
  already-compressed data, so a codec costs CPU without saving much, and an
  uncompressed archive stays seekable.
- Renaming or moving a path across the boundary of a packed directory reports
  `EXDEV`, so callers fall back to copy-and-unlink, as they would across any two
  filesystems.
- Mounted directories and their pending bytes are reported in
  `/api/sessions/{sessionID}` under `packed_directories`.

## Usage

Run in foreground (logs to stderr + file):

```bash
./irodsfs-pool run -c config.yaml
```

Foreground `run` uses the current working directory as `data_root_path`. A
staging path derived from the configured data root is moved to `./staging`;
an explicitly configured staging path is preserved. No PID file is created.

Run as daemon:

```bash
./irodsfs-pool start -c config.yaml

# Check or stop the daemon
./irodsfs-pool status -c config.yaml
./irodsfs-pool stop -c config.yaml
```

## Endpoints

| Port | Path | Description |
|------|------|-------------|
| 12020 | — | gRPC service (FUSE clients connect here) |
| 12021 | `/monitor` | HTML monitoring dashboard |
| 12021 | `/metrics` | Prometheus scrape target |
| 12021 | `/api/sysinfo` | Server, memory cache, staging, and I/O metrics (JSON) |
| 12021 | `/api/sessions` | List active sessions with full monitoring details (JSON) |
| 12021 | `/api/sessions/{sessionID}` | Get active session details (JSON) |
| 12021 | `POST /api/sessions/{sessionID}/metadata-cache/invalidate` | Invalidate an active session's filesystem metadata cache |
| 12021 | `POST /api/sessions/{sessionID}/staging/sync` | Sync an active session's staged data to iRODS |
| 12021 | `/api/recovery-sessions` | List interrupted, recovering, or release-failed sessions (JSON) |
| 12021 | `/api/recovery-sessions/{sessionID}` | Get persisted recovery metadata for one session (JSON) |

`/api/failed-sessions` and `/api/failed-sessions/{sessionID}` remain compatibility aliases.
