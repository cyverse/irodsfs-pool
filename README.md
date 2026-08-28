# iRODS FUSE Pool

A shared middleware server that pools iRODS connections, caches data blocks in memory, and stages writes to local disk for multiple FUSE client instances.

## Features

- **Session multiplexing** — clients sharing the same iRODS account reuse a single session and connection pool.
- **Block cache** — 4MB blocks cached in memory (Ristretto) with configurable capacity and TTL, shared across all sessions.
- **Local staging** — writes are stored on local disk immediately and synced to iRODS in the background.
- **Session recovery** — active-session lifecycle metadata is persisted without credentials so crashes and failed releases can be detected and recovered later.
- **Monitoring** — built-in HTTP dashboard (`/monitor`), Prometheus metrics (`/metrics`), and a read-only REST API (`/api`) on a single port.
- **Resource checks** — warns on startup and in the dashboard when memory or disk is insufficient.

## Build

```bash
go build -o irodsfs-pool ./cmd
```

## Configuration

Create a YAML config file (see `packaging/systemd/config.yaml` for a full example):

```yaml
service_endpoint: tcp://0.0.0.0:12020
data_root_path: /irodsfs_pool
max_data_mem_cache_size: 107374182400  # 100GB
data_mem_cache_ttl: 6h
staging_root_path: /irodsfs_pool/staging
monitoring_service_port: 12021
log_root_path: /var/log/irodsfs-pool
```

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
| 12021 | `/api/recovery-sessions` | List interrupted, recovering, or release-failed sessions (JSON) |
| 12021 | `/api/recovery-sessions/{sessionID}` | Get persisted recovery metadata for one session (JSON) |

`/api/failed-sessions` and `/api/failed-sessions/{sessionID}` remain compatibility aliases.
