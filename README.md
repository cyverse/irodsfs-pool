# iRODS FUSE Lite Pool

A shared middleware server that pools iRODS connections, caches data blocks in memory, and stages writes to local disk for multiple FUSE client instances.

## Features

- **Session multiplexing** — clients sharing the same iRODS account reuse a single session and connection pool.
- **Block cache** — 4MB blocks cached in memory (Ristretto) with configurable capacity and TTL, shared across all sessions.
- **Local staging** — writes are stored on local disk immediately and synced to iRODS in the background.
- **Monitoring** — built-in HTTP dashboard (`/monitor`) and Prometheus metrics (`/metrics`) on a single port.
- **Resource checks** — warns on startup and in the dashboard when memory or disk is insufficient.

## Build

```bash
go build -o irodsfs-pool ./cmd
```

## Configuration

Create a YAML config file (see `install/config.yaml` for a full example):

```yaml
service_endpoint: tcp://0.0.0.0:12020
data_root_path: /irodsfs_pool
max_data_mem_cache_size: 107374182400  # 100GB
data_mem_cache_ttl: 6h
staging_root_path: /irodsfs_pool/staging
monitoring_service_port: 12021
foreground: false
```

## Usage

Run in foreground (logs to stderr + file):

```bash
./irodsfs-pool -c config.yaml -f
```

Run as daemon:

```bash
./irodsfs-pool -c config.yaml
```

## Endpoints

| Port | Path | Description |
|------|------|-------------|
| 12020 | — | gRPC service (FUSE clients connect here) |
| 12021 | `/monitor` | HTML monitoring dashboard |
| 12021 | `/metrics` | Prometheus scrape target |
