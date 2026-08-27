# irodsfs-pool systemd Installation

## Prerequisites

- Go 1.21+
- systemd-based Linux system

## Build

```bash
make build
```

The binary is output to `bin/irodsfs-pool`.

## Install

```bash
sudo make install
```

This installs:
- `/usr/bin/irodsfs-pool` — service binary
- `/etc/irodsfs-pool/config.yaml` — configuration file
- `/usr/lib/systemd/system/irodsfs-pool.service` — systemd unit

## Configuration

Edit `/etc/irodsfs-pool/config.yaml` to adjust settings:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `service_endpoint` | `unix:///var/lib/irodsfs_pool/comm.sock` | gRPC listen endpoint (unix or tcp) |
| `data_root_path` | `/var/lib/irodsfs_pool` | Root directory for runtime data |
| `session_timeout` | `10m` | Idle session timeout |
| `data_block_size` | `4194304` (4MB) | Block size for caching and streaming |
| `max_data_mem_cache_size` | `107374182400` (100GB) | Total memory cache capacity |
| `data_mem_cache_ttl` | `12h` | Cache entry time-to-live |
| `max_io_connection_per_session` | `30` | Max iRODS connections per session |
| `staging_root_path` | `/var/lib/irodsfs_pool/staging` | Local staging directory for writes |
| `staging_data_grace_period` | `10s` | Delay before syncing staged writes |
| `operation_timeout` | `5m` | gRPC operation timeout |
| `prometheus_exporter_port` | `12022` | Prometheus metrics port (0 to disable) |
| `pid_file` | `/run/irodsfs-pool/irodsfs-pool.pid` | PID file used by the daemon and systemd |
| `debug` | `false` | Enable debug logging |
| `log_root_path` | `/var/log/irodsfs-pool` | Directory containing the service log and per-session logs |

## Service Management

Enable and start:
```bash
sudo systemctl enable irodsfs-pool.service
sudo systemctl start irodsfs-pool.service
```

Check status:
```bash
sudo systemctl status irodsfs-pool.service
```

View logs:
```bash
journalctl -u irodsfs-pool.service -f
tail -F /var/log/irodsfs-pool/irodsfs-pool.log
```

Each pool session writes its iRODS client logs only to
`/var/log/irodsfs-pool/session_logs/<session-id>.log`.

The unit uses `Type=forking`. It starts the service with
`irodsfs-pool start`, tracks the daemon through `PIDFile`, and stops it with
`irodsfs-pool stop`. Use `irodsfs-pool run` for foreground development or with
a separate `Type=simple` unit.

## Uninstall

```bash
sudo systemctl stop irodsfs-pool.service
sudo systemctl disable irodsfs-pool.service
sudo make uninstall
```
