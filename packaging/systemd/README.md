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
sudo ./packaging/systemd/install.sh
```

The script works from a source checkout. A release archive contains the same
assets at its root, so install it after extraction with `sudo ./install.sh`.
It installs:
- `/usr/bin/irodsfs-pool` — service binary
- `/etc/irodsfs-pool/config.yaml` — configuration file
- `/etc/systemd/system/irodsfs-pool.service` — systemd unit

It creates the `irodsfs-pool` system user/group and the packaged data and
staging directories. An existing configuration file is preserved, so a
reinstall never replaces its recovery encryption key or other local changes.
It also enables and starts `irodsfs-pool.service` immediately.

When `recovery_encryption_key` is empty, the installer generates a new
base64-encoded 32-byte key and stores it in the configuration file before
starting the service. Back up this configuration file: the key must remain
stable to decrypt persisted recovery credentials after a host replacement.

## Configuration

Before starting the service, edit `/etc/irodsfs-pool/config.yaml`. The values
below are the values shipped in the package, not the application's built-in
defaults.

| Parameter | Packaged value | Description |
|-----------|----------------|-------------|
| `service_endpoint` | `tcp://0.0.0.0:12020` | gRPC listen endpoint (TCP or Unix socket) |
| `debug` | `false` | Enable debug logging |
| `data_root_path` | `/irodsfs_pool` | Root directory for runtime data |
| `pid_file` | `/run/irodsfs-pool/irodsfs-pool.pid` | PID file used by `start`, `stop`, and `status` |
| `recovery_encryption_key` | empty | Required Base64-encoded 32-byte key used to encrypt recovery credentials |
| `session_timeout` | `10m` | Idle session timeout |
| `session_timeout_check_interval` | `10s` | Interval for checking idle sessions |
| `data_block_size` | `4194304` (4 MiB) | Block size for caching and streaming |
| `max_data_mem_cache_size` | `107374182400` (100 GiB) | Total in-memory data cache capacity |
| `max_data_mem_cache_buffer_items` | `512` | Maximum number of buffered memory-cache items |
| `data_mem_cache_ttl` | `6h` | In-memory cache entry lifetime |
| `max_io_connection_per_session` | `10` | Maximum number of iRODS I/O connections per session |
| `start_new_transaction` | `false` | Start each new session with a new transaction |
| `max_metadata_cache_entries_per_session` | `1000000` | Maximum metadata-cache entries per session |
| `max_metadata_cache_size_per_session` | `10485760` (10 MiB) | Maximum metadata-cache size per session |
| `max_metadata_cache_buffer_items_per_session` | `256` | Maximum buffered metadata-cache items per session |
| `metadata_cache_ttl` | `1m` | Metadata-cache entry lifetime |
| `staging_root_path` | `/irodsfs_pool/staging` | Local staging directory for writes |
| `staging_data_grace_period` | `10s` | Delay before syncing staged writes |
| `max_staging_data_size` | `536870912000` (500 GiB) | Maximum total size of staged data |
| `operation_timeout` | `5m` | Operation timeout |
| `management_service_port` | `12021` | HTTP port for `/monitor`, `/metrics`, and `/api` (set to `0` to disable) |
| `log_root_path` | `/var/log/irodsfs-pool` | Directory containing service and per-session logs |

The service will not start while `recovery_encryption_key` is empty. Generate a
key and copy the output into the configuration file:

```bash
openssl rand -base64 32
```

Keep this key stable across restarts. Replacing or losing it prevents the
service from decrypting credentials stored for session recovery. Protect the
configuration file because it contains this secret.

The packaged configuration listens for gRPC on all network interfaces at port
`12020`. The management service also listens on all interfaces at port `12021`
and exposes operational API actions without built-in HTTP authentication.
Restrict both ports to trusted hosts with a firewall or an authenticated reverse
proxy. If remote gRPC access is unnecessary, use a Unix socket endpoint instead,
for example:

```yaml
service_endpoint: unix:///irodsfs_pool/comm.sock
```

## Service Management

The install script already enables and starts the service. To restart it after
changing configuration:

```bash
sudo systemctl restart irodsfs-pool.service
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
sudo systemctl disable --now irodsfs-pool.service
sudo rm -f /etc/systemd/system/irodsfs-pool.service /usr/bin/irodsfs-pool
sudo systemctl daemon-reload
```
