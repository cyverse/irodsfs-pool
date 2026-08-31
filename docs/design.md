# irodsfs-pool Design

## System Topology

```
[FUSE Client (DE Pod)] <-- gRPC --> [irodsfs-pool Server] <-- native protocol --> [iRODS]
```

- **FUSE Client**: Lightweight gateway that separates I/O strategies by file open mode to offset network latency.
- **irodsfs-pool Server**: Shared middleware with memory block cache, local staging filesystem, and session multiplexing.

## Architecture Overview

The pool client (`client/client.go`) implements `IRODSFSClient` interface and communicates with the pool server via gRPC. The server wraps `IRODSFSClientBuffered` from `irodsfs-common`, which provides 4MB block caching for reads and local staging for writes.

## Client-Side I/O Strategies

### ReadOnly Mode — Prefetch (Double Buffering)

- Two 4MB buffers per file handle (current + next).
- On `ReadAt`, data is served directly from the local prefetch buffer (zero network RTT).
- When 50% of the current buffer is consumed, the next 4MB block is fetched in the background.
- When the current buffer is exhausted, the next buffer is swapped in.
- On random seek (cache miss), a synchronous block-aligned fetch repopulates the buffer.
- Prefetch is disabled when more than `maxPrefetchHandles` (default 10) ReadOnly handles are open simultaneously to limit memory usage.

### Seek Detection and Prefetch Disable

- On a cache miss (requested offset not in current or next buffer), a seek miss counter is incremented.
- On a cache hit (served from current or next buffer), the counter resets to 0.
- When the counter reaches `prefetchDisableThreshold` (default 3), prefetch is permanently disabled for that handle:
  - Buffers are freed (nil'd out) to reclaim memory.
  - A background `CacheFile` RPC is issued to make the pool server pre-warm its block cache for the entire file.
  - All subsequent reads fall through to direct `ReadAt` RPCs served from the server's warm cache.

### WriteOnly Mode — Micro Buffering

- Small writes are accumulated in a 1MB buffer before being sent to the server.
- Contiguous sequential writes are appended to the buffer; non-contiguous writes flush the buffer first.
- Buffer is flushed on `Flush`, `Close`, `Truncate`, or when it reaches 1MB.
- Micro buffering is enabled for every WriteOnly handle, with no per-session handle limit.

### ReadWrite Mode — Pass-Through

- No client-side buffering or prefetching.
- All `ReadAt`/`WriteAt` calls are forwarded directly to the server via gRPC unary RPCs.

## Server-Side Design

### Session Management

- Sessions are keyed by account credentials (host, port, user, zone, ticket, resource).
- Multiple client connections sharing the same account reuse a single session.
- Sessions are released when all connections disconnect or after a configurable timeout.
- Every active session has lifecycle metadata in the Badger database at `<data_root_path>/failed_sessions.db`.
- Records contain the session ID, lifecycle status, account key, redacted account, client connections, and last-access time. Passwords, tickets, and PAM tokens are never persisted.
- Records left in `active` or `recovering` state by a crash are changed to `interrupted` on server startup. A matching client login reopens the persistent staging directory and changes the record to `recovering`; the staging worker then retries queued operations.
- A clean release failure changes the status to `release_failed`. A successful release removes the record, and the database directory is deleted when no records remain.

### Block Cache (ReadOnly)

- 4MB blocks cached in shared memory (Ristretto-based) with configurable TTL and LRU eviction.
- Default capacity: 100GB. Default TTL: 12 hours.
- Shared across all sessions — multiple FUSE clients reading the same file hit the same cache.
- Cache is populated only via `ReadAt` (FUSE random-access) and `CacheFile` (pre-warm) paths.
- Streaming downloads (`DownloadFile`, `DownloadFileWithCallback`) do NOT populate cache — they are one-shot operations where caching would waste memory without benefit.

### Staging Filesystem (WriteOnly)

- Write data is stored on local disk immediately; success is returned to the client without waiting for iRODS sync.
- Background worker syncs dirty files to iRODS with configurable grace period.
- Provides crash recovery: un-synced files are re-uploaded on server restart.
- All download paths check staging first: if a file is staged (pending upload), it is served from the local staging copy rather than fetching from iRODS.

### Working Copy (ReadWrite)

- File is downloaded to local disk on open.
- All subsequent reads/writes are served from the local copy.
- On close, the modified file is synced back to iRODS.

### Resource Availability Checks

- On startup, the server checks system memory and staging disk availability.
- If available memory is less than the configured cache size, a warning is logged.
- If staging disk free space is below 1GB, a warning is logged.
- The monitoring web page displays real-time red warnings when resources are insufficient.

### Metrics Collection

- `AccumulatedMetrics` stores only terminated sessions' final metric values.
- Live total = accumulated + active sessions' current `GetMetrics()` values (read-only, no clear).
- Prometheus counters use delta-based reporting: each collection cycle computes `current_total - last_reported` and calls `Add(delta)`.
- This avoids the inflation bug where `GetAndClear*` on a copy of `IRODSMetrics` fails to clear the original session metrics.

### Logging

- `GetLogWriter(foreground=true)` returns a `MultiWriteCloser` that writes to both stderr and a rotating log file (`.fg`).
- `GetLogWriter(foreground=false)` returns a rotating log file writer (`.daemon`) only.
- `log.SetOutput` is called by the main function, not internally by `GetLogWriter`.

## gRPC API

### Streaming RPCs

| RPC | Direction | Purpose |
|-----|-----------|---------|
| `ReadStream` | Server → Client | Download full file with block-level callbacks (path-based) |
| `ReadStreamParallel` | Server → Client | Parallel download with multiple iRODS connections (path-based) |
| `WriteStream` | Client → Server | Bulk upload via streaming chunks (handle-based) |

### Key Unary RPCs

- `ReadAt` / `WriteAt` — handle-based random I/O (used for RDWR pass-through and prefetch block fetch)
- `GetAvailable` — returns bytes readable from offset without blocking (based on server cache state)
- `CacheFile` — pre-warm server cache for a file

## Memory Management

Read prefetch memory is limited when many files are open:

- `maxPrefetchHandles` (default 10): Only the first N ReadOnly handles get prefetch buffers (8MB each).
- ReadOnly handles exceeding the threshold operate in direct pass-through mode; a background `CacheFile` RPC is issued so the server pre-warms its block cache for that file.
- The ReadOnly handle count is tracked per session and decremented on `Close`.
- Every WriteOnly handle gets a 1MB micro buffer, without a per-session handle limit.

## HTTP Monitoring and API Service

A single HTTP server (default port 12021) exposes monitoring, metrics, and a read-only REST API:

| Path | Purpose |
|------|---------|
| `/monitor` | HTML dashboard with sessions, cache, staging, metrics, and resource warnings |
| `/metrics` | Prometheus exporter (scrape target) |
| `/api/sysinfo` | JSON server, memory cache, staging, and I/O metrics information |
| `/api/sessions` | JSON list of active sessions with clients, staged files, and open file handles |
| `/api/sessions/{sessionID}` | JSON details for one active session, including clients, staged files, and open file handles |
| `/api/recovery-sessions` | JSON list of interrupted, recovering, or release-failed sessions |
| `/api/recovery-sessions/{sessionID}` | JSON persisted recovery metadata for one session |

The dashboard auto-refreshes every 10 seconds, shows sessions pending recovery in a separate section, and shows red warnings when system memory or staging disk space is insufficient.
The REST API does not expose account credentials, tickets, or PAM tokens.
The earlier `/api/failed-sessions` paths remain compatibility aliases for the recovery-session endpoints.

## Configuration

Key server-side parameters (see `commons/config.go`):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `data_block_size` | 4MB | Block size for caching and streaming |
| `max_data_mem_cache_size` | 100GB | Total memory cache capacity |
| `data_mem_cache_ttl` | 12h | Cache entry time-to-live |
| `max_io_connection_per_session` | 30 | Max iRODS connections per session |
| `session_timeout` | 10min | Idle session timeout |
| `staging_data_grace_period` | 10s | Delay before syncing staged writes to iRODS |
| `monitoring_service_port` | 12021 | HTTP port for /monitor, /metrics, and /api |
