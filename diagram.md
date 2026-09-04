# iRODS FUSE Architecture

```
 +--------------------------- Client ---------------------------------+
 | irodsfs (iRODS FUSE Lite)                                          |
 |   client: irodsfs-pool/client (gRPC client, auto-reconnect)        |
 |                                                                    |
 |  RDONLY: prefetchState — 4MB x 2 double buffer                     |
 |     read hit  -> served from local buffer (zero RTT)               |
 |     50% consumed -> background goroutine prefetches next 4MB      |
 |     (beyond 10 read handles/session -> server-side CacheFileAsync) |
 |     (after 16MB read -> local prefetch disabled, server cache)     |
 |                                                                    |
 |  WRONLY: micro buffer 1MB (enabled for every write-only handle)    |
 |     flush on 1MB full / non-sequential offset / Flush / Close      |
 |  RDWR:  pass-through (no buffering)                                 |
 |     every ReadAt/WriteAt/Flush forwarded 1:1 to the server         |
 +====================================================================+
             |  gRPC
             |  - unary ReadAt/WriteAt, chunked at 1MB (fileRWLengthMax)
             |  - streaming ReadStream/WriteStream for bulk transfers
             v
 +------------------- fs-proxy (irodsfs-pool service) ---------------+
 |  PoolSessionManager (per iRODS user session, reuse + grace release) |
 |  Per session: go-irodsclient + IRODSFSClientBuffered (common lib)  |
 |                                                                    |
 |  [MemoryCacheManager]  Ristretto, 100GB, 4MB blocks, TTL 12h       |
 |                           key: irods:block:{path}:{N}              |
 |                           (block -1 = size/mtime freshness stamp)  |
 |  [StagingFS]                                                     |
 |    {root}/{sessionID}/data/...   <- local files (working copies)   |
 |    {root}/{sessionID}/meta       <- BadgerDB                       |
 |      - staging:{path}   (dirty metadata, recorded at open/create)  |
 |      - operation:{id}   (ordering DAG: RENAME->UPLOAD, RMDIR, ...) |
 |    background worker (interval 5s, grace period 10s)              |
 |      - uploads to iRODS when dirty & idle (DAG order respected)    |
 |      - on success: <=1GB kept locally as read cache, larger deleted|
 |      - bulk uploads: local copy deleted immediately after sync    |
 |  [InodeManager]  stable inode IDs for staging entries (runtime)    |
 |  Crash recovery: Badger Restore (RUNNING -> QUEUED) -> worker      |
 |  resumes pending uploads automatically on restart                  |
 +====================================================================+
             |  go-irodsclient (native iRODS protocol, direct)
             v
        [ iRODS server ]

 Open-mode paths through the stack:

  RDONLY  FUSE read
            -> client 4MB prefetch buffer (miss -> 1MB-chunked ReadAt RPCs)
            -> server: IRODSFSClientBuffered.ReadAt
                 hit  -> 100GB memory cache (block-level)
                 miss -> fetch 4MB block from iRODS, cache it, return
            (staged/pending file -> read from local staging copy)

  WRONLY  FUSE write (all concurrently open write-only handles)
            -> client 1MB micro buffer -> WriteAt RPC (1MB chunks)
            -> server: staged handle -> local file under {root}/{session}/data
                 + Badger "dirty" record (at open/create)
            -> background worker uploads to iRODS after grace period
                 (quota exceeded -> fall back to direct iRODS write)

  RDWR    FUSE open
            -> server downloads working copy from iRODS (parallel)
                 memory cache bypassed + stale blocks invalidated
            -> all random reads/writes served from the local working copy
            -> Close / idle -> staged for sync -> final version uploaded
```
