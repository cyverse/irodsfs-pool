# iRODS FUSE Pool Architecture

## Components and ownership

```
 +---------------------------- Client host ----------------------------------+
 |  irodsfs (FUSE)                                                           |
 |    +-- irodsfs-pool/client  (gRPC, auto-reconnect, one conn per mount)    |
 |          RDONLY  4MB x 2 prefetch buffer                                  |
 |          WRONLY  1MB micro buffer                                         |
 |          RDWR    pass-through, 1:1 to the server                          |
 +---------------------------------------------------------------------------+
                              |
                              |  gRPC  (Login/Logout/KeepAlive, List/Stat,
                              |         Open/Create/ReadAt/WriteAt/Close,
                              |         ReadStream/WriteStream, Sync)
                              v
 +--------------------------- irodsfs-pool server --------------------------+
 |                                                                          |
 |  PoolServer  ──  gRPC :12020  |  monitoring + REST :12021                |
 |      |                                                                   |
 |      v                                                                   |
 |  PoolSessionManager                                                      |
 |      sessions[accountKey]           live, reusable by a new Login        |
 |      releasingSessions[accountKey]  tearing down; reported, not reusable |
 |      connMap[connID] -> sessionID                                        |
 |      pendingReleases[sessionID]     grace timer (30s)                    |
 |                                                                          |
 |      Many clients on the SAME iRODS account share ONE PoolSession.       |
 |      sessionID is the account key hash, so the map is keyed by account.  |
 |      |                                                                   |
 |      v                                                                   |
 |  PoolSession                                                             |
 |      connections[connID]        which clients are attached               |
 |      poolFileHandles[handleID]  open files across all of them            |
 |      fs        go-irodsclient FileSystem  (metadata cache)               |
 |      fsClient  IRODSFSClientBuffered  ....... irodsfs-common             |
 |                                                                          |
 +--------------------------------------------------------------------------+
                              |
                              v
 +----------------- irodsfs-common: IRODSFSClientBuffered ------------------+
 |                                                                          |
 |  Routes every path to exactly one of three backends.                     |
 |                                                                          |
 |   path matches a packed directory name?                                  |
 |        yes -> packedfs          (.venv, .git, .claude, ...)              |
 |        no, and it is a write or a staged file -> stagingfs               |
 |        no, and it is a plain read -> MemoryCacheManager -> iRODS         |
 |                                                                          |
 |  +-- MemoryCacheManager ------------------------------------------+      |
 |  |   Ristretto, shared by ALL sessions, 100GB, 4MB blocks, TTL 12h |      |
 |  |   key irods:block:{path}:{N}   (block -1 = size/mtime stamp)    |      |
 |  +-----------------------------------------------------------------+     |
 |                                                                          |
 |  +-- stagingfs -------------------+  +-- packedfs -------------------+   |
 |  | per-file staging               |  | per-directory archives        |   |
 |  |                                |  |                               |   |
 |  | {root}/{sid}/data/...  copies  |  | {root}/{sid}-packed/...  tree |   |
 |  | {root}/{sid}/meta      Badger  |  |   (a SIBLING of the staging   |   |
 |  |   staging:{path}   dirty state |  |    root: staging deletes its  |   |
 |  |   operation:{id}   ordering DAG|  |    own root on close)         |   |
 |  |                                |  |                               |   |
 |  | worker every 5s, grace 10s:    |  | one archive per directory:    |   |
 |  |   upload dirty & idle files    |  |   .venv -> .venv.packedfs.tar    |   |
 |  |   <=1GB kept as read cache     |  | no per-file metadata at all   |   |
 |  |                                |  | never evicted, never force-   |   |
 |  | crash recovery: Badger restore |  |   synced file by file         |   |
 |  |   RUNNING -> QUEUED, resumes   |  | crash loses at most one       |   |
 |  |                                |  |   snapshot interval (30m)     |   |
 |  +--------------------------------+  +-------------------------------+   |
 |            \                                    /                        |
 |             \___ ONE staging quota (500GB) ____/                         |
 |                  currentSize  + externalSize                             |
 |                  (staged files)  (packed trees, not evictable)           |
 |                  full -> writes FAIL, packed trees are never             |
 |                          silently uploaded the slow way                  |
 +--------------------------------------------------------------------------+
                              |
                              |  go-irodsclient (native iRODS protocol)
                              v
                        [  iRODS server  ]
                          collections + data objects
                          .venv.packedfs.tar  <- a packed directory lives here
```

## What reaches iRODS, and when

```
  stagingfs                              packedfs
  ---------                              --------
  file closed                            first access to the directory
     |                                      |  DOWNLOAD archive, extract locally
     v  after 10s idle                      v
  UPLOAD one data object                 every operation is local from now on
     |                                      |  (list, stat, read, write, rename,
     |  <=1GB -> kept as read cache         |   delete: zero iRODS round trips)
     |  >1GB  -> local copy deleted         |
                                            v  every 30m while dirty
                                         PACK + UPLOAD one archive, stay mounted
                                            |
                                            v  session release
                                         PACK + UPLOAD, then drop the local tree
                                            (skipped when nothing changed)
```

## Session lifecycle

```
  Login            client attaches; an existing session for the same account
                     is reused, so the second mount pays no setup cost
    |
    v
  in use           connections > 0
    |
    |  last client disconnects
    v
  grace period     30s, still in sessions[] and reusable
    |                a client that reconnects here keeps everything warm
    v
  releasing        moved to releasingSessions[]: no longer reusable, but still
    |                listed on the monitoring page, because the upload below
    |                can take minutes and the resources are still held
    |
    |   1. flushSessionStaging -> fsClient.Sync()
    |        packedfs: pack + upload every dirty mount
    |        stagingfs: SyncAll()
    |   2. fsClient.Release()
    |        packedfs.Close()   pack + upload, drop trees, free quota
    |        stagingfs.Close()  final SyncAll, close Badger, delete its root
    |        go-irodsclient release
    v
  gone             dropped from releasingSessions[]

  A release that fails keeps its data: staging preserves its root, packedfs
  preserves any tree whose archive did not reach iRODS, and the session is
  recorded in the recovery store rather than discarded.
```

## Open-mode paths

```
  RDONLY  FUSE read
            -> client 4MB prefetch buffer (miss -> 1MB-chunked ReadAt RPCs)
            -> packed dir?  local extracted file, no iRODS at all
            -> staged file? local staging copy
            -> otherwise    memory cache hit, else fetch a 4MB block and cache

  WRONLY  FUSE write
            -> client 1MB micro buffer -> WriteAt RPC
            -> packed dir?  local file in the extracted tree; uploads with the
                            whole directory as one archive, not per file
            -> otherwise    local staging file + Badger dirty record;
                            background worker uploads after the grace period

  RDWR    FUSE open
            -> packed dir?  local file, opened read-write directly
            -> otherwise    server downloads a working copy from iRODS,
                            memory cache bypassed and stale blocks invalidated;
                            Close/idle -> staged -> uploaded
```

## Why packed directories exist

A directory such as `.venv` or `.git` holds tens of thousands of small files.
Uploading them through stagingfs costs one round trip per file, which dominates
the transfer. packedfs holds the whole directory on local disk for as long as a
session uses it and moves it as a single archive, turning those round trips into
one. The trade is that iRODS stores a data object rather than a browsable
collection, so only directories that are not read directly in iRODS are packed.
