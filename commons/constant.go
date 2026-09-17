package commons

import "time"

const (
	DataRootPathDefault                          string        = "/var/lib/irodsfs_pool"
	PIDFilePathDefault                           string        = "/run/irodsfs-pool/irodsfs-pool.pid"
	SessionTimeoutDefault                        time.Duration = 10 * time.Minute
	SessionTimeoutCheckIntervalDefault           time.Duration = 10 * time.Second
	DataBlockSizeDefault                         int64         = 4 * 1024 * 1024          // 4mb
	MaxDataMemCacheSizeDefault                   int64         = 100 * 1024 * 1024 * 1024 // 100gb
	MaxDataMemCacheBufferItemsDefault            int64         = 512
	DataMemCacheTTLDefault                       time.Duration = 12 * time.Hour
	MaxIOConnectionPerSessionDefault             int           = 30
	StartNewTransactionDefault                   bool          = false
	MaxMetadataCacheEntriesPerSessionDefault     int64         = 1000000
	MaxMetadataCacheSizePerSessionDefault        int64         = 10 * 1024 * 1024 // 10mb
	MaxMetadataCacheBufferItemsPerSessionDefault int64         = 256
	MetadataCacheTTLDefault                      time.Duration = 1 * time.Minute
	StagingRootPathDefault                       string        = "staging"
	MaxStagingDataSizeDefault                    int64         = 500 * 1024 * 1024 * 1024 // 500GB
	MaxCacheFileSizeDefault                      int64         = 1 * 1024 * 1024 * 1024   // 1GB
	StagingDataGracePeriodDefault                time.Duration = 10 * time.Second
	SessionCloseGracePeriodDefault               time.Duration = 30 * time.Second
	OperationTimeoutDefault                      time.Duration = 5 * time.Minute

	// Packed directory defaults. Packing is on: the directories it covers hold
	// tens of thousands of small files that each cost a round trip through the
	// per-file staging path, and none of them is meant to be browsed in iRODS.
	// It does change how they are stored there, into one data object per
	// directory, so the name list is worth reviewing for a given deployment.
	PackedDirectoriesEnabledDefault   bool          = true
	PackedDirectorySuffixDefault      string        = ".mount.tar"
	PackedDirectoryCompressionDefault string        = "none"
	MaxPackedDirectorySizeDefault     int64         = 5 * 1024 * 1024 * 1024 // 5GB
	PackedSnapshotIntervalDefault     time.Duration = 30 * time.Minute
	ConcurrentPackLimitDefault        int           = 2

	ManagementServiceEndpointDefault string = "http://0.0.0.0:12021"

	// ClientIDMetadataKey is the gRPC metadata a client sends its own id in.
	// The server makes a connection id of its own, but that one changes
	// whenever the transport reconnects, so it cannot name a client for
	// anything that has to outlive a reconnect - the owner of a file lock,
	// most of all.
	ClientIDMetadataKey string = "x-irodsfs-client-id"
)
