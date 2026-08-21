package commons

import "time"

const (
	DataRootPathFallback                         string        = "/var/lib/irodsfs_pool"
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
	StagingDataGracePeriodDefault                time.Duration = 10 * time.Second
	OperationTimeoutDefault                      time.Duration = 5 * time.Minute

	MonitoringServicePortDefault int = 12021
)

const ()
