package commons

const (
	DataRootPathDefault     string = "/var/lib/irodsfs_pool"
	DataCacheSizeMaxDefault int64  = 1024 * 1024 * 1024 * 20 // 20GB
	OperationTimeoutDefault int    = 30
	SessionTimeoutDefault   int    = 30 * 60 // 30 minutes

	ProfileServicePortDefault     int = 12021
	PrometheusExporterPortDefault int = 12022
)
