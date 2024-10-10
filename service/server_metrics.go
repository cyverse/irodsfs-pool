package service

import (
	irodsclient_metrics "github.com/cyverse/go-irodsclient/irods/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	promCounterForStat = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_stat_ops_total",
		Help: "The total number of stat calls",
	})
	promCounterForList = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_list_ops_total",
		Help: "The total number of list calls",
	})
	promCounterForSearch = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_search_ops_total",
		Help: "The total number of search calls",
	})
	promCounterForCollectionCreate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_create_collection_ops_total",
		Help: "The total number of create collection calls",
	})
	promCounterForCollectionDelete = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_delete_collection_ops_total",
		Help: "The total number of delete collection calls",
	})
	promCounterForCollectionRename = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_rename_collection_ops_total",
		Help: "The total number of rename collection calls",
	})
	promCounterForDataObjectCreate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_create_data_object_ops_total",
		Help: "The total number of create data object calls",
	})
	promCounterForDataObjectOpen = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_open_data_object_ops_total",
		Help: "The total number of open data object calls",
	})
	promCounterForDataObjectClose = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_close_data_object_ops_total",
		Help: "The total number of close data object calls",
	})
	promCounterForDataObjectDelete = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_delete_data_object_ops_total",
		Help: "The total number of delete data object calls",
	})
	promCounterForDataObjectRename = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_rename_data_object_ops_total",
		Help: "The total number of rename data object calls",
	})
	promCounterForDataObjectUpdate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_update_data_object_ops_total",
		Help: "The total number of update data object calls",
	})
	promCounterForDataObjectCopy = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_copy_data_object_ops_total",
		Help: "The total number of copy data object calls",
	})
	promCounterForDataObjectRead = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_read_data_object_ops_total",
		Help: "The total number of read data object calls",
	})
	promCounterForDataObjectWrite = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_write_data_object_ops_total",
		Help: "The total number of write data object calls",
	})
	promCounterForMetadataList = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_list_metadata_ops_total",
		Help: "The total number of list metadata calls",
	})
	promCounterForMetadataDelete = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_delete_metadata_ops_total",
		Help: "The total number of delete metadata calls",
	})
	promCounterForMetadataUpdate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_update_metadata_ops_total",
		Help: "The total number of update metadata calls",
	})
	promCounterForAccessList = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_list_access_ops_total",
		Help: "The total number of list access calls",
	})
	promCounterForAccessUpdate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_update_access_ops_total",
		Help: "The total number of update access calls",
	})
	promCounterForBytesSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_bytes_sent_total",
		Help: "The total number of bytes sent",
	})
	promCounterForBytesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_bytes_received_total",
		Help: "The total number of bytes received",
	})
	promCounterForCacheHit = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_cache_hit_total",
		Help: "The total number of cache hit",
	})
	promCounterForCacheMiss = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_cache_miss_total",
		Help: "The total number of cache miss",
	})
	promGaugeForOpenFileHandles = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "irodsfs_pool_open_file_handles",
		Help: "The number of open file handles",
	})
	promGaugeForConnectionsOpened = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "irodsfs_pool_open_connections",
		Help: "The number of open connections",
	})
	promGaugeForConnectionsOccupied = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "irodsfs_pool_occupied_connections",
		Help: "The number of occupied connections",
	})
	promCounterForRequestResponseFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_request_response_failures_total",
		Help: "The total number of request/response failures",
	})
	promCounterForConnectionFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_connection_failures_total",
		Help: "The total number of connection failures",
	})
	promCounterForConnectionPoolFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_connection_pool_failures_total",
		Help: "The total number of connection pool failures",
	})

	// iRODSFS-Pool metrics
	promCounterForGRPCCalls = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_grpc_calls_total",
		Help: "The total number of GRPC calls",
	})
	promCounterForGRPCClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "irodsfs_pool_grpc_clients",
		Help: "The number of GRPC clients",
	})
	promCounterForCacheEventSubscriptions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "irodsfs_pool_cache_event_subscriptions",
		Help: "The number of cache event subscriptions",
	})
)

func (server *PoolServer) CollectPrometheusMetrics() {
	metrics := server.CollectMetrics()

	stat := metrics.GetCounterForStat()
	promCounterForStat.Add(float64(stat))

	list := metrics.GetCounterForList()
	promCounterForList.Add(float64(list))

	search := metrics.GetCounterForSearch()
	promCounterForSearch.Add(float64(search))

	collectionCreate := metrics.GetCounterForCollectionCreate()
	promCounterForCollectionCreate.Add(float64(collectionCreate))

	collectionDelete := metrics.GetCounterForCollectionDelete()
	promCounterForCollectionDelete.Add(float64(collectionDelete))

	collectionRename := metrics.GetCounterForCollectionRename()
	promCounterForCollectionRename.Add(float64(collectionRename))

	dataObjectCreate := metrics.GetCounterForDataObjectCreate()
	promCounterForDataObjectCreate.Add(float64(dataObjectCreate))

	dataObjectOpen := metrics.GetCounterForDataObjectOpen()
	promCounterForDataObjectOpen.Add(float64(dataObjectOpen))

	dataObjectClose := metrics.GetCounterForDataObjectClose()
	promCounterForDataObjectClose.Add(float64(dataObjectClose))

	dataObjectDelete := metrics.GetCounterForDataObjectDelete()
	promCounterForDataObjectDelete.Add(float64(dataObjectDelete))

	dataObjectRename := metrics.GetCounterForDataObjectRename()
	promCounterForDataObjectRename.Add(float64(dataObjectRename))

	dataObjectUpdate := metrics.GetCounterForDataObjectUpdate()
	promCounterForDataObjectUpdate.Add(float64(dataObjectUpdate))

	dataObjectCopy := metrics.GetCounterForDataObjectCopy()
	promCounterForDataObjectCopy.Add(float64(dataObjectCopy))

	dataObjectRead := metrics.GetCounterForDataObjectRead()
	promCounterForDataObjectRead.Add(float64(dataObjectRead))

	dataObjectWrite := metrics.GetCounterForDataObjectWrite()
	promCounterForDataObjectWrite.Add(float64(dataObjectWrite))

	metadataList := metrics.GetCounterForMetadataList()
	promCounterForMetadataList.Add(float64(metadataList))

	metadataDelete := metrics.GetCounterForMetadataDelete()
	promCounterForMetadataDelete.Add(float64(metadataDelete))

	metadataUpdate := metrics.GetCounterForMetadataUpdate()
	promCounterForMetadataUpdate.Add(float64(metadataUpdate))

	accessList := metrics.GetCounterForAccessList()
	promCounterForAccessList.Add(float64(accessList))

	accessUpdate := metrics.GetCounterForAccessUpdate()
	promCounterForAccessUpdate.Add(float64(accessUpdate))

	bytesSent := metrics.GetBytesSent()
	promCounterForBytesSent.Add(float64(bytesSent))

	bytesReceived := metrics.GetBytesReceived()
	promCounterForBytesReceived.Add(float64(bytesReceived))

	cacheHit := metrics.GetCounterForCacheHit()
	promCounterForCacheHit.Add(float64(cacheHit))

	cacheMiss := metrics.GetCounterForCacheMiss()
	promCounterForCacheMiss.Add(float64(cacheMiss))

	newOpenFileHandles := metrics.GetCounterForOpenFileHandles()
	promGaugeForOpenFileHandles.Set(float64(newOpenFileHandles))

	newConnectionsOpened := metrics.GetConnectionsOpened()
	promGaugeForConnectionsOpened.Set(float64(newConnectionsOpened))

	newConnectionsOccupied := metrics.GetConnectionsOccupied()
	promGaugeForConnectionsOccupied.Set(float64(newConnectionsOccupied))

	requestResponseFailures := metrics.GetCounterForRequestResponseFailures()
	promCounterForRequestResponseFailures.Add(float64(requestResponseFailures))

	connectionFailures := metrics.GetCounterForConnectionFailures()
	promCounterForConnectionFailures.Add(float64(connectionFailures))

	connectionPoolFailures := metrics.GetCounterForConnectionPoolFailures()
	promCounterForConnectionPoolFailures.Add(float64(connectionPoolFailures))

}

func (server *PoolServer) CollectMetrics() *irodsclient_metrics.IRODSMetrics {
	server.mutex.Lock()

	instances := server.sessionManager.GetIRODSFSClientInstances()
	metrics := make([]*irodsclient_metrics.IRODSMetrics, len(instances))

	idx := 0
	for _, instance := range instances {
		metrics[idx] = instance.GetFSClient().GetMetrics()
		idx++
	}
	server.mutex.Unlock()

	// sum up
	metricsTotal := irodsclient_metrics.IRODSMetrics{}
	for _, metric := range metrics {
		// sum
		metricsTotal.IncreaseCounterForStat(metric.GetAndClearCounterForStat())
		metricsTotal.IncreaseCounterForList(metric.GetAndClearCounterForList())
		metricsTotal.IncreaseCounterForSearch(metric.GetAndClearCounterForSearch())
		metricsTotal.IncreaseCounterForCollectionCreate(metric.GetAndClearCounterForCollectionCreate())
		metricsTotal.IncreaseCounterForCollectionDelete(metric.GetAndClearCounterForCollectionDelete())
		metricsTotal.IncreaseCounterForCollectionRename(metric.GetAndClearCounterForCollectionRename())
		metricsTotal.IncreaseCounterForDataObjectCreate(metric.GetAndClearCounterForDataObjectCreate())
		metricsTotal.IncreaseCounterForDataObjectOpen(metric.GetAndClearCounterForDataObjectOpen())
		metricsTotal.IncreaseCounterForDataObjectClose(metric.GetAndClearCounterForDataObjectClose())
		metricsTotal.IncreaseCounterForDataObjectDelete(metric.GetAndClearCounterForDataObjectDelete())
		metricsTotal.IncreaseCounterForDataObjectRename(metric.GetAndClearCounterForDataObjectRename())
		metricsTotal.IncreaseCounterForDataObjectUpdate(metric.GetAndClearCounterForDataObjectUpdate())
		metricsTotal.IncreaseCounterForDataObjectCopy(metric.GetAndClearCounterForDataObjectCopy())
		metricsTotal.IncreaseCounterForDataObjectRead(metric.GetAndClearCounterForDataObjectRead())
		metricsTotal.IncreaseCounterForDataObjectWrite(metric.GetAndClearCounterForDataObjectWrite())
		metricsTotal.IncreaseCounterForMetadataList(metric.GetAndClearCounterForMetadataList())
		metricsTotal.IncreaseCounterForMetadataCreate(metric.GetAndClearCounterForMetadataCreate())
		metricsTotal.IncreaseCounterForMetadataDelete(metric.GetAndClearCounterForMetadataDelete())
		metricsTotal.IncreaseCounterForMetadataUpdate(metric.GetAndClearCounterForMetadataUpdate())
		metricsTotal.IncreaseCounterForAccessList(metric.GetAndClearCounterForAccessList())
		metricsTotal.IncreaseCounterForAccessUpdate(metric.GetAndClearCounterForAccessUpdate())
		metricsTotal.IncreaseBytesSent(metric.GetAndClearBytesSent())
		metricsTotal.IncreaseBytesReceived(metric.GetAndClearBytesReceived())
		metricsTotal.IncreaseCounterForCacheHit(metric.GetAndClearCounterForCacheHit())
		metricsTotal.IncreaseCounterForCacheMiss(metric.GetAndClearCounterForCacheMiss())
		metricsTotal.IncreaseCounterForOpenFileHandles(metric.GetCounterForOpenFileHandles())
		metricsTotal.IncreaseConnectionsOpened(metric.GetConnectionsOpened())
		metricsTotal.IncreaseConnectionsOccupied(metric.GetConnectionsOccupied())
		metricsTotal.IncreaseCounterForRequestResponseFailures(metric.GetAndClearCounterForRequestResponseFailures())
		metricsTotal.IncreaseCounterForConnectionFailures(metric.GetAndClearCounterForConnectionFailures())
		metricsTotal.IncreaseCounterForConnectionPoolFailures(metric.GetAndClearCounterForConnectionPoolFailures())
	}

	return &metricsTotal
}
