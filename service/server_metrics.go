package service

import (
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
	promCounterForLoginFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_login_failures_total",
		Help: "The total number of login failures",
	})
	promCounterForLogins = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_logins_total",
		Help: "The total number of logins",
	})

	// iRODSFS-Pool metrics
	promCounterForGRPCRequests = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_grpc_requests_total",
		Help: "The total number of GRPC requests",
	})
	promCounterForGRPCResponses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_grpc_responses_total",
		Help: "The total number of GRPC responses",
	})
	promCounterForGRPCRequestsTimedout = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_grpc_requests_timedout_total",
		Help: "The total number of GRPC requests timedout",
	})
	promCounterForGRPCRequestsCanceled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_grpc_requests_canceled_total",
		Help: "The total number of GRPC requests canceled",
	})
	promCounterForGRPCClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "irodsfs_pool_grpc_clients",
		Help: "The number of GRPC clients",
	})
)

// GetTotalMetrics returns the current total: accumulated (terminated sessions) + active sessions
func (server *PoolServer) GetTotalMetrics() AccumulatedMetrics {
	server.metricsMutex.Lock()
	total := server.accumulatedMetrics
	server.metricsMutex.Unlock()

	sessions := server.sessionManager.GetAllSessions()
	for _, session := range sessions {
		session.mutex.RLock()
		// Skip sessions that are being released — their metrics are already in accumulatedMetrics.
		if session.releasing || session.fsClient == nil {
			session.mutex.RUnlock()
			continue
		}

		metric := session.fsClient.GetMetrics()
		if metric == nil {
			session.mutex.RUnlock()
			continue
		}

		total.Stat += metric.GetCounterForStat()
		total.List += metric.GetCounterForList()
		total.Search += metric.GetCounterForSearch()
		total.CollectionCreate += metric.GetCounterForCollectionCreate()
		total.CollectionDelete += metric.GetCounterForCollectionDelete()
		total.CollectionRename += metric.GetCounterForCollectionRename()
		total.DataObjectCreate += metric.GetCounterForDataObjectCreate()
		total.DataObjectOpen += metric.GetCounterForDataObjectOpen()
		total.DataObjectClose += metric.GetCounterForDataObjectClose()
		total.DataObjectDelete += metric.GetCounterForDataObjectDelete()
		total.DataObjectRename += metric.GetCounterForDataObjectRename()
		total.DataObjectUpdate += metric.GetCounterForDataObjectUpdate()
		total.DataObjectCopy += metric.GetCounterForDataObjectCopy()
		total.DataObjectRead += metric.GetCounterForDataObjectRead()
		total.DataObjectWrite += metric.GetCounterForDataObjectWrite()
		total.MetadataList += metric.GetCounterForMetadataList()
		total.MetadataCreate += metric.GetCounterForMetadataCreate()
		total.MetadataDelete += metric.GetCounterForMetadataDelete()
		total.MetadataUpdate += metric.GetCounterForMetadataUpdate()
		total.AccessList += metric.GetCounterForAccessList()
		total.AccessUpdate += metric.GetCounterForAccessUpdate()
		total.BytesSent += metric.GetBytesSent()
		total.BytesReceived += metric.GetBytesReceived()
		total.CacheHit += metric.GetCounterForCacheHit()
		total.CacheMiss += metric.GetCounterForCacheMiss()
		total.RequestFailures += metric.GetCounterForRequestResponseFailures()
		total.ConnectionFailures += metric.GetCounterForConnectionFailures()
		total.ConnectionPoolFailures += metric.GetCounterForConnectionPoolFailures()
		session.mutex.RUnlock()
	}

	return total
}

func (server *PoolServer) CollectPrometheusMetrics() {
	current := server.GetTotalMetrics()
	server.metricsMutex.Lock()
	last := &server.lastReportedMetrics

	promCounterForStat.Add(float64(current.Stat - last.Stat))
	promCounterForList.Add(float64(current.List - last.List))
	promCounterForSearch.Add(float64(current.Search - last.Search))
	promCounterForCollectionCreate.Add(float64(current.CollectionCreate - last.CollectionCreate))
	promCounterForCollectionDelete.Add(float64(current.CollectionDelete - last.CollectionDelete))
	promCounterForCollectionRename.Add(float64(current.CollectionRename - last.CollectionRename))
	promCounterForDataObjectCreate.Add(float64(current.DataObjectCreate - last.DataObjectCreate))
	promCounterForDataObjectOpen.Add(float64(current.DataObjectOpen - last.DataObjectOpen))
	promCounterForDataObjectClose.Add(float64(current.DataObjectClose - last.DataObjectClose))
	promCounterForDataObjectDelete.Add(float64(current.DataObjectDelete - last.DataObjectDelete))
	promCounterForDataObjectRename.Add(float64(current.DataObjectRename - last.DataObjectRename))
	promCounterForDataObjectUpdate.Add(float64(current.DataObjectUpdate - last.DataObjectUpdate))
	promCounterForDataObjectCopy.Add(float64(current.DataObjectCopy - last.DataObjectCopy))
	promCounterForDataObjectRead.Add(float64(current.DataObjectRead - last.DataObjectRead))
	promCounterForDataObjectWrite.Add(float64(current.DataObjectWrite - last.DataObjectWrite))
	promCounterForMetadataList.Add(float64(current.MetadataList - last.MetadataList))
	promCounterForMetadataDelete.Add(float64(current.MetadataDelete - last.MetadataDelete))
	promCounterForMetadataUpdate.Add(float64(current.MetadataUpdate - last.MetadataUpdate))
	promCounterForAccessList.Add(float64(current.AccessList - last.AccessList))
	promCounterForAccessUpdate.Add(float64(current.AccessUpdate - last.AccessUpdate))
	promCounterForBytesSent.Add(float64(current.BytesSent - last.BytesSent))
	promCounterForBytesReceived.Add(float64(current.BytesReceived - last.BytesReceived))
	promCounterForCacheHit.Add(float64(current.CacheHit - last.CacheHit))
	promCounterForCacheMiss.Add(float64(current.CacheMiss - last.CacheMiss))
	promCounterForRequestResponseFailures.Add(float64(current.RequestFailures - last.RequestFailures))
	promCounterForConnectionFailures.Add(float64(current.ConnectionFailures - last.ConnectionFailures))
	promCounterForConnectionPoolFailures.Add(float64(current.ConnectionPoolFailures - last.ConnectionPoolFailures))

	server.lastReportedMetrics = current
	server.metricsMutex.Unlock()

	// gauges: set directly from active sessions
	sessions := server.sessionManager.GetAllSessions()
	var openFileHandles, connectionsOpened, connectionsOccupied uint64
	for _, session := range sessions {
		session.mutex.RLock()
		if session.releasing || session.fsClient == nil {
			session.mutex.RUnlock()
			continue
		}

		metric := session.fsClient.GetMetrics()
		if metric == nil {
			session.mutex.RUnlock()
			continue
		}
		openFileHandles += metric.GetCounterForOpenFileHandles()
		connectionsOpened += metric.GetConnectionsOpened()
		connectionsOccupied += metric.GetConnectionsOccupied()
		session.mutex.RUnlock()
	}
	promGaugeForOpenFileHandles.Set(float64(openFileHandles))
	promGaugeForConnectionsOpened.Set(float64(connectionsOpened))
	promGaugeForConnectionsOccupied.Set(float64(connectionsOccupied))
}

// CollectSessionMetrics captures a session's final metrics before it is released.
func (server *PoolServer) CollectSessionMetrics(session *PoolSession) {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	if session.fsClient == nil {
		return
	}
	metric := session.fsClient.GetMetrics()
	if metric == nil {
		return
	}

	server.metricsMutex.Lock()
	defer server.metricsMutex.Unlock()

	server.accumulatedMetrics.Stat += metric.GetCounterForStat()
	server.accumulatedMetrics.List += metric.GetCounterForList()
	server.accumulatedMetrics.Search += metric.GetCounterForSearch()
	server.accumulatedMetrics.CollectionCreate += metric.GetCounterForCollectionCreate()
	server.accumulatedMetrics.CollectionDelete += metric.GetCounterForCollectionDelete()
	server.accumulatedMetrics.CollectionRename += metric.GetCounterForCollectionRename()
	server.accumulatedMetrics.DataObjectCreate += metric.GetCounterForDataObjectCreate()
	server.accumulatedMetrics.DataObjectOpen += metric.GetCounterForDataObjectOpen()
	server.accumulatedMetrics.DataObjectClose += metric.GetCounterForDataObjectClose()
	server.accumulatedMetrics.DataObjectDelete += metric.GetCounterForDataObjectDelete()
	server.accumulatedMetrics.DataObjectRename += metric.GetCounterForDataObjectRename()
	server.accumulatedMetrics.DataObjectUpdate += metric.GetCounterForDataObjectUpdate()
	server.accumulatedMetrics.DataObjectCopy += metric.GetCounterForDataObjectCopy()
	server.accumulatedMetrics.DataObjectRead += metric.GetCounterForDataObjectRead()
	server.accumulatedMetrics.DataObjectWrite += metric.GetCounterForDataObjectWrite()
	server.accumulatedMetrics.MetadataList += metric.GetCounterForMetadataList()
	server.accumulatedMetrics.MetadataCreate += metric.GetCounterForMetadataCreate()
	server.accumulatedMetrics.MetadataDelete += metric.GetCounterForMetadataDelete()
	server.accumulatedMetrics.MetadataUpdate += metric.GetCounterForMetadataUpdate()
	server.accumulatedMetrics.AccessList += metric.GetCounterForAccessList()
	server.accumulatedMetrics.AccessUpdate += metric.GetCounterForAccessUpdate()
	server.accumulatedMetrics.BytesSent += metric.GetBytesSent()
	server.accumulatedMetrics.BytesReceived += metric.GetBytesReceived()
	server.accumulatedMetrics.CacheHit += metric.GetCounterForCacheHit()
	server.accumulatedMetrics.CacheMiss += metric.GetCounterForCacheMiss()
	server.accumulatedMetrics.RequestFailures += metric.GetCounterForRequestResponseFailures()
	server.accumulatedMetrics.ConnectionFailures += metric.GetCounterForConnectionFailures()
	server.accumulatedMetrics.ConnectionPoolFailures += metric.GetCounterForConnectionPoolFailures()
}
