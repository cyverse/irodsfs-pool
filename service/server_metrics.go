package service

import (
	irodsclient_metrics "github.com/cyverse/go-irodsclient/irods/metrics"
	irodsfs_common "github.com/cyverse/irodsfs-common/irods"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	promCounterForStat = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_stat_ops_total",
		Help: "The total number of stat calls",
	})
	oldCounterForStat uint64 = 0

	promCounterForList = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_list_ops_total",
		Help: "The total number of list calls",
	})
	oldCounterForList uint64 = 0

	promCounterForSearch = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_search_ops_total",
		Help: "The total number of search calls",
	})
	oldCounterForSearch uint64 = 0

	promCounterForCollectionCreate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_create_collection_ops_total",
		Help: "The total number of create collection calls",
	})
	oldCounterForCollectionCreate uint64 = 0

	promCounterForCollectionDelete = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_delete_collection_ops_total",
		Help: "The total number of delete collection calls",
	})
	oldCounterForCollectionDelete uint64 = 0

	promCounterForCollectionRename = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_rename_collection_ops_total",
		Help: "The total number of rename collection calls",
	})
	oldCounterForCollectionRename uint64 = 0

	promCounterForDataObjectCreate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_create_data_object_ops_total",
		Help: "The total number of create data object calls",
	})
	oldCounterForDataObjectCreate uint64 = 0

	promCounterForDataObjectOpen = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_open_data_object_ops_total",
		Help: "The total number of open data object calls",
	})
	oldCounterForDataObjectOpen uint64 = 0

	promCounterForDataObjectClose = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_close_data_object_ops_total",
		Help: "The total number of close data object calls",
	})
	oldCounterForDataObjectClose uint64 = 0

	promCounterForDataObjectDelete = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_delete_data_object_ops_total",
		Help: "The total number of delete data object calls",
	})
	oldCounterForDataObjectDelete uint64 = 0

	promCounterForDataObjectRename = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_rename_data_object_ops_total",
		Help: "The total number of rename data object calls",
	})
	oldCounterForDataObjectRename uint64 = 0

	promCounterForDataObjectUpdate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_update_data_object_ops_total",
		Help: "The total number of update data object calls",
	})
	oldCounterForDataObjectUpdate uint64 = 0

	promCounterForDataObjectCopy = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_copy_data_object_ops_total",
		Help: "The total number of copy data object calls",
	})
	oldCounterForDataObjectCopy uint64 = 0

	promCounterForDataObjectRead = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_read_data_object_ops_total",
		Help: "The total number of read data object calls",
	})
	oldCounterForDataObjectRead uint64 = 0

	promCounterForDataObjectWrite = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_write_data_object_ops_total",
		Help: "The total number of write data object calls",
	})
	oldCounterForDataObjectWrite uint64 = 0

	promCounterForMetadataList = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_list_metadata_ops_total",
		Help: "The total number of list metadata calls",
	})
	oldCounterForMetadataList uint64 = 0

	promCounterForMetadataDelete = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_delete_metadata_ops_total",
		Help: "The total number of delete metadata calls",
	})
	oldCounterForMetadataDelete uint64 = 0

	promCounterForMetadataUpdate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_update_metadata_ops_total",
		Help: "The total number of update metadata calls",
	})
	oldCounterForMetadataUpdate uint64 = 0

	promCounterForAccessList = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_list_access_ops_total",
		Help: "The total number of list access calls",
	})
	oldCounterForAccessList uint64 = 0

	promCounterForAccessUpdate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_update_access_ops_total",
		Help: "The total number of update access calls",
	})
	oldCounterForAccessUpdate uint64 = 0

	promCounterForBytesSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_bytes_sent_total",
		Help: "The total number of bytes sent",
	})
	oldCounterForBytesSent uint64 = 0

	promCounterForBytesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_bytes_received_total",
		Help: "The total number of bytes received",
	})
	oldCounterForBytesReceived uint64 = 0

	promCounterForCacheHit = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_cache_hit_total",
		Help: "The total number of cache hit",
	})
	oldCounterForCacheHit uint64 = 0

	promCounterForCacheMiss = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_cache_miss_total",
		Help: "The total number of cache miss",
	})
	oldCounterForCacheMiss uint64 = 0

	promGaugeForOpenFileHandles = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "irodsfs_pool_open_file_handles_total",
		Help: "The total number of open file handles",
	})

	promGaugeForConnectionsOpened = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "irodsfs_pool_open_connections_total",
		Help: "The total number of open connections",
	})

	promGaugeForConnectionsOccupied = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "irodsfs_pool_occupied_connections_total",
		Help: "The total number of occupied connections",
	})

	promCounterForRequestResponseFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_request_response_failures_total",
		Help: "The total number of request/response failures",
	})
	oldCounterForRequestResponseFailures uint64 = 0

	promCounterForConnectionPoolFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "irodsfs_pool_connection_pool_failures_total",
		Help: "The total number of connection pool failures",
	})
	oldCounterForConnectionPoolFailures uint64 = 0
)

func (server *PoolServer) CollectPrometheusMetrics() {
	metrics := server.CollectMetrics()

	newStat := metrics.GetCounterForStat()
	promCounterForStat.Add(float64(newStat - oldCounterForStat))
	oldCounterForStat = newStat

	newList := metrics.GetCounterForList()
	promCounterForList.Add(float64(newList - oldCounterForList))
	oldCounterForList = newList

	newSearch := metrics.GetCounterForSearch()
	promCounterForSearch.Add(float64(newSearch - oldCounterForSearch))
	oldCounterForSearch = newSearch

	newCollectionCreate := metrics.GetCounterForCollectionCreate()
	promCounterForCollectionCreate.Add(float64(newCollectionCreate - oldCounterForCollectionCreate))
	oldCounterForCollectionCreate = newCollectionCreate

	newCollectionDelete := metrics.GetCounterForCollectionDelete()
	promCounterForCollectionDelete.Add(float64(newCollectionDelete - oldCounterForCollectionDelete))
	oldCounterForCollectionDelete = newCollectionDelete

	newCollectionRename := metrics.GetCounterForCollectionRename()
	promCounterForCollectionRename.Add(float64(newCollectionRename - oldCounterForCollectionRename))
	oldCounterForCollectionRename = newCollectionRename

	newDataObjectCreate := metrics.GetCounterForDataObjectCreate()
	promCounterForDataObjectCreate.Add(float64(newDataObjectCreate - oldCounterForDataObjectCreate))
	oldCounterForDataObjectCreate = newDataObjectCreate

	newDataObjectOpen := metrics.GetCounterForDataObjectOpen()
	promCounterForDataObjectOpen.Add(float64(newDataObjectOpen - oldCounterForDataObjectOpen))
	oldCounterForDataObjectOpen = newDataObjectOpen

	newDataObjectClose := metrics.GetCounterForDataObjectClose()
	promCounterForDataObjectClose.Add(float64(newDataObjectClose - oldCounterForDataObjectClose))
	oldCounterForDataObjectClose = newDataObjectClose

	newDataObjectDelete := metrics.GetCounterForDataObjectDelete()
	promCounterForDataObjectDelete.Add(float64(newDataObjectDelete - oldCounterForDataObjectDelete))
	oldCounterForDataObjectDelete = newDataObjectDelete

	newDataObjectRename := metrics.GetCounterForDataObjectRename()
	promCounterForDataObjectRename.Add(float64(newDataObjectRename - oldCounterForDataObjectRename))
	oldCounterForDataObjectRename = newDataObjectRename

	newDataObjectUpdate := metrics.GetCounterForDataObjectUpdate()
	promCounterForDataObjectUpdate.Add(float64(newDataObjectUpdate - oldCounterForDataObjectUpdate))
	oldCounterForDataObjectUpdate = newDataObjectUpdate

	newDataObjectCopy := metrics.GetCounterForDataObjectCopy()
	promCounterForDataObjectCopy.Add(float64(newDataObjectCopy - oldCounterForDataObjectCopy))
	oldCounterForDataObjectCopy = newDataObjectCopy

	newDataObjectRead := metrics.GetCounterForDataObjectRead()
	promCounterForDataObjectRead.Add(float64(newDataObjectRead - oldCounterForDataObjectRead))
	oldCounterForDataObjectRead = newDataObjectRead

	newDataObjectWrite := metrics.GetCounterForDataObjectWrite()
	promCounterForDataObjectWrite.Add(float64(newDataObjectWrite - oldCounterForDataObjectWrite))
	oldCounterForDataObjectWrite = newDataObjectWrite

	newMetadataList := metrics.GetCounterForMetadataList()
	promCounterForMetadataList.Add(float64(newMetadataList - oldCounterForMetadataList))
	oldCounterForMetadataList = newMetadataList

	newMetadataDelete := metrics.GetCounterForMetadataDelete()
	promCounterForMetadataDelete.Add(float64(newMetadataDelete - oldCounterForMetadataDelete))
	oldCounterForMetadataDelete = newMetadataDelete

	newMetadataUpdate := metrics.GetCounterForMetadataUpdate()
	promCounterForMetadataUpdate.Add(float64(newMetadataUpdate - oldCounterForMetadataUpdate))
	oldCounterForMetadataUpdate = newMetadataUpdate

	newAccessList := metrics.GetCounterForAccessList()
	promCounterForAccessList.Add(float64(newAccessList - oldCounterForAccessList))
	oldCounterForAccessList = newAccessList

	newAccessUpdate := metrics.GetCounterForAccessUpdate()
	promCounterForAccessUpdate.Add(float64(newAccessUpdate - oldCounterForAccessUpdate))
	oldCounterForAccessUpdate = newAccessUpdate

	newBytesSent := metrics.GetBytesSent()
	promCounterForBytesSent.Add(float64(newBytesSent - oldCounterForBytesSent))
	oldCounterForBytesSent = newBytesSent

	newBytesReceived := metrics.GetBytesReceived()
	promCounterForBytesReceived.Add(float64(newBytesReceived - oldCounterForBytesReceived))
	oldCounterForBytesReceived = newBytesReceived

	newCacheHit := metrics.GetCounterForCacheHit()
	promCounterForCacheHit.Add(float64(newCacheHit - oldCounterForCacheHit))
	oldCounterForCacheHit = newCacheHit

	newCacheMiss := metrics.GetCounterForCacheMiss()
	promCounterForCacheMiss.Add(float64(newCacheMiss - oldCounterForCacheMiss))
	oldCounterForCacheMiss = newCacheMiss

	newOpenFileHandles := metrics.GetCounterForOpenFileHandles()
	promGaugeForOpenFileHandles.Set(float64(newOpenFileHandles))

	newConnectionsOpened := metrics.GetConnectionsOpened()
	promGaugeForConnectionsOpened.Set(float64(newConnectionsOpened))

	newConnectionsOccupied := metrics.GetConnectionsOccupied()
	promGaugeForConnectionsOccupied.Set(float64(newConnectionsOccupied))

	newRequestResponseFailures := metrics.GetCounterForRequestResponseFailures()
	promCounterForRequestResponseFailures.Add(float64(newRequestResponseFailures - oldCounterForRequestResponseFailures))
	oldCounterForRequestResponseFailures = newRequestResponseFailures

	newConnectionPoolFailures := metrics.GetCounterForConnectionPoolFailures()
	promCounterForConnectionPoolFailures.Add(float64(newConnectionPoolFailures - oldCounterForConnectionPoolFailures))
	oldCounterForConnectionPoolFailures = newConnectionPoolFailures
}

func (server *PoolServer) CollectMetrics() irodsclient_metrics.IRODSMetrics {
	fsClients := []irodsfs_common.IRODSFSClient{}
	server.mutex.Lock()
	for _, fsClientInstance := range server.irodsFsClientInstances {
		fsClient := fsClientInstance.GetFSClient()
		fsClients = append(fsClients, fsClient)
	}
	server.mutex.Unlock()

	// sum up
	metricsTotal := irodsclient_metrics.IRODSMetrics{}
	for _, fsClient := range fsClients {
		metrics := fsClient.GetMetrics()
		// sum
		metricsTotal.IncreaseCounterForStat(metrics.GetCounterForStat())
		metricsTotal.IncreaseCounterForList(metrics.GetCounterForList())
		metricsTotal.IncreaseCounterForSearch(metrics.GetCounterForSearch())
		metricsTotal.IncreaseCounterForCollectionCreate(metrics.GetCounterForCollectionCreate())
		metricsTotal.IncreaseCounterForCollectionDelete(metrics.GetCounterForCollectionDelete())
		metricsTotal.IncreaseCounterForCollectionRename(metrics.GetCounterForCollectionRename())
		metricsTotal.IncreaseCounterForDataObjectCreate(metrics.GetCounterForDataObjectCreate())
		metricsTotal.IncreaseCounterForDataObjectOpen(metrics.GetCounterForDataObjectOpen())
		metricsTotal.IncreaseCounterForDataObjectClose(metrics.GetCounterForDataObjectClose())
		metricsTotal.IncreaseCounterForDataObjectDelete(metrics.GetCounterForDataObjectDelete())
		metricsTotal.IncreaseCounterForDataObjectRename(metrics.GetCounterForDataObjectRename())
		metricsTotal.IncreaseCounterForDataObjectUpdate(metrics.GetCounterForDataObjectUpdate())
		metricsTotal.IncreaseCounterForDataObjectCopy(metrics.GetCounterForDataObjectCopy())
		metricsTotal.IncreaseCounterForDataObjectRead(metrics.GetCounterForDataObjectRead())
		metricsTotal.IncreaseCounterForDataObjectWrite(metrics.GetCounterForDataObjectWrite())
		metricsTotal.IncreaseCounterForMetadataList(metrics.GetCounterForMetadataList())
		metricsTotal.IncreaseCounterForMetadataCreate(metrics.GetCounterForMetadataCreate())
		metricsTotal.IncreaseCounterForMetadataDelete(metrics.GetCounterForMetadataDelete())
		metricsTotal.IncreaseCounterForMetadataUpdate(metrics.GetCounterForMetadataUpdate())
		metricsTotal.IncreaseCounterForAccessList(metrics.GetCounterForAccessList())
		metricsTotal.IncreaseCounterForAccessUpdate(metrics.GetCounterForAccessUpdate())
		metricsTotal.IncreaseBytesSent(metrics.GetBytesSent())
		metricsTotal.IncreaseBytesReceived(metrics.GetBytesReceived())
		metricsTotal.IncreaseCounterForCacheHit(metrics.GetCounterForCacheHit())
		metricsTotal.IncreaseCounterForCacheMiss(metrics.GetCounterForCacheMiss())
		metricsTotal.IncreaseCounterForOpenFileHandles(metrics.GetCounterForOpenFileHandles())
		metricsTotal.IncreaseConnectionsOpened(metrics.GetConnectionsOpened())
		metricsTotal.IncreaseConnectionsOccupied(metrics.GetConnectionsOccupied())
		metricsTotal.IncreaseCounterForRequestResponseFailures(metrics.GetCounterForRequestResponseFailures())
		metricsTotal.IncreaseCounterForConnectionFailures(metrics.GetCounterForConnectionFailures())
		metricsTotal.IncreaseCounterForConnectionPoolFailures(metrics.GetCounterForConnectionPoolFailures())
	}

	return metricsTotal
}
