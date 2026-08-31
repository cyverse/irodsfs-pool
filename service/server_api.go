package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-pool/commons"
)

type RESTAPIHandler struct {
	poolServer *PoolServer
	config     *commons.Config
	startTime  time.Time
}

type SystemInfo struct {
	Server      ServerInfo      `json:"server"`
	MemoryCache MemoryCacheInfo `json:"memory_cache"`
	Staging     StagingInfo     `json:"staging"`
	IOMetrics   IOMetricsInfo   `json:"io_metrics"`
}

type ServerInfo struct {
	Version            VersionInfo `json:"version"`
	UptimeSeconds      int64       `json:"uptime_seconds"`
	Endpoint           string      `json:"endpoint"`
	DataRootPath       string      `json:"data_root_path"`
	MonitoringEndpoint string      `json:"monitoring_endpoint"`
	MetricsEndpoint    string      `json:"metrics_endpoint"`
	RESTAPIEndpoint    string      `json:"rest_api_endpoint"`
}

type VersionInfo struct {
	ServiceVersion string `json:"service_version"`
	GitCommit      string `json:"git_commit"`
	BuildDate      string `json:"build_date"`
	GoVersion      string `json:"go_version"`
	Compiler       string `json:"compiler"`
	Platform       string `json:"platform"`
}

type MemoryCacheInfo struct {
	UsedBytes                  int64   `json:"used_bytes"`
	MaximumBytes               int64   `json:"maximum_bytes"`
	UsagePercent               float64 `json:"usage_percent"`
	EntryCount                 int     `json:"entry_count"`
	SystemMemoryTotalBytes     uint64  `json:"system_memory_total_bytes"`
	SystemMemoryAvailableBytes uint64  `json:"system_memory_available_bytes"`
	InsufficientMemory         bool    `json:"insufficient_memory"`
}

type StagingInfo struct {
	Path                  string `json:"path"`
	UsedBytes             int64  `json:"used_bytes"`
	MaximumBytes          int64  `json:"maximum_bytes"`
	FileCount             int    `json:"file_count"`
	DiskTotalBytes        uint64 `json:"disk_total_bytes"`
	DiskFreeBytes         uint64 `json:"disk_free_bytes"`
	InsufficientDiskSpace bool   `json:"insufficient_disk_space"`
}

type IOMetricsInfo struct {
	BytesSent            uint64  `json:"bytes_sent"`
	BytesReceived        uint64  `json:"bytes_received"`
	CacheHit             uint64  `json:"cache_hit"`
	CacheMiss            uint64  `json:"cache_miss"`
	CacheHitRatioPercent float64 `json:"cache_hit_ratio_percent"`
	RequestFailures      uint64  `json:"request_failures"`
	ConnectionFailures   uint64  `json:"connection_failures"`
}

type SessionSummary struct {
	ID                       string    `json:"id"`
	Host                     string    `json:"host"`
	Port                     int       `json:"port"`
	ClientUser               string    `json:"client_user"`
	ClientZone               string    `json:"client_zone"`
	ProxyUser                string    `json:"proxy_user,omitempty"`
	ProxyZone                string    `json:"proxy_zone,omitempty"`
	LastAccessTime           time.Time `json:"last_access_time"`
	ClientCount              int       `json:"client_count"`
	OpenFileHandleCount      int       `json:"open_file_handle_count"`
	OpenIRODSConnectionCount int       `json:"open_irods_connection_count"`
	InGracePeriod            bool      `json:"in_grace_period"`
	Releasing                bool      `json:"releasing"`
}

type SessionClientInfo struct {
	ConnectionID string `json:"connection_id"`
	Application  string `json:"application"`
	Description  string `json:"description,omitempty"`
}

type SessionFileHandleInfo struct {
	ID   string `json:"id"`
	Path string `json:"path"`
	Mode string `json:"mode"`
}

type SessionStagedFileInfo struct {
	Path             string    `json:"path"`
	OldPath          string    `json:"old_path,omitempty"`
	Action           string    `json:"action"`
	FileState        string    `json:"file_state"`
	LastModifiedTime time.Time `json:"last_modified_time"`
	SyncFailureCount int       `json:"sync_failure_count"`
}

type SessionInfo struct {
	SessionSummary
	Clients         []SessionClientInfo     `json:"clients"`
	OpenFileHandles []SessionFileHandleInfo `json:"open_file_handles"`
	StagedFiles     []SessionStagedFileInfo `json:"staged_files"`
}

type apiErrorResponse struct {
	Error string `json:"error"`
}

type MetadataCacheInvalidationResult struct {
	SessionID string `json:"session_id"`
	Success   bool   `json:"success"`
}

func NewRESTAPIHandler(poolServer *PoolServer, config *commons.Config) *RESTAPIHandler {
	return newRESTAPIHandler(poolServer, config, time.Now())
}

func newRESTAPIHandler(poolServer *PoolServer, config *commons.Config, startTime time.Time) *RESTAPIHandler {
	if config == nil {
		config = commons.NewDefaultConfig()
	}
	return &RESTAPIHandler{poolServer: poolServer, config: config, startTime: startTime}
}

func (h *RESTAPIHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/sysinfo", h.getSystemInfo)
	mux.HandleFunc("GET /api/sessions", h.listSessions)
	mux.HandleFunc("GET /api/sessions/{sessionID}", h.getSession)
	mux.HandleFunc("POST /api/sessions/{sessionID}/metadata-cache/invalidate", h.invalidateSessionMetadataCache)
	mux.HandleFunc("GET /api/recovery-sessions", h.listFailedSessions)
	mux.HandleFunc("GET /api/recovery-sessions/{sessionID}", h.getFailedSession)
	mux.HandleFunc("POST /api/recovery-sessions/{sessionID}/recover", h.recoverSession)
	mux.HandleFunc("POST /api/recovery-sessions/{sessionID}/discard", h.discardSessionStaging)
}

func (h *RESTAPIHandler) getSystemInfo(w http.ResponseWriter, _ *http.Request) {
	version := commons.GetVersion()
	uptime := time.Since(h.startTime)
	if uptime < 0 {
		uptime = 0
	}

	port := h.config.MonitoringServicePort
	info := SystemInfo{
		Server: ServerInfo{
			Version: VersionInfo{
				ServiceVersion: version.ServiceVersion,
				GitCommit:      version.GitCommit,
				BuildDate:      version.BuildDate,
				GoVersion:      version.GoVersion,
				Compiler:       version.Compiler,
				Platform:       version.Platform,
			},
			UptimeSeconds:      int64(uptime / time.Second),
			Endpoint:           h.config.GetServiceEndpoint(),
			DataRootPath:       h.config.DataRootPath,
			MonitoringEndpoint: fmt.Sprintf(":%d/monitor", port),
			MetricsEndpoint:    fmt.Sprintf(":%d/metrics", port),
			RESTAPIEndpoint:    fmt.Sprintf(":%d/api", port),
		},
		MemoryCache: h.getMemoryCacheInfo(),
		Staging:     h.getStagingInfo(),
		IOMetrics:   h.getIOMetricsInfo(),
	}

	writeJSON(w, http.StatusOK, info)
}

func (h *RESTAPIHandler) getMemoryCacheInfo() MemoryCacheInfo {
	var usedBytes, maximumBytes int64
	var entryCount int
	if cacheManager := h.poolServer.GetSessionManager().GetCacheManager(); cacheManager != nil {
		usedBytes = cacheManager.GetTotalSize()
		maximumBytes = cacheManager.GetMaxSize()
		entryCount = cacheManager.GetCount()
	}

	var usagePercent float64
	if maximumBytes > 0 {
		usagePercent = float64(usedBytes) / float64(maximumBytes) * 100
	}
	systemTotal, systemAvailable := getSystemMemoryInfo()

	return MemoryCacheInfo{
		UsedBytes:                  usedBytes,
		MaximumBytes:               maximumBytes,
		UsagePercent:               usagePercent,
		EntryCount:                 entryCount,
		SystemMemoryTotalBytes:     systemTotal,
		SystemMemoryAvailableBytes: systemAvailable,
		InsufficientMemory:         systemAvailable > 0 && systemAvailable < uint64(maximumBytes),
	}
}

func (h *RESTAPIHandler) getStagingInfo() StagingInfo {
	stagingPath := h.config.StagingRootPath
	if stagingPath == "" {
		stagingPath = h.config.GetDataStagingRootPath()
	}

	var usedBytes int64
	var fileCount int
	for _, session := range h.poolServer.GetSessionManager().GetAllSessions() {
		session.mutex.RLock()
		if bufferedClient, ok := session.fsClient.(*irodsfs_common_irods.IRODSFSClientBuffered); ok {
			if stagingFS := bufferedClient.GetStagingFS(); stagingFS != nil {
				usedBytes += stagingFS.GetCurrentDataSize()
				fileCount += len(stagingFS.GetAll())
			}
		}
		session.mutex.RUnlock()
	}

	diskTotal, diskFree := getDiskInfo(stagingPath)
	maximumBytes := h.config.MaxStagingDataSize
	return StagingInfo{
		Path:                  stagingPath,
		UsedBytes:             usedBytes,
		MaximumBytes:          maximumBytes,
		FileCount:             fileCount,
		DiskTotalBytes:        diskTotal,
		DiskFreeBytes:         diskFree,
		InsufficientDiskSpace: diskFree > 0 && diskFree < uint64(maximumBytes),
	}
}

func (h *RESTAPIHandler) getIOMetricsInfo() IOMetricsInfo {
	metrics := h.poolServer.GetTotalMetrics()
	var hitRatio float64
	if metrics.CacheHit+metrics.CacheMiss > 0 {
		hitRatio = float64(metrics.CacheHit) / float64(metrics.CacheHit+metrics.CacheMiss) * 100
	}

	return IOMetricsInfo{
		BytesSent:            metrics.BytesSent,
		BytesReceived:        metrics.BytesReceived,
		CacheHit:             metrics.CacheHit,
		CacheMiss:            metrics.CacheMiss,
		CacheHitRatioPercent: hitRatio,
		RequestFailures:      metrics.RequestFailures,
		ConnectionFailures:   metrics.ConnectionFailures,
	}
}

func (h *RESTAPIHandler) listSessions(w http.ResponseWriter, _ *http.Request) {
	sessions := h.poolServer.GetSessionManager().GetAllSessions()
	result := make([]SessionInfo, 0, len(sessions))
	for _, session := range sessions {
		result = append(result, snapshotSessionInfo(session))
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	writeJSON(w, http.StatusOK, result)
}

func (h *RESTAPIHandler) getSession(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.PathValue("sessionID"))
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, apiErrorResponse{Error: "session ID is required"})
		return
	}

	session, err := h.poolServer.GetSessionManager().GetSession(sessionID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, apiErrorResponse{Error: "session not found"})
		return
	}

	writeJSON(w, http.StatusOK, snapshotSessionInfo(session))
}

func (h *RESTAPIHandler) invalidateSessionMetadataCache(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.PathValue("sessionID"))
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, apiErrorResponse{Error: "session ID is required"})
		return
	}

	err := h.poolServer.GetSessionManager().InvalidateSessionMetadataCache(sessionID)
	if err != nil {
		if commons.IsSessionNotFoundError(err) {
			writeJSON(w, http.StatusNotFound, apiErrorResponse{Error: "session not found"})
			return
		}

		writeJSON(w, http.StatusConflict, apiErrorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, MetadataCacheInvalidationResult{
		SessionID: sessionID,
		Success:   true,
	})
}

func (h *RESTAPIHandler) listFailedSessions(w http.ResponseWriter, _ *http.Request) {
	sessions, err := h.poolServer.GetSessionManager().GetFailedSessions()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiErrorResponse{Error: "failed to list sessions pending recovery"})
		return
	}
	writeJSON(w, http.StatusOK, sessions)
}

func (h *RESTAPIHandler) getFailedSession(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.PathValue("sessionID"))
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, apiErrorResponse{Error: "session ID is required"})
		return
	}

	session, err := h.poolServer.GetSessionManager().GetFailedSession(sessionID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiErrorResponse{Error: "failed to read session pending recovery"})
		return
	}
	if session == nil {
		writeJSON(w, http.StatusNotFound, apiErrorResponse{Error: "recovery session not found"})
		return
	}
	writeJSON(w, http.StatusOK, session)
}

func (h *RESTAPIHandler) recoverSession(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.PathValue("sessionID"))
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, apiErrorResponse{Error: "session ID is required"})
		return
	}

	result, err := h.poolServer.GetSessionManager().RecoverSession(sessionID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiErrorResponse{Error: err.Error()})
		return
	}

	code := http.StatusOK
	if !result.Success {
		code = http.StatusInternalServerError
	}
	writeJSON(w, code, result)
}

func (h *RESTAPIHandler) discardSessionStaging(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.PathValue("sessionID"))
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, apiErrorResponse{Error: "session ID is required"})
		return
	}

	result, err := h.poolServer.GetSessionManager().DiscardSessionStaging(sessionID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiErrorResponse{Error: err.Error()})
		return
	}

	code := http.StatusOK
	if !result.Success {
		code = http.StatusInternalServerError
	}
	writeJSON(w, code, result)
}

func snapshotSessionInfo(session *PoolSession) SessionInfo {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	info := SessionInfo{
		SessionSummary:  snapshotSessionSummaryLocked(session),
		Clients:         make([]SessionClientInfo, 0, len(session.connections)),
		OpenFileHandles: make([]SessionFileHandleInfo, 0, len(session.poolFileHandles)),
		StagedFiles:     []SessionStagedFileInfo{},
	}

	for connectionID, client := range session.connections {
		info.Clients = append(info.Clients, SessionClientInfo{
			ConnectionID: connectionID,
			Application:  client.appName,
			Description:  client.description,
		})
	}
	for _, handle := range session.poolFileHandles {
		info.OpenFileHandles = append(info.OpenFileHandles, SessionFileHandleInfo{
			ID:   handle.GetID(),
			Path: handle.GetEntryPath(),
			Mode: string(handle.GetOpenMode()),
		})
	}

	if bufferedClient, ok := session.fsClient.(*irodsfs_common_irods.IRODSFSClientBuffered); ok {
		if stagingFS := bufferedClient.GetStagingFS(); stagingFS != nil {
			for _, metadata := range stagingFS.GetAll() {
				info.StagedFiles = append(info.StagedFiles, SessionStagedFileInfo{
					Path:             metadata.Path,
					OldPath:          metadata.OldPath,
					Action:           metadata.Action.String(),
					FileState:        metadata.FileState.String(),
					LastModifiedTime: metadata.LastModifiedAt,
					SyncFailureCount: metadata.SyncFailCount,
				})
			}
		}
	}

	sort.Slice(info.Clients, func(i, j int) bool { return info.Clients[i].ConnectionID < info.Clients[j].ConnectionID })
	sort.Slice(info.OpenFileHandles, func(i, j int) bool {
		if info.OpenFileHandles[i].Path == info.OpenFileHandles[j].Path {
			return info.OpenFileHandles[i].ID < info.OpenFileHandles[j].ID
		}
		return info.OpenFileHandles[i].Path < info.OpenFileHandles[j].Path
	})
	sort.Slice(info.StagedFiles, func(i, j int) bool { return info.StagedFiles[i].Path < info.StagedFiles[j].Path })
	return info
}

func snapshotSessionSummaryLocked(session *PoolSession) SessionSummary {
	info := SessionSummary{
		ID:                  session.id,
		LastAccessTime:      session.lastAccessTime,
		ClientCount:         len(session.connections),
		OpenFileHandleCount: len(session.poolFileHandles),
		InGracePeriod:       len(session.connections) == 0 && !session.releasing,
		Releasing:           session.releasing,
	}
	if session.irodsAccount != nil {
		info.Host = session.irodsAccount.Host
		info.Port = session.irodsAccount.Port
		info.ClientUser = session.irodsAccount.ClientUser
		info.ClientZone = session.irodsAccount.ClientZone
		info.ProxyUser = session.irodsAccount.ProxyUser
		info.ProxyZone = session.irodsAccount.ProxyZone
	}
	if session.fsClient != nil {
		info.OpenIRODSConnectionCount = session.fsClient.GetOpenConnections()
	}
	return info
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
