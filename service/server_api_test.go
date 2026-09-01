package service

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_cache "github.com/cyverse/irodsfs-common/irods/cache"
	"github.com/cyverse/irodsfs-pool/commons"
)

func newRESTAPITestHandler(sessions ...*PoolSession) http.Handler {
	sessionMap := make(map[string]*PoolSession, len(sessions))
	for _, session := range sessions {
		sessionMap[session.id] = session
	}

	server := &PoolServer{
		sessionManager: &PoolSessionManager{sessions: sessionMap},
	}
	mux := http.NewServeMux()
	NewRESTAPIHandler(server, nil).RegisterRoutes(mux)
	return mux
}

type fakeSessionFileSystem struct {
	clearCacheCalls int
}

func (fs *fakeSessionFileSystem) ClearCache() {
	fs.clearCacheCalls++
}

func (fs *fakeSessionFileSystem) Release() {}

type fakeSyncIRODSFSClient struct {
	irodsfs_common_irods.IRODSFSClient
	syncCalls int
	syncErr   error
}

func newRESTAPITestCacheManager(t *testing.T) *irodsfs_common_cache.MemoryCacheManager {
	t.Helper()
	cacheManager, err := irodsfs_common_cache.NewMemoryCacheManager(&irodsfs_common_cache.MemoryCacheConfig{
		NumCounters: 1000,
		MaxCost:     1024 * 1024,
		BufferItems: 64,
		TTL:         time.Hour,
	})
	if err != nil {
		t.Fatalf("NewMemoryCacheManager: %v", err)
	}
	t.Cleanup(cacheManager.Release)
	return cacheManager
}

func (client *fakeSyncIRODSFSClient) Sync() error {
	client.syncCalls++
	return client.syncErr
}

func TestRESTAPIGetSystemInfo(t *testing.T) {
	config := commons.NewDefaultConfig()
	config.ServiceEndpoint = "tcp://127.0.0.1:12020"
	config.DataRootPath = t.TempDir()
	config.StagingRootPath = t.TempDir()
	config.MaxStagingDataSize = 123456
	config.ManagementServicePort = 12021

	server := &PoolServer{
		sessionManager: &PoolSessionManager{sessions: map[string]*PoolSession{}},
		accumulatedMetrics: AccumulatedMetrics{
			BytesSent:          100,
			BytesReceived:      200,
			CacheHit:           3,
			CacheMiss:          1,
			RequestFailures:    4,
			ConnectionFailures: 5,
		},
	}
	handler := newRESTAPIHandler(server, config, time.Now().Add(-2*time.Minute))
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/sysinfo", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type = %q", contentType)
	}

	var info SystemInfo
	if err := json.Unmarshal(recorder.Body.Bytes(), &info); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if info.Server.Endpoint != config.ServiceEndpoint || info.Server.DataRootPath != config.DataRootPath {
		t.Fatalf("unexpected server info: %#v", info.Server)
	}
	if info.Server.UptimeSeconds < 120 || info.Server.RESTAPIEndpoint != ":12021/api" {
		t.Fatalf("unexpected HTTP service info: %#v", info.Server)
	}
	if info.Server.Version.GoVersion == "" || info.Server.Version.Platform == "" {
		t.Fatalf("version runtime information is missing: %#v", info.Server.Version)
	}
	if info.Staging.Path != config.StagingRootPath || info.Staging.MaximumBytes != config.MaxStagingDataSize {
		t.Fatalf("unexpected staging info: %#v", info.Staging)
	}
	if info.IOMetrics.BytesSent != 100 || info.IOMetrics.BytesReceived != 200 || info.IOMetrics.CacheHitRatioPercent != 75 {
		t.Fatalf("unexpected I/O metrics: %#v", info.IOMetrics)
	}
	if info.IOMetrics.RequestFailures != 4 || info.IOMetrics.ConnectionFailures != 5 {
		t.Fatalf("unexpected failure metrics: %#v", info.IOMetrics)
	}
}

func TestRESTAPIHealth(t *testing.T) {
	mux := http.NewServeMux()
	NewRESTAPIHandler(nil, nil).RegisterRoutes(mux)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var status HealthStatus
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if status.Status != "ok" {
		t.Fatalf("health status = %q, want %q", status.Status, "ok")
	}
}

func TestRESTAPIReadiness(t *testing.T) {
	tests := []struct {
		name       string
		poolServer *PoolServer
		wantCode   int
		wantStatus string
	}{
		{
			name:       "ready",
			poolServer: &PoolServer{sessionManager: &PoolSessionManager{cacheManager: newRESTAPITestCacheManager(t)}},
			wantCode:   http.StatusOK,
			wantStatus: "ready",
		},
		{
			name:       "missing pool server",
			poolServer: nil,
			wantCode:   http.StatusServiceUnavailable,
			wantStatus: "not_ready",
		},
		{
			name:       "missing session manager",
			poolServer: &PoolServer{},
			wantCode:   http.StatusServiceUnavailable,
			wantStatus: "not_ready",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			NewRESTAPIHandler(tt.poolServer, nil).RegisterRoutes(mux)

			recorder := httptest.NewRecorder()
			mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))

			if recorder.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantCode)
			}
			var status HealthStatus
			if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if status.Status != tt.wantStatus {
				t.Fatalf("readiness status = %q, want %q", status.Status, tt.wantStatus)
			}
		})
	}
}

func TestRESTAPIListSessions(t *testing.T) {
	lastAccess := time.Date(2026, time.August, 28, 12, 30, 0, 0, time.UTC)
	handler := newRESTAPITestHandler(
		&PoolSession{
			id: "session-b",
			irodsAccount: &irodsclient_types.IRODSAccount{
				Host:       "irods.example.org",
				Port:       1247,
				ClientUser: "rods",
				ClientZone: "tempZone",
				Password:   "must-not-be-exposed",
				Ticket:     "must-not-be-exposed",
				PAMToken:   "must-not-be-exposed",
			},
			connections: map[string]connInfo{
				"connection-1": {appName: "irodsfs", description: "mount one"},
			},
			lastAccessTime:  lastAccess,
			poolFileHandles: map[string]*PoolFileHandle{},
		},
		&PoolSession{
			id:              "session-a",
			irodsAccount:    &irodsclient_types.IRODSAccount{},
			connections:     map[string]connInfo{},
			lastAccessTime:  lastAccess.Add(-time.Minute),
			poolFileHandles: map[string]*PoolFileHandle{},
		},
	)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/sessions", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type = %q", contentType)
	}

	var sessions []SessionInfo
	if err := json.Unmarshal(recorder.Body.Bytes(), &sessions); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(sessions) != 2 {
		t.Fatalf("session count = %d, want 2", len(sessions))
	}
	if sessions[0].ID != "session-a" || sessions[1].ID != "session-b" {
		t.Fatalf("sessions are not sorted by ID: %#v", sessions)
	}
	if sessions[1].ClientCount != 1 || sessions[1].ClientUser != "rods" || sessions[1].ClientZone != "tempZone" {
		t.Fatalf("unexpected session info: %#v", sessions[1])
	}
	if !sessions[0].InGracePeriod || sessions[1].InGracePeriod {
		t.Fatalf("unexpected grace-period states: %#v", sessions)
	}
	if len(sessions[1].Clients) != 1 || sessions[1].Clients[0].ConnectionID != "connection-1" {
		t.Fatalf("session list does not include client details: %#v", sessions[1].Clients)
	}
	if sessions[0].OpenFileHandles == nil || sessions[0].StagedFiles == nil {
		t.Fatalf("session list must include detail collections: %#v", sessions[0])
	}

	body := recorder.Body.String()
	for _, secret := range []string{"must-not-be-exposed", "password", "ticket", "pam_token"} {
		if strings.Contains(strings.ToLower(body), secret) {
			t.Fatalf("response exposes sensitive account data %q: %s", secret, body)
		}
	}
}

func TestRESTAPIGetSession(t *testing.T) {
	lastAccess := time.Date(2026, time.August, 28, 12, 30, 0, 0, time.UTC)
	session := &PoolSession{
		id:           "session-1",
		irodsAccount: &irodsclient_types.IRODSAccount{Host: "irods.example.org", Port: 1247, ClientUser: "rods", ClientZone: "tempZone"},
		connections: map[string]connInfo{
			"connection-b": {appName: "irodsfs", description: "second mount"},
			"connection-a": {appName: "irodsfs", description: "first mount"},
		},
		lastAccessTime:  lastAccess,
		poolFileHandles: map[string]*PoolFileHandle{},
	}
	handler := newRESTAPITestHandler(session)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/sessions/session-1", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var info SessionInfo
	if err := json.Unmarshal(recorder.Body.Bytes(), &info); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if info.ID != session.id || info.LastAccessTime != lastAccess {
		t.Fatalf("unexpected session info: %#v", info)
	}
	if len(info.Clients) != 2 || info.Clients[0].ConnectionID != "connection-a" || info.Clients[1].ConnectionID != "connection-b" {
		t.Fatalf("clients are not sorted by connection ID: %#v", info.Clients)
	}
	if info.OpenFileHandles == nil || info.StagedFiles == nil {
		t.Fatalf("empty detail collections must be JSON arrays: %#v", info)
	}
}

func TestRESTAPIGetSessionNotFound(t *testing.T) {
	handler := newRESTAPITestHandler()
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/sessions/missing", nil))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
	var response apiErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Error != "session not found" {
		t.Fatalf("error = %q", response.Error)
	}
}

func TestRESTAPIInvalidateSessionMetadataCache(t *testing.T) {
	fs := &fakeSessionFileSystem{}
	handler := newRESTAPITestHandler(&PoolSession{
		id: "session-1",
		fs: fs,
	})
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/sessions/session-1/metadata-cache/invalidate", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if fs.clearCacheCalls != 1 {
		t.Fatalf("ClearCache calls = %d, want 1", fs.clearCacheCalls)
	}

	var result MetadataCacheInvalidationResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.SessionID != "session-1" || !result.Success {
		t.Fatalf("unexpected invalidation result: %#v", result)
	}
}

func TestRESTAPIInvalidateSessionMetadataCacheNotFound(t *testing.T) {
	handler := newRESTAPITestHandler()
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/sessions/missing/metadata-cache/invalidate", nil))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestRESTAPIInvalidateSessionMetadataCacheWhileReleasing(t *testing.T) {
	fs := &fakeSessionFileSystem{}
	handler := newRESTAPITestHandler(&PoolSession{
		id:        "session-1",
		fs:        fs,
		releasing: true,
	})
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/sessions/session-1/metadata-cache/invalidate", nil))

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	if fs.clearCacheCalls != 0 {
		t.Fatalf("ClearCache calls = %d, want 0", fs.clearCacheCalls)
	}
}

func TestRESTAPISyncSessionStaging(t *testing.T) {
	client := &fakeSyncIRODSFSClient{}
	handler := newRESTAPITestHandler(&PoolSession{
		id:       "session-1",
		fsClient: client,
	})
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/sessions/session-1/staging/sync", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if client.syncCalls != 1 {
		t.Fatalf("Sync calls = %d, want 1", client.syncCalls)
	}

	var result SessionStagingSyncResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.SessionID != "session-1" || !result.Success {
		t.Fatalf("unexpected sync result: %#v", result)
	}
}

func TestRESTAPISyncSessionStagingNotFound(t *testing.T) {
	handler := newRESTAPITestHandler()
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/sessions/missing/staging/sync", nil))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestRESTAPISyncSessionStagingWhileReleasing(t *testing.T) {
	client := &fakeSyncIRODSFSClient{}
	handler := newRESTAPITestHandler(&PoolSession{
		id:        "session-1",
		fsClient:  client,
		releasing: true,
	})
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/sessions/session-1/staging/sync", nil))

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	if client.syncCalls != 0 {
		t.Fatalf("Sync calls = %d, want 0", client.syncCalls)
	}
}

func TestRESTAPISyncSessionStagingFailure(t *testing.T) {
	client := &fakeSyncIRODSFSClient{syncErr: errors.New("sync failed")}
	handler := newRESTAPITestHandler(&PoolSession{
		id:       "session-1",
		fsClient: client,
	})
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/sessions/session-1/staging/sync", nil))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
	if client.syncCalls != 1 {
		t.Fatalf("Sync calls = %d, want 1", client.syncCalls)
	}
}

func TestRESTAPIFailedSessions(t *testing.T) {
	manager := newFailedSessionStoreTestManager(t.TempDir())
	manager.handleSessionReleaseResult(newFailedSessionStoreTestSession("session-1"), errors.New("sync failed"))
	t.Cleanup(func() {
		if err := manager.closeFailedSessionStore(); err != nil {
			t.Errorf("closeFailedSessionStore: %v", err)
		}
	})

	server := &PoolServer{sessionManager: manager}
	mux := http.NewServeMux()
	NewRESTAPIHandler(server, nil).RegisterRoutes(mux)

	listRecorder := httptest.NewRecorder()
	mux.ServeHTTP(listRecorder, httptest.NewRequest(http.MethodGet, "/api/recovery-sessions", nil))
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d", listRecorder.Code, http.StatusOK)
	}
	var sessions []FailedSessionInfo
	if err := json.Unmarshal(listRecorder.Body.Bytes(), &sessions); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(sessions) != 1 || sessions[0].ID != "session-1" || sessions[0].AccountKey != "account-session-1" {
		t.Fatalf("unexpected failed sessions: %#v", sessions)
	}
	if sessions[0].Status != FailedSessionStatusReleaseFailed {
		t.Fatalf("status = %q, want %q", sessions[0].Status, FailedSessionStatusReleaseFailed)
	}
	if len(sessions[0].Connections) != 2 || sessions[0].IRODSAccount.ClientUser != "rods" {
		t.Fatalf("failed session metadata is incomplete: %#v", sessions[0])
	}

	detailRecorder := httptest.NewRecorder()
	mux.ServeHTTP(detailRecorder, httptest.NewRequest(http.MethodGet, "/api/recovery-sessions/session-1", nil))
	if detailRecorder.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d", detailRecorder.Code, http.StatusOK)
	}
	var detail FailedSessionInfo
	if err := json.Unmarshal(detailRecorder.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode detail response: %v", err)
	}
	if detail.ID != "session-1" || detail.IRODSAccount.Host != "irods.example.org" {
		t.Fatalf("unexpected failed session detail: %#v", detail)
	}

	missingRecorder := httptest.NewRecorder()
	mux.ServeHTTP(missingRecorder, httptest.NewRequest(http.MethodGet, "/api/recovery-sessions/missing", nil))
	if missingRecorder.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d, want %d", missingRecorder.Code, http.StatusNotFound)
	}

	combinedBody := strings.ToLower(listRecorder.Body.String() + detailRecorder.Body.String())
	for _, secret := range []string{"secret-password", "secret-ticket", "secret-pam-token", `"password"`, `"ticket"`, `"pam_token"`} {
		if strings.Contains(combinedBody, secret) {
			t.Fatalf("failed-session API exposes sensitive account data %q: %s", secret, combinedBody)
		}
	}
}

func TestRESTAPIRejectsUnsupportedMethod(t *testing.T) {
	handler := newRESTAPITestHandler()
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/sessions", nil))

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusMethodNotAllowed)
	}
}
