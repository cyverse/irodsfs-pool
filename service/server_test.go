package service

import (
	"bytes"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-pool/commons"
	log "github.com/sirupsen/logrus"
)

func TestMonitoringModalClosesOnlyWithCloseButton(t *testing.T) {
	server := &PoolServer{
		sessionManager: &PoolSessionManager{
			sessions: map[string]*PoolSession{},
		},
	}
	handler := NewMonitoringHandler(server, commons.NewDefaultConfig())
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/monitor", nil))

	body := recorder.Body.String()
	if strings.Contains(body, "modal-overlay').addEventListener('click'") {
		t.Fatal("monitoring modal must not close when the overlay is clicked")
	}
	if !strings.Contains(body, `id="modal-close" onclick="closeDetail()"`) {
		t.Fatal("monitoring modal close button is missing")
	}
}

func TestMonitoringSessionDetailIncludesSyncStagingAction(t *testing.T) {
	session := &PoolSession{
		id: "0123456789abcdef",
		irodsAccount: &irodsclient_types.IRODSAccount{
			Host:       "irods.example.org",
			Port:       1247,
			ClientUser: "rods",
			ClientZone: "tempZone",
		},
		connections:     map[string]connInfo{},
		poolFileHandles: map[string]*PoolFileHandle{},
	}
	server := &PoolServer{
		sessionManager: &PoolSessionManager{
			sessions: map[string]*PoolSession{session.id: session},
		},
	}
	handler := NewMonitoringHandler(server, commons.NewDefaultConfig())
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/monitor", nil))

	body := recorder.Body.String()
	for _, expected := range []string{
		`class="action-btn sync-staging-btn"`,
		`Sync Staging`,
		`syncSessionStaging('0123456789abcdef')`,
		`/api/sessions/`,
		`/staging/sync`,
		`class="session-action-result"`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("monitor response does not contain %q", expected)
		}
	}
}

func TestMonitoringClientDescriptionsWrapWithinTheirTable(t *testing.T) {
	session := &PoolSession{
		id: "session-1234",
		irodsAccount: &irodsclient_types.IRODSAccount{
			Host:       "irods.example.org",
			Port:       1247,
			ClientUser: "rods",
			ClientZone: "tempZone",
		},
		connections: map[string]connInfo{
			"connection-1": {appName: "irodsfs", description: strings.Repeat("long-description-", 20)},
		},
		poolFileHandles: map[string]*PoolFileHandle{},
	}
	server := &PoolServer{
		sessionManager: &PoolSessionManager{
			sessions: map[string]*PoolSession{session.id: session},
		},
	}
	handler := NewMonitoringHandler(server, commons.NewDefaultConfig())
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/monitor", nil))

	body := recorder.Body.String()
	for _, expected := range []string{
		`.clients-table { table-layout: fixed; }`,
		`overflow-wrap: anywhere`,
		`<table class="clients-table"><tr><th>Connection ID</th><th>Application</th><th>Description</th></tr>`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("monitor response does not contain %q", expected)
		}
	}
}

func TestMonitoringShowsSessionsPendingRecoveryWithoutCredentials(t *testing.T) {
	manager := newFailedSessionStoreTestManager(t.TempDir())
	manager.handleSessionReleaseResult(newFailedSessionStoreTestSession("session-1"), errors.New("sync failed"))
	t.Cleanup(func() {
		if err := manager.closeFailedSessionStore(); err != nil {
			t.Errorf("closeFailedSessionStore: %v", err)
		}
	})

	server := &PoolServer{sessionManager: manager}
	handler := NewMonitoringHandler(server, commons.NewDefaultConfig())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/monitor", nil))

	body := recorder.Body.String()
	for _, expected := range []string{
		"Sessions Pending Recovery",
		"session-1",
		"account-session-1",
		"rods@tempZone",
		"connection-a",
		"Session Pending Recovery",
		"release_failed",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("monitor response does not contain %q", expected)
		}
	}
	lowerBody := strings.ToLower(body)
	for _, secret := range []string{"secret-password", "secret-ticket", "secret-pam-token"} {
		if strings.Contains(lowerBody, secret) {
			t.Fatalf("monitor response exposes sensitive account data %q", secret)
		}
	}
}

func TestGetSessionAndLoggerUsesSessionLogger(t *testing.T) {
	serverOutput := &bytes.Buffer{}
	serverLogger := log.New()
	serverLogger.SetOutput(serverOutput)

	sessionOutput := &bytes.Buffer{}
	sessionLogger := log.New()
	sessionLogger.SetOutput(sessionOutput)

	session := &PoolSession{
		id:     "session-1",
		logger: sessionLogger.WithField("session_id", "session-1"),
	}
	server := &PoolServer{
		logger: serverLogger.WithFields(log.Fields{}),
		sessionManager: &PoolSessionManager{
			sessions: map[string]*PoolSession{
				session.id: session,
			},
		},
	}

	returnedSession, requestLogger, err := server.getSessionAndLogger(session.id, log.Fields{"path": "/test"})
	if err != nil {
		t.Fatalf("getSessionAndLogger: %v", err)
	}
	if returnedSession != session {
		t.Fatal("getSessionAndLogger returned an unexpected session")
	}

	requestLogger.Info("API request")
	if serverOutput.Len() != 0 {
		t.Fatalf("API log was written to server logger: %q", serverOutput.String())
	}

	logged := sessionOutput.String()
	for _, expected := range []string{"API request", "sessionID=session-1", "path=/test"} {
		if !strings.Contains(logged, expected) {
			t.Fatalf("session log %q does not contain %q", logged, expected)
		}
	}
}

// stubFileHandle is the minimum IRODSFSFileHandle the monitoring page reads, so
// a session can be given an open handle without an iRODS connection.
type stubFileHandle struct {
	id    string
	entry *irodsclient_fs.Entry
	mode  irodsclient_types.FileOpenMode
}

func (h *stubFileHandle) GetID() string                               { return h.id }
func (h *stubFileHandle) GetEntry() *irodsclient_fs.Entry             { return h.entry }
func (h *stubFileHandle) GetOpenMode() irodsclient_types.FileOpenMode { return h.mode }
func (h *stubFileHandle) IsReadMode() bool                            { return h.mode.IsRead() }
func (h *stubFileHandle) IsWriteMode() bool                           { return h.mode.IsWrite() }
func (h *stubFileHandle) GetAvailable(int64) int64                    { return 0 }
func (h *stubFileHandle) ReadAt([]byte, int64) (int, error)           { return 0, nil }
func (h *stubFileHandle) WriteAt([]byte, int64) (int, error)          { return 0, nil }
func (h *stubFileHandle) Truncate(int64) error                        { return nil }
func (h *stubFileHandle) Flush() error                                { return nil }
func (h *stubFileHandle) Close() error                                { return nil }

// A deeply nested iRODS path has no spaces to break on, so an auto-layout table
// grows as wide as the longest path and pushes past the session modal, which is
// capped at 960px. Fixed layout plus break-anywhere keeps it inside.
func TestMonitoringPathTablesWrapWithinTheSessionModal(t *testing.T) {
	longPath := "/tempZone/home/rods/" + strings.Repeat("deeply-nested-directory-name/", 12) + "some-long-file-name.dat"

	handle, err := NewPoolFileHandle("session-1234", &stubFileHandle{
		id:    "handle-1",
		entry: &irodsclient_fs.Entry{Path: longPath},
		mode:  irodsclient_types.FileOpenModeReadOnly,
	})
	if err != nil {
		t.Fatalf("failed to create pool file handle: %v", err)
	}

	session := &PoolSession{
		id: "session-1234",
		irodsAccount: &irodsclient_types.IRODSAccount{
			Host:       "irods.example.org",
			Port:       1247,
			ClientUser: "rods",
			ClientZone: "tempZone",
		},
		connections:     map[string]connInfo{},
		poolFileHandles: map[string]*PoolFileHandle{"handle-1": handle},
	}
	server := &PoolServer{
		sessionManager: &PoolSessionManager{
			sessions: map[string]*PoolSession{session.id: session},
		},
	}

	handler := NewMonitoringHandler(server, commons.NewDefaultConfig())
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/monitor", nil))

	body := recorder.Body.String()
	for _, expected := range []string{
		// Both path-bearing tables in the modal need the fixed layout. The
		// staged files table only renders for a session with a live staging
		// client, which cannot be built from this package, so its markup is
		// not asserted here -- only that its rule ships.
		`.staged-files-table { table-layout: fixed; }`,
		`.staged-files-table th, .staged-files-table td { overflow-wrap: anywhere; word-break: break-word; white-space: normal; }`,
		`.handles-table { table-layout: fixed; }`,
		`.handles-table th, .handles-table td { overflow-wrap: anywhere; word-break: break-word; white-space: normal; }`,
		`<table class="handles-table"><tr><th>Path</th><th>Mode</th></tr>`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("monitor response does not contain %q", expected)
		}
	}

	// The long path is rendered, so the wrapping rules are what keep it in.
	if !strings.Contains(body, longPath) {
		t.Fatalf("monitor response does not contain the long path")
	}

	// A path table that kept the default auto layout would widen past the
	// modal, so none may be emitted without a wrapping class.
	if strings.Contains(body, `<table><tr><th>Path</th>`) {
		t.Fatalf("a path table is rendered without the fixed layout that keeps it inside the modal")
	}
}
