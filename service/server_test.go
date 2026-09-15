package service

import (
	"bytes"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_packedfs "github.com/cyverse/irodsfs-common/irods/packedfs"
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

// A packed directory's contents are held as a local tree and sent as one
// archive, so its files carry no staging metadata and never appear under Staged
// Files. The modal reports the directory and the data object it becomes
// instead, which is what says whether it has reached iRODS.
func TestMonitoringSessionDetailReportsPackedDirectories(t *testing.T) {
	session := &PoolSession{
		id: "session-1234",
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
		`<h3>Packed Directories (0)</h3>`,
		`No packed directories.`,
		// Both paths in this table are long, so it needs the same fixed layout
		// that keeps the other path tables inside the modal.
		`.packed-dirs-table { table-layout: fixed; }`,
		`.packed-dirs-table th, .packed-dirs-table td { overflow-wrap: anywhere; word-break: break-word; white-space: normal; }`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("monitor response does not contain %q", expected)
		}
	}
}

func TestRenderPackedDirectoriesShowsTheDirectoryAndItsArchive(t *testing.T) {
	packedAt := time.Date(2026, 9, 15, 21, 35, 15, 0, time.UTC)
	entries := []irodsfs_common_packedfs.Status{
		{
			Root:         "/iplant/home/iychoi/irods-csi-driver/.git",
			ArchivePath:  "/iplant/home/iychoi/irods-csi-driver/.git.mount.tar",
			LocalPath:    "/irodsfs_pool/staging/session-1234-packed/iplant/home/iychoi/irods-csi-driver/.git",
			State:        "MOUNTED",
			Dirty:        true,
			SizeBytes:    3 * 1024 * 1024,
			LastPackedAt: packedAt,
		},
		{
			Root:        "/iplant/home/iychoi/proj/.venv",
			ArchivePath: "/iplant/home/iychoi/proj/.venv.mount.tar",
			State:       "MOUNTED",
			Dirty:       false,
			SizeBytes:   512 * 1024 * 1024,
		},
	}

	var buffer bytes.Buffer
	renderPackedDirectories(&buffer, entries)
	body := buffer.String()

	for _, expected := range []string{
		`<h3>Packed Directories (2)</h3>`,
		// The directory a user sees, and the data object iRODS actually holds.
		`/iplant/home/iychoi/irods-csi-driver/.git`,
		`/iplant/home/iychoi/irods-csi-driver/.git.mount.tar`,
		`/iplant/home/iychoi/proj/.venv.mount.tar`,
		// An unsent directory is called out; one already uploaded is not.
		`class="dirty">pending`,
		`class="cached">synced`,
		`21:35:15`,
		`never`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("packed directory table does not contain %q", expected)
		}
	}

	// The files inside a packed directory must not be enumerated: that is the
	// point of reporting the archive instead.
	if strings.Contains(body, "<th>Action</th>") || strings.Contains(body, "<th>Sync Status</th>") {
		t.Fatalf("packed directories are listed as individual staged files")
	}
}

func TestRenderPackedDirectoriesShowsAMountFailure(t *testing.T) {
	var buffer bytes.Buffer
	renderPackedDirectories(&buffer, []irodsfs_common_packedfs.Status{{
		Root:        "/iplant/home/iychoi/proj/.venv",
		ArchivePath: "/iplant/home/iychoi/proj/.venv.mount.tar",
		State:       "FAILED",
		Error:       "packed directory exceeds the configured size limit",
	}})

	body := buffer.String()
	for _, expected := range []string{"FAILED", "exceeds the configured size limit"} {
		if !strings.Contains(body, expected) {
			t.Fatalf("packed directory table does not contain %q", expected)
		}
	}
}

// Paths and error text reach this table from iRODS, so they are data, not
// markup.
func TestRenderPackedDirectoriesEscapesItsContent(t *testing.T) {
	var buffer bytes.Buffer
	renderPackedDirectories(&buffer, []irodsfs_common_packedfs.Status{{
		Root:        `/z/home/u/<img src=x onerror=alert(1)>/.venv`,
		ArchivePath: `/z/home/u/<img src=x onerror=alert(1)>/.venv.mount.tar`,
		State:       "MOUNTED",
		Error:       `<script>alert(2)</script>`,
	}})

	body := buffer.String()
	for _, injected := range []string{"<img src=x", "<script>alert(2)</script>"} {
		if strings.Contains(body, injected) {
			t.Fatalf("packed directory table renders %q as markup", injected)
		}
	}
	if !strings.Contains(body, "&lt;img src=x") {
		t.Fatalf("packed directory table does not escape the path")
	}
}

// Paths, client application names and descriptions all reach the monitoring
// page from outside: a user names the files, and a client sends whatever it
// likes at login. The page is served to operators, so all of it is data.
func TestMonitoringEscapesSessionContent(t *testing.T) {
	const injectedPath = `/tempZone/home/rods/<img src=x onerror=alert(1)>.dat`
	const injectedApp = `<script>alert('app')</script>`
	const injectedDesc = `"><script>alert('desc')</script>`

	handle, err := NewPoolFileHandle("session-1234", &stubFileHandle{
		id:    "handle-1",
		entry: &irodsclient_fs.Entry{Path: injectedPath},
		mode:  irodsclient_types.FileOpenModeReadOnly,
	})
	if err != nil {
		t.Fatalf("failed to create pool file handle: %v", err)
	}

	session := &PoolSession{
		id: "session-1234",
		irodsAccount: &irodsclient_types.IRODSAccount{
			Host:       `<b>irods.example.org</b>`,
			Port:       1247,
			ClientUser: `<i>rods</i>`,
			ClientZone: "tempZone",
		},
		connections: map[string]connInfo{
			"conn-<1>": {appName: injectedApp, description: injectedDesc},
		},
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
	for _, injected := range []string{
		`<img src=x onerror=alert(1)>`,
		`<script>alert('app')</script>`,
		`"><script>alert('desc')</script>`,
		`<b>irods.example.org</b>`,
		`<i>rods</i>`,
	} {
		if strings.Contains(body, injected) {
			t.Fatalf("monitor response renders %q as markup", injected)
		}
	}

	// The content is still shown, just escaped.
	if !strings.Contains(body, `&lt;img src=x onerror=alert(1)&gt;`) {
		t.Fatalf("monitor response does not show the escaped path")
	}
	if !strings.Contains(body, `&lt;script&gt;alert(&#39;app&#39;)&lt;/script&gt;`) {
		t.Fatalf("monitor response does not show the escaped application name")
	}
}

func TestRenderStagedFilesEscapesItsContent(t *testing.T) {
	var buffer bytes.Buffer
	renderStagedFiles(&buffer, []stagedFileEntry{{
		path:      `/z/home/u/<img src=x onerror=alert(1)>.dat`,
		oldPath:   `/z/home/u/<svg onload=alert(2)>.dat`,
		action:    "RENAME",
		fileState: "DIRTY",
		modified:  time.Date(2026, 9, 15, 21, 35, 15, 0, time.UTC),
	}})

	body := buffer.String()
	for _, injected := range []string{`<img src=x`, `<svg onload=alert(2)>`} {
		if strings.Contains(body, injected) {
			t.Fatalf("staged files table renders %q as markup", injected)
		}
	}

	// The row is still rendered, with its rename arrow and dirty marker intact.
	for _, expected := range []string{`&lt;img src=x`, `&lt;svg onload=alert(2)&gt;`, `class="dirty"`, `←`} {
		if !strings.Contains(body, expected) {
			t.Fatalf("staged files table does not contain %q", expected)
		}
	}
}

func TestRenderStagedFilesEmptyState(t *testing.T) {
	var buffer bytes.Buffer
	renderStagedFiles(&buffer, nil)

	body := buffer.String()
	if !strings.Contains(body, `<h3>Staged Files (0)</h3>`) || !strings.Contains(body, `No staged files.`) {
		t.Fatalf("staged files table does not render its empty state: %s", body)
	}
}

// A session's staging upload runs long after its last client is gone. It used
// to be dropped from the session map the moment the release started, so it
// vanished from the monitoring page while minutes of uploading were still
// ahead of it, with nothing anywhere showing that work was in flight.
func TestMonitoringKeepsReleasingSessionsVisible(t *testing.T) {
	session := &PoolSession{
		id: "session-releasing",
		irodsAccount: &irodsclient_types.IRODSAccount{
			Host:       "irods.example.org",
			Port:       1247,
			ClientUser: "rods",
			ClientZone: "tempZone",
		},
		connections:     map[string]connInfo{},
		poolFileHandles: map[string]*PoolFileHandle{},
		releasing:       true,
	}

	manager := &PoolSessionManager{
		sessions:          map[string]*PoolSession{},
		releasingSessions: map[string]*PoolSession{session.id: session},
	}
	server := &PoolServer{sessionManager: manager}

	if got := manager.GetTotalSessions(); got != 1 {
		t.Fatalf("GetTotalSessions() = %d, want 1: a releasing session still holds its resources", got)
	}

	if _, err := manager.GetSession(session.id); err != nil {
		t.Fatalf("GetSession() on a releasing session: %v", err)
	}

	sessions := manager.GetAllSessions()
	if len(sessions) != 1 || sessions[0].id != session.id {
		t.Fatalf("GetAllSessions() = %v, want the releasing session", sessions)
	}

	handler := NewMonitoringHandler(server, commons.NewDefaultConfig())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/monitor", nil))

	body := recorder.Body.String()
	if !strings.Contains(body, session.id) {
		t.Fatalf("monitor response does not list the releasing session")
	}
	// It is past the grace period, so calling it that would be wrong.
	if !strings.Contains(body, "releasing — syncing to iRODS") {
		t.Fatalf("monitor response does not mark the session as releasing")
	}
}

// Once the release finishes the session must disappear, or the page would grow
// a permanent row for every session the server ever had.
func TestReleasedSessionsAreDropped(t *testing.T) {
	session := &PoolSession{id: "session-done", connections: map[string]connInfo{}}
	manager := &PoolSessionManager{
		sessions:          map[string]*PoolSession{},
		releasingSessions: map[string]*PoolSession{session.id: session},
	}

	manager.finishAsyncRelease(session.id)

	if got := manager.GetTotalSessions(); got != 0 {
		t.Fatalf("GetTotalSessions() = %d, want 0 after the release finished", got)
	}
	if _, err := manager.GetSession(session.id); err == nil {
		t.Fatalf("GetSession() still finds a session whose release has finished")
	}
	if sessions := manager.GetAllSessions(); len(sessions) != 0 {
		t.Fatalf("GetAllSessions() = %v, want none", sessions)
	}
}

func TestBeginAsyncReleaseMovesTheSessionOutOfReuse(t *testing.T) {
	session := &PoolSession{id: "session-1234", connections: map[string]connInfo{}}
	manager := &PoolSessionManager{
		sessions:          map[string]*PoolSession{session.id: session},
		releasingSessions: map[string]*PoolSession{},
	}

	manager.mutex.Lock()
	manager.beginAsyncReleaseUnlocked(session)
	manager.mutex.Unlock()

	// A new login must not pick up a session that is being torn down.
	manager.mutex.RLock()
	_, reusable := manager.sessions[session.id]
	manager.mutex.RUnlock()
	if reusable {
		t.Fatalf("a releasing session is still offered for reuse")
	}

	// It stays reportable though.
	if sessions := manager.GetAllSessions(); len(sessions) != 1 {
		t.Fatalf("GetAllSessions() = %v, want the releasing session", sessions)
	}
}
