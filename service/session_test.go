package service

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-pool/service/api"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

func TestNewSessionLoggerWritesOnlyToSessionFile(t *testing.T) {
	standardLogger := log.StandardLogger()
	originalOutput := standardLogger.Out
	defer standardLogger.SetOutput(originalOutput)

	standardOutput := &bytes.Buffer{}
	standardLogger.SetOutput(standardOutput)

	logRootPath := t.TempDir()
	sessionID := "test-session"
	logger, logFile, err := newSessionLogger(logRootPath, sessionID, 42)
	if err != nil {
		t.Fatalf("newSessionLogger: %v", err)
	}

	logWriter, ok := logFile.(*lumberjack.Logger)
	if !ok {
		t.Fatalf("session log writer type = %T, want *lumberjack.Logger", logFile)
	}
	if logWriter.MaxSize != 10 || logWriter.MaxBackups != 42 || logWriter.MaxAge != 30 {
		t.Fatalf("session log rotation = size:%d backups:%d age:%d, want 10/42/30", logWriter.MaxSize, logWriter.MaxBackups, logWriter.MaxAge)
	}

	logger.Info("session-only message")
	if err := logFile.Close(); err != nil {
		t.Fatalf("close session log: %v", err)
	}

	if standardOutput.Len() != 0 {
		t.Fatalf("session log was also written to standard logger: %q", standardOutput.String())
	}

	logData, err := os.ReadFile(filepath.Join(logRootPath, "session_logs", sessionID+".log"))
	if err != nil {
		t.Fatalf("read session log: %v", err)
	}

	logText := string(logData)
	if !strings.Contains(logText, "session-only message") {
		t.Fatalf("session log does not contain message: %q", logText)
	}
	if !strings.Contains(logText, "session_id=test-session") {
		t.Fatalf("session log does not contain session ID: %q", logText)
	}
}

// newReleasingTestSession builds a session parked in the releasing map, the way
// a logout leaves one while its staged data is still going to iRODS.
func newReleasingTestSession(t *testing.T, sessionID string) (*PoolSessionManager, *PoolSession) {
	t.Helper()

	session := &PoolSession{
		id:              sessionID,
		accountKey:      sessionID,
		connections:     map[string]connInfo{},
		poolFileHandles: map[string]*PoolFileHandle{},
		logger:          log.NewEntry(log.StandardLogger()),
	}
	manager := &PoolSessionManager{
		sessions:          map[string]*PoolSession{},
		releasingSessions: map[string]*PoolSession{sessionID: session},
		pendingReleases:   map[string]*time.Timer{},
		logger:            log.NewEntry(log.StandardLogger()),
	}

	manager.mutex.Lock()
	epoch := markSessionReleasing(session, true)
	manager.mutex.Unlock()
	if epoch == 0 {
		t.Fatal("markSessionReleasing did not start a release epoch")
	}
	return manager, session
}

// A release spends most of its time flushing staged data, and the session works
// throughout. Handing it back to a new login is what keeps a remount from
// waiting for that flush, and from colliding with it over the staging area.
func TestTakingOverAReleasingSessionPutsItBackInService(t *testing.T) {
	manager, session := newReleasingTestSession(t, "session-adopted")

	session.mutex.RLock()
	releaseDone := session.releaseDone
	epoch := session.releaseEpoch
	session.mutex.RUnlock()

	manager.mutex.Lock()
	adopted := manager.adoptReleasingSessionUnlocked(session)
	manager.mutex.Unlock()
	if !adopted {
		t.Fatal("a session that is only flushing was not taken over")
	}

	manager.mutex.RLock()
	_, live := manager.sessions[session.id]
	_, stillReleasing := manager.releasingSessions[session.id]
	manager.mutex.RUnlock()
	if !live || stillReleasing {
		t.Fatalf("session is live=%v, releasing=%v, want live", live, stillReleasing)
	}

	session.mutex.RLock()
	releasing := session.releasing
	session.mutex.RUnlock()
	if releasing {
		t.Fatal("the session is still marked as releasing after being taken over")
	}

	select {
	case <-releaseDone:
	default:
		t.Fatal("waiters were not woken when the release was taken over")
	}

	// The goroutine that was running that release must now leave the session
	// alone instead of tearing down a session that is back in use.
	if manager.beginSessionTeardown(session, epoch) {
		t.Fatal("the cancelled release still claimed the session for teardown")
	}
}

// A session can be taken over and released again while the first release is
// still flushing. Only the release that owns the session may tear it down, or
// the two teardowns would close the same channel twice.
func TestOnlyTheCurrentReleaseTearsTheSessionDown(t *testing.T) {
	manager, session := newReleasingTestSession(t, "session-rereleased")

	session.mutex.RLock()
	firstEpoch := session.releaseEpoch
	session.mutex.RUnlock()

	manager.mutex.Lock()
	if !manager.adoptReleasingSessionUnlocked(session) {
		manager.mutex.Unlock()
		t.Fatal("a session that is only flushing was not taken over")
	}
	// The client left again, so the session is released a second time.
	manager.beginAsyncReleaseUnlocked(session)
	secondEpoch := markSessionReleasing(session, true)
	manager.mutex.Unlock()

	if manager.beginSessionTeardown(session, firstEpoch) {
		t.Fatal("the first release tore down a session that a later release owns")
	}
	if !manager.beginSessionTeardown(session, secondEpoch) {
		t.Fatal("the current release could not claim the session for teardown")
	}

	// Once the teardown starts the session is gone for good, so a login has to
	// wait for it instead of taking it over.
	manager.mutex.Lock()
	adopted := manager.adoptReleasingSessionUnlocked(session)
	manager.mutex.Unlock()
	if adopted {
		t.Fatal("a session whose teardown has started was taken over")
	}
}

func TestNewSessionTakesOverAReleasingSession(t *testing.T) {
	account := &api.Account{
		IrodsHost:     "irods.example.org",
		IrodsPort:     1247,
		IrodsUserName: "rods",
		IrodsZoneName: "tempZone",
	}
	accountKey := makeAccountKey(convertAccountFromAPIToIRODS(account))

	manager, session := newReleasingTestSession(t, accountKey)

	reused, err := manager.NewSession(account, "test")
	if err != nil {
		t.Fatalf("NewSession on a releasing session: %v", err)
	}
	if reused != session {
		t.Fatalf("NewSession returned %v, want the session that was being released", reused)
	}

	manager.mutex.RLock()
	_, live := manager.sessions[accountKey]
	manager.mutex.RUnlock()
	if !live {
		t.Fatal("the taken over session is not offered for reuse")
	}
}

// blockingSyncIRODSFSClient holds a staging flush open, so a test can act while
// a release is in the part of its work that leaves the session usable.
type blockingSyncIRODSFSClient struct {
	irodsfs_common_irods.IRODSFSClient
	started chan struct{}
	finish  chan struct{}
}

func (client *blockingSyncIRODSFSClient) Sync() error {
	close(client.started)
	<-client.finish
	return nil
}

func (client *blockingSyncIRODSFSClient) Release() error { return nil }

// The flush is the part of a release a login can take the session over from, so
// the goroutine running it has to check before it closes anything.
func TestAReleaseTakenOverLeavesTheSessionIntact(t *testing.T) {
	manager, session := newReleasingTestSession(t, "session-flushing")

	client := &blockingSyncIRODSFSClient{started: make(chan struct{}), finish: make(chan struct{})}
	session.fsClient = client
	session.fs = &fakeSessionFileSystem{}

	session.mutex.RLock()
	epoch := session.releaseEpoch
	session.mutex.RUnlock()

	manager.releaseSessionAsync(session, epoch, "test")
	<-client.started

	manager.mutex.Lock()
	adopted := manager.adoptReleasingSessionUnlocked(session)
	manager.mutex.Unlock()
	if !adopted {
		close(client.finish)
		manager.releaseWg.Wait()
		t.Fatal("a session flushing its staged data was not taken over")
	}

	close(client.finish)
	manager.releaseWg.Wait()

	session.mutex.RLock()
	defer session.mutex.RUnlock()
	if session.fsClient == nil || session.fs == nil {
		t.Fatal("the release tore down a session that a new login had taken over")
	}
	if session.releasing {
		t.Fatal("the taken over session is still marked as releasing")
	}
}

// countingCloseFileHandle records that the session closed it.
type countingCloseFileHandle struct {
	stubFileHandle
	closed int
}

func (h *countingCloseFileHandle) Close() error {
	h.closed++
	return nil
}

// drainRecordingIRODSFSClient reports how many file handles the session still
// held when its staged data was flushed.
type drainRecordingIRODSFSClient struct {
	irodsfs_common_irods.IRODSFSClient
	session        *PoolSession
	syncCalls      int
	handlesAtSync  int
	drainCalls     int
	handlesAtDrain int
	releaseCalls   int
	// onDrain runs inside Drain, for a test that needs something to happen
	// while the session's staged data is going out.
	onDrain func()
}

func (client *drainRecordingIRODSFSClient) Sync() error {
	client.session.mutex.RLock()
	client.handlesAtSync = len(client.session.poolFileHandles)
	client.session.mutex.RUnlock()

	client.syncCalls++
	return nil
}

func (client *drainRecordingIRODSFSClient) Drain() error {
	client.session.mutex.RLock()
	client.handlesAtDrain = len(client.session.poolFileHandles)
	client.session.mutex.RUnlock()

	client.drainCalls++
	if client.onDrain != nil {
		client.onDrain()
	}
	return nil
}

func (client *drainRecordingIRODSFSClient) Release() error {
	client.releaseCalls++
	return nil
}

// Staging refuses to sync at all while a write handle is open, so a flush that
// runs with the handles of a departed client still open uploads nothing and
// leaves the work to the teardown, where a returning client can no longer be
// served.
func TestDrainClosesFileHandlesBeforeFlushingStagedData(t *testing.T) {
	manager, session := newReleasingTestSession(t, "session-drain")

	fileHandle := &countingCloseFileHandle{stubFileHandle: stubFileHandle{
		id:    "handle-1",
		entry: &irodsclient_fs.Entry{Path: "/tempZone/home/rods/staged.dat"},
	}}
	poolFileHandle, err := NewPoolFileHandle(session.id, "", fileHandle, irodsfs_common_irods.NewFileLockManager())
	if err != nil {
		t.Fatalf("NewPoolFileHandle: %v", err)
	}
	session.AddPoolFileHandle(poolFileHandle)

	client := &drainRecordingIRODSFSClient{session: session}
	session.fsClient = client
	session.fs = &fakeSessionFileSystem{}

	session.mutex.RLock()
	epoch := session.releaseEpoch
	session.mutex.RUnlock()

	manager.drainSessionForRelease(session, epoch)

	if fileHandle.closed != 1 {
		t.Fatalf("file handle closed %d times, want once", fileHandle.closed)
	}
	if client.drainCalls != 1 || client.syncCalls != 1 {
		t.Fatalf("staged data drained %d times and flushed %d times, want once each", client.drainCalls, client.syncCalls)
	}
	if client.handlesAtDrain != 0 || client.handlesAtSync != 0 {
		t.Fatalf("staged data was uploaded while file handles were still open (drain %d, flush %d)",
			client.handlesAtDrain, client.handlesAtSync)
	}

	// The drain only moves data. The session stays whole, so a login arriving
	// now still takes it over.
	session.mutex.RLock()
	defer session.mutex.RUnlock()
	if session.fsClient == nil || session.fs == nil {
		t.Fatal("the drain tore the session down")
	}
	if client.releaseCalls != 0 {
		t.Fatalf("the drain released the filesystem client %d times", client.releaseCalls)
	}
}

// The flush holds off the write handles of whoever is using the session, so a
// login that arrives while the staged data is going out must not end up waiting
// for it. What the drain could not move stays for the background sync.
func TestDrainSkipsTheFlushWhenTheSessionIsTakenOverWhileItRuns(t *testing.T) {
	manager, session := newReleasingTestSession(t, "session-drain-adopted")

	client := &drainRecordingIRODSFSClient{session: session}
	client.onDrain = func() {
		manager.mutex.Lock()
		defer manager.mutex.Unlock()

		if !manager.adoptReleasingSessionUnlocked(session) {
			t.Error("a session that is only uploading was not taken over")
		}
	}
	session.fsClient = client
	session.fs = &fakeSessionFileSystem{}

	session.mutex.RLock()
	epoch := session.releaseEpoch
	session.mutex.RUnlock()

	manager.drainSessionForRelease(session, epoch)

	if client.drainCalls != 1 {
		t.Fatalf("drained %d times, want once", client.drainCalls)
	}
	if client.syncCalls != 0 {
		t.Fatalf("flushed a session that was taken over during the drain %d times", client.syncCalls)
	}
}

// A release that was taken over can still be inside its drain: the flush it was
// waiting on is slow, and meanwhile the session is serving a new client. It
// must not collect that client's file handles, or touch the session at all.
func TestAStaleDrainLeavesTheNewClientsSessionAlone(t *testing.T) {
	manager, session := newReleasingTestSession(t, "session-stale-drain")

	session.mutex.RLock()
	epoch := session.releaseEpoch
	session.mutex.RUnlock()

	manager.mutex.Lock()
	if !manager.adoptReleasingSessionUnlocked(session) {
		manager.mutex.Unlock()
		t.Fatal("a session that is only flushing was not taken over")
	}
	manager.mutex.Unlock()

	// The client that took the session over opens a file.
	fileHandle := &countingCloseFileHandle{stubFileHandle: stubFileHandle{
		id:    "handle-of-the-new-client",
		entry: &irodsclient_fs.Entry{Path: "/tempZone/home/rods/in-use.dat"},
	}}
	poolFileHandle, err := NewPoolFileHandle(session.id, "client-that-took-over", fileHandle, irodsfs_common_irods.NewFileLockManager())
	if err != nil {
		t.Fatalf("NewPoolFileHandle: %v", err)
	}
	session.AddPoolFileHandle(poolFileHandle)

	client := &drainRecordingIRODSFSClient{session: session}
	session.fsClient = client
	session.fs = &fakeSessionFileSystem{}

	// The goroutine of the release that was taken over gets here late.
	manager.drainSessionForRelease(session, epoch)

	if fileHandle.closed != 0 {
		t.Fatalf("a release that was taken over closed the new client's file handle %d times", fileHandle.closed)
	}
	session.mutex.RLock()
	remaining := len(session.poolFileHandles)
	session.mutex.RUnlock()
	if remaining != 1 {
		t.Fatalf("the new client holds %d file handles, want 1", remaining)
	}
	if client.drainCalls != 0 || client.syncCalls != 0 {
		t.Fatalf("a release that was taken over uploaded through the session (drain %d, flush %d)",
			client.drainCalls, client.syncCalls)
	}
}

// A session is shared by every mount of one iRODS account, so a handle has to
// name the client that opened it: another client asking for it is told it does
// not exist rather than handed someone else's open file.
func TestAFileHandleAnswersOnlyToTheClientThatOpenedIt(t *testing.T) {
	session := &PoolSession{
		id:              "session-owned-handles",
		connections:     map[string]connInfo{},
		poolFileHandles: map[string]*PoolFileHandle{},
		logger:          log.NewEntry(log.StandardLogger()),
	}

	handle, err := NewPoolFileHandle(session.id, "client-a", &stubFileHandle{
		id:    "handle-1",
		entry: &irodsclient_fs.Entry{Path: "/tempZone/home/rods/a.dat"},
	}, irodsfs_common_irods.NewFileLockManager())
	if err != nil {
		t.Fatalf("NewPoolFileHandle: %v", err)
	}
	session.AddPoolFileHandle(handle)

	if _, err := session.GetPoolFileHandle("client-a", "handle-1"); err != nil {
		t.Fatalf("the client that opened the handle cannot use it: %v", err)
	}
	if _, err := session.GetPoolFileHandle("client-b", "handle-1"); err == nil {
		t.Fatal("another client was handed a file handle it did not open")
	}
	// A client that reports no id of its own - an older one - keeps the
	// session-wide behaviour it had before handles carried an owner.
	if _, err := session.GetPoolFileHandle("", "handle-1"); err != nil {
		t.Fatalf("a client without an id of its own cannot use the handle: %v", err)
	}
}

// The drain collects what a departed client left behind, and only that: the
// handles of a client connected to the session are in use.
func TestTheDrainCollectsOnlyTheHandlesOfDepartedClients(t *testing.T) {
	session := &PoolSession{
		id:              "session-mixed-handles",
		connections:     map[string]connInfo{"conn-1": {clientID: "client-present"}},
		poolFileHandles: map[string]*PoolFileHandle{},
		logger:          log.NewEntry(log.StandardLogger()),
	}

	handles := map[string]*countingCloseFileHandle{}
	for _, owner := range []string{"client-present", "client-gone"} {
		fileHandle := &countingCloseFileHandle{stubFileHandle: stubFileHandle{
			id:    "handle-of-" + owner,
			entry: &irodsclient_fs.Entry{Path: "/tempZone/home/rods/" + owner + ".dat"},
		}}
		poolFileHandle, err := NewPoolFileHandle(session.id, owner, fileHandle, irodsfs_common_irods.NewFileLockManager())
		if err != nil {
			t.Fatalf("NewPoolFileHandle: %v", err)
		}
		session.AddPoolFileHandle(poolFileHandle)
		handles[owner] = fileHandle
	}

	if err := session.releaseFileHandlesOfDepartedClients(); err != nil {
		t.Fatalf("releaseFileHandlesOfDepartedClients: %v", err)
	}

	if handles["client-gone"].closed != 1 {
		t.Fatalf("the departed client's handle was closed %d times, want once", handles["client-gone"].closed)
	}
	if handles["client-present"].closed != 0 {
		t.Fatalf("the connected client's handle was closed %d times, want never", handles["client-present"].closed)
	}

	session.mutex.RLock()
	defer session.mutex.RUnlock()
	if _, kept := session.poolFileHandles["handle-of-client-present"]; !kept {
		t.Fatal("the connected client's handle was forgotten")
	}
	if _, kept := session.poolFileHandles["handle-of-client-gone"]; kept {
		t.Fatal("the departed client's handle is still registered")
	}
}

// A logout says the client is done, so what it left open goes right away
// instead of waiting for the session's release. A transport drop says nothing
// of the sort, and is covered by TestADroppedConnectionKeepsItsFileHandles.
func TestALogoutCollectsTheHandlesOfTheClientThatLeft(t *testing.T) {
	manager, session := newConnectedTestSession(t, "session-logout")
	leaving := openTestFileHandle(t, session, "client-leaving")
	staying := openTestFileHandle(t, session, "client-staying")

	manager.LogoutConnection("conn-of-client-leaving")
	session.backgroundWg.Wait()

	if leaving.closed != 1 {
		t.Fatalf("the handle of the client that logged out was closed %d times, want once", leaving.closed)
	}
	if staying.closed != 0 {
		t.Fatalf("the handle of a client that is still connected was closed %d times, want never", staying.closed)
	}
}

// A connection can go away without a logout - a transport drop - and the client
// reconnects and retries with the handle ids it holds, so the handles have to
// survive until the session is released.
func TestADroppedConnectionKeepsItsFileHandles(t *testing.T) {
	manager, session := newConnectedTestSession(t, "session-dropped")
	dropped := openTestFileHandle(t, session, "client-leaving")

	manager.RemoveConnection("conn-of-client-leaving")
	session.backgroundWg.Wait()

	if dropped.closed != 0 {
		t.Fatalf("the handle of a client that only lost its connection was closed %d times, want never", dropped.closed)
	}
	if _, err := session.GetPoolFileHandle("client-leaving", "handle-of-client-leaving"); err != nil {
		t.Fatalf("a reconnecting client cannot reach its handle: %v", err)
	}
}

// newConnectedTestSession builds a live session holding two clients, each on its
// own connection.
func newConnectedTestSession(t *testing.T, sessionID string) (*PoolSessionManager, *PoolSession) {
	t.Helper()

	session := &PoolSession{
		id:         sessionID,
		accountKey: sessionID,
		connections: map[string]connInfo{
			"conn-of-client-leaving": {clientID: "client-leaving"},
			"conn-of-client-staying": {clientID: "client-staying"},
		},
		poolFileHandles: map[string]*PoolFileHandle{},
		logger:          log.NewEntry(log.StandardLogger()),
	}
	manager := &PoolSessionManager{
		sessions:          map[string]*PoolSession{sessionID: session},
		releasingSessions: map[string]*PoolSession{},
		pendingReleases:   map[string]*time.Timer{},
		connMap: map[string]string{
			"conn-of-client-leaving": sessionID,
			"conn-of-client-staying": sessionID,
		},
		config: &PoolServerConfig{sessionCloseGracePeriod: time.Hour},
		logger: log.NewEntry(log.StandardLogger()),
	}
	return manager, session
}

func openTestFileHandle(t *testing.T, session *PoolSession, clientID string) *countingCloseFileHandle {
	t.Helper()

	fileHandle := &countingCloseFileHandle{stubFileHandle: stubFileHandle{
		id:    "handle-of-" + clientID,
		entry: &irodsclient_fs.Entry{Path: "/tempZone/home/rods/" + clientID + ".dat"},
	}}
	poolFileHandle, err := NewPoolFileHandle(session.id, clientID, fileHandle, irodsfs_common_irods.NewFileLockManager())
	if err != nil {
		t.Fatalf("NewPoolFileHandle: %v", err)
	}
	session.AddPoolFileHandle(poolFileHandle)
	return fileHandle
}
