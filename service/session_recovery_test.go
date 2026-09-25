package service

import (
	"testing"
	"time"

	"github.com/cyverse/irodsfs-pool/service/api"
	log "github.com/sirupsen/logrus"
)

// newRecoveringTestSession builds a session the way a recovery does: parked in
// the releasing map holding the staging area of a run that was interrupted,
// with no client of its own yet.
func newRecoveringTestSession(t *testing.T, sessionID string) (*PoolSessionManager, *PoolSession, uint64) {
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
		releasingSessions: map[string]*PoolSession{},
		pendingReleases:   map[string]*time.Timer{},
		logger:            log.NewEntry(log.StandardLogger()),
	}

	return manager, session, manager.registerRecoveringSession(session)
}

func recoveryTestAccount() *api.Account {
	return &api.Account{
		IrodsHost:     "irods.example.org",
		IrodsPort:     1247,
		IrodsUserName: "rods",
		IrodsZoneName: "tempZone",
	}
}

// A recovery rebuilds the session of an interrupted run to upload what is left
// in its staging area, and the client that comes back for that account meets it
// holding that staging area. The session has to be handed over, because a
// session created beside the recovery would open the same staging directory,
// whose lock the recovery holds until it is done.
func TestALoginDuringARecoveryTakesTheSessionOver(t *testing.T) {
	account := recoveryTestAccount()
	accountKey := makeAccountKey(convertAccountFromAPIToIRODS(account))

	manager, session, epoch := newRecoveringTestSession(t, accountKey)
	session.fsClient = &drainRecordingIRODSFSClient{session: session}
	session.fs = &fakeSessionFileSystem{}
	manager.offerRecoveringSessionForAdoption(session)

	recovered, err := manager.NewSession(account, "test")
	if err != nil {
		t.Fatalf("NewSession during a recovery: %v", err)
	}
	if recovered != session {
		t.Fatalf("NewSession returned %v, want the session being recovered", recovered)
	}

	manager.mutex.RLock()
	_, live := manager.sessions[accountKey]
	_, stillRecovering := manager.releasingSessions[accountKey]
	manager.mutex.RUnlock()
	if !live || stillRecovering {
		t.Fatalf("session is live=%v, recovering=%v, want live", live, stillRecovering)
	}

	// The recovery has to leave the session alone now, rather than release a
	// session that is back in use.
	tornDown, releaseErr := manager.teardownReleasingSession(session, epoch, nil)
	if releaseErr != nil {
		t.Fatalf("teardown of a taken over recovery: %v", releaseErr)
	}
	if tornDown {
		t.Fatal("the recovery released a session that a login had taken over")
	}
}

// Between claiming the session and opening its client the recovery has nothing
// to hand over, so a login that arrives then waits. It must be woken as soon as
// the session can serve it, or it would wait out an upload that a run with a
// full staging area stretches into minutes.
func TestALoginWaitingForARecoveryIsWokenWhenTheSessionIsReady(t *testing.T) {
	account := recoveryTestAccount()
	accountKey := makeAccountKey(convertAccountFromAPIToIRODS(account))

	manager, session, _ := newRecoveringTestSession(t, accountKey)

	type loginResult struct {
		session *PoolSession
		err     error
	}
	logins := make(chan loginResult, 1)
	go func() {
		loggedIn, err := manager.NewSession(account, "test")
		logins <- loginResult{session: loggedIn, err: err}
	}()

	select {
	case result := <-logins:
		t.Fatalf("a login was served while the recovered session was still opening: %v, %v", result.session, result.err)
	case <-time.After(50 * time.Millisecond):
	}

	session.fsClient = &drainRecordingIRODSFSClient{session: session}
	session.fs = &fakeSessionFileSystem{}
	manager.offerRecoveringSessionForAdoption(session)

	select {
	case result := <-logins:
		if result.err != nil {
			t.Fatalf("NewSession waiting for a recovery: %v", result.err)
		}
		if result.session != session {
			t.Fatalf("NewSession returned %v, want the session being recovered", result.session)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the login was not woken when the recovered session became ready")
	}
}

// A recovery nobody came back for uploads the staging area it rebuilt and then
// releases the session, which frees that staging area for the next login.
func TestARecoveryWithoutALoginUploadsAndReleasesTheSession(t *testing.T) {
	manager, session, epoch := newRecoveringTestSession(t, "session-recovered")

	client := &drainRecordingIRODSFSClient{session: session}
	session.fsClient = client
	session.fs = &fakeSessionFileSystem{}
	manager.offerRecoveringSessionForAdoption(session)

	if err := manager.uploadRecoveredStaging(session, epoch); err != nil {
		t.Fatalf("uploadRecoveredStaging: %v", err)
	}
	if client.drainCalls != 1 || client.syncCalls != 1 {
		t.Fatalf("staged data drained %d times and flushed %d times, want once each", client.drainCalls, client.syncCalls)
	}

	tornDown, releaseErr := manager.teardownReleasingSession(session, epoch, nil)
	if releaseErr != nil {
		t.Fatalf("teardown after a recovery: %v", releaseErr)
	}
	if !tornDown {
		t.Fatal("the recovery did not release the session it rebuilt")
	}
	if client.releaseCalls != 1 {
		t.Fatalf("the filesystem client was released %d times, want once", client.releaseCalls)
	}

	manager.mutex.RLock()
	_, stillHeld := manager.releasingSessions[session.id]
	manager.mutex.RUnlock()
	if stillHeld {
		t.Fatal("the recovered session still holds its staging area after its release")
	}
}

// A login that lands during the drain leaves nothing to flush: the flush holds
// every write handle off while it runs, and the session belongs to a client
// again. What the drain could not move goes out with the background sync.
func TestARecoverySkipsTheFlushWhenALoginTakesTheSessionOver(t *testing.T) {
	manager, session, epoch := newRecoveringTestSession(t, "session-recovered-adopted")

	client := &drainRecordingIRODSFSClient{session: session}
	client.onDrain = func() {
		manager.mutex.Lock()
		defer manager.mutex.Unlock()

		if !manager.adoptReleasingSessionUnlocked(session) {
			t.Error("a recovery that is only uploading was not taken over")
		}
	}
	session.fsClient = client
	session.fs = &fakeSessionFileSystem{}
	manager.offerRecoveringSessionForAdoption(session)

	if err := manager.uploadRecoveredStaging(session, epoch); err != nil {
		t.Fatalf("uploadRecoveredStaging: %v", err)
	}
	if client.drainCalls != 1 {
		t.Fatalf("staged data drained %d times, want once", client.drainCalls)
	}
	if client.syncCalls != 0 {
		t.Fatalf("flushed a session that a login took over during the drain %d times", client.syncCalls)
	}
}

// A live session already holds the staging area and uploads it when it is
// released, so a recovery must not open a second client on it.
func TestRecoverSessionRefusesWhileAClientHoldsTheStagingArea(t *testing.T) {
	manager, session, _ := newRecoveringTestSession(t, "session-in-use")

	if !manager.sessionInUse(session.id) {
		t.Fatal("a session held by a recovery is not reported as in use")
	}

	manager.mutex.Lock()
	manager.adoptReleasingSessionUnlocked(session)
	manager.mutex.Unlock()

	if !manager.sessionInUse(session.id) {
		t.Fatal("a live session is not reported as in use")
	}

	if _, err := manager.RecoverSession(session.id); err == nil {
		t.Fatal("a session whose staging area a client holds was accepted for recovery")
	}
}
