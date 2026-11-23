package service

import (
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

// A grace-period timer that fired but lost the race for the manager lock to a
// login that cancelled it must not release the session the login was just
// handed. The login adds its connection only after NewSession returns, so the
// session has no connection at that point.
func TestCancelledGracePeriodTimerDoesNotReleaseSession(t *testing.T) {
	session := &PoolSession{
		id:          "timer-test",
		connections: map[string]connInfo{},
		logger:      log.NewEntry(log.New()),
	}
	manager := &PoolSessionManager{
		sessions:          map[string]*PoolSession{session.id: session},
		releasingSessions: map[string]*PoolSession{},
		pendingReleases:   map[string]*time.Timer{},
		logger:            log.NewEntry(log.New()),
	}

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	manager.pendingReleases[session.id] = timer

	// The login cancels the release, as NewSession does.
	manager.mutex.Lock()
	timer.Stop()
	delete(manager.pendingReleases, session.id)
	manager.mutex.Unlock()

	// The callback that had already fired gets the lock afterwards.
	manager.startSessionRelease(session.id, &timer)

	manager.mutex.RLock()
	_, live := manager.sessions[session.id]
	_, releasing := manager.releasingSessions[session.id]
	manager.mutex.RUnlock()
	if !live || releasing {
		t.Fatalf("session live=%v releasing=%v after a cancelled timer fired, want live", live, releasing)
	}
}
