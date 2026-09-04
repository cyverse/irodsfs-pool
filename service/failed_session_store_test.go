package service

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/dgraph-io/badger/v3"
	log "github.com/sirupsen/logrus"
)

func newFailedSessionStoreTestManager(dataRootPath string) *PoolSessionManager {
	logger := log.New()
	logger.SetOutput(&bytes.Buffer{})
	return &PoolSessionManager{
		config:              &PoolServerConfig{dataRootPath: dataRootPath},
		logger:              logger.WithFields(log.Fields{}),
		failedSessionDBPath: filepath.Join(dataRootPath, failedSessionDBDirectoryName),
	}
}

func newFailedSessionStoreTestSession(id string) *PoolSession {
	return &PoolSession{
		id:         id,
		accountKey: "account-" + id,
		irodsAccount: &irodsclient_types.IRODSAccount{
			AuthenticationScheme: "pam",
			Host:                 "irods.example.org",
			Port:                 1247,
			ClientUser:           "rods",
			ClientZone:           "tempZone",
			ProxyUser:            "proxy",
			ProxyZone:            "tempZone",
			Password:             "secret-password",
			Ticket:               "secret-ticket",
			PAMToken:             "secret-pam-token",
			DefaultResource:      "demoResc",
		},
		connections: map[string]connInfo{
			"connection-b": {appName: "irodsfs", description: "second"},
			"connection-a": {appName: "irodsfs", description: "first"},
		},
		lastAccessTime: time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC),
	}
}

func TestFailedSessionStorePersistsReloadsAndDeletesWhenEmpty(t *testing.T) {
	dataRootPath := t.TempDir()
	manager := newFailedSessionStoreTestManager(dataRootPath)
	firstSession := newFailedSessionStoreTestSession("session-1")
	secondSession := newFailedSessionStoreTestSession("session-2")

	manager.handleSessionReleaseResult(firstSession, errors.New("sync failed"))
	manager.handleSessionReleaseResult(secondSession, errors.New("sync failed"))

	failedSessions, err := manager.GetFailedSessions()
	if err != nil {
		t.Fatalf("GetFailedSessions: %v", err)
	}
	if len(failedSessions) != 2 || failedSessions[0].ID != "session-1" || failedSessions[1].ID != "session-2" {
		t.Fatalf("unexpected failed sessions: %#v", failedSessions)
	}
	if failedSessions[0].Status != FailedSessionStatusReleaseFailed || failedSessions[1].Status != FailedSessionStatusReleaseFailed {
		t.Fatalf("unexpected failed session statuses: %#v", failedSessions)
	}
	if len(failedSessions[0].Connections) != 2 || failedSessions[0].Connections[0].ConnectionID != "connection-a" {
		t.Fatalf("connections were not persisted deterministically: %#v", failedSessions[0].Connections)
	}
	if failedSessions[0].IRODSAccount.Host != "irods.example.org" || failedSessions[0].LastAccessTime != firstSession.lastAccessTime {
		t.Fatalf("failed session metadata was not persisted: %#v", failedSessions[0])
	}

	var storedValue []byte
	err = manager.failedSessionDB.View(func(transaction *badger.Txn) error {
		item, err := transaction.Get([]byte(failedSessionKeyPrefix + firstSession.id))
		if err != nil {
			return err
		}
		storedValue, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		t.Fatalf("read raw failed session record: %v", err)
	}
	for _, secret := range [][]byte{[]byte("secret-password"), []byte("secret-ticket"), []byte("secret-pam-token")} {
		if bytes.Contains(storedValue, secret) {
			t.Fatalf("failed session record contains secret %q: %s", secret, storedValue)
		}
	}

	if err := manager.closeFailedSessionStore(); err != nil {
		t.Fatalf("closeFailedSessionStore: %v", err)
	}

	reloaded := newFailedSessionStoreTestManager(dataRootPath)
	if err := reloaded.loadFailedSessionStore(); err != nil {
		t.Fatalf("loadFailedSessionStore: %v", err)
	}
	failedSessions, err = reloaded.GetFailedSessions()
	if err != nil || len(failedSessions) != 2 {
		t.Fatalf("reloaded failed sessions = %#v, err = %v", failedSessions, err)
	}

	reloaded.handleSessionReleaseResult(firstSession, nil)
	if _, err := os.Stat(reloaded.failedSessionDBPath); err != nil {
		t.Fatalf("failed session DB must remain while records exist: %v", err)
	}
	failedSessions, err = reloaded.GetFailedSessions()
	if err != nil || len(failedSessions) != 1 || failedSessions[0].ID != secondSession.id {
		t.Fatalf("failed sessions after first recovery = %#v, err = %v", failedSessions, err)
	}

	reloaded.handleSessionReleaseResult(secondSession, nil)
	if _, err := os.Stat(reloaded.failedSessionDBPath); !os.IsNotExist(err) {
		t.Fatalf("empty failed session DB must be removed, stat error: %v", err)
	}
	failedSessions, err = reloaded.GetFailedSessions()
	if err != nil || len(failedSessions) != 0 {
		t.Fatalf("failed sessions after full recovery = %#v, err = %v", failedSessions, err)
	}
}

func TestSessionLifecycleStoreMarksCrashAndReconnect(t *testing.T) {
	dataRootPath := t.TempDir()
	manager := newFailedSessionStoreTestManager(dataRootPath)
	session := newFailedSessionStoreTestSession("session-1")

	if err := manager.trackActiveSession(session); err != nil {
		t.Fatalf("trackActiveSession: %v", err)
	}
	visible, err := manager.GetFailedSessions()
	if err != nil || len(visible) != 0 {
		t.Fatalf("active sessions must not be exposed as pending recovery: %#v, err = %v", visible, err)
	}
	if err := manager.closeFailedSessionStore(); err != nil {
		t.Fatalf("closeFailedSessionStore: %v", err)
	}

	restarted := newFailedSessionStoreTestManager(dataRootPath)
	if err := restarted.loadFailedSessionStore(); err != nil {
		t.Fatalf("loadFailedSessionStore: %v", err)
	}
	visible, err = restarted.GetFailedSessions()
	if err != nil || len(visible) != 1 || visible[0].Status != FailedSessionStatusInterrupted {
		t.Fatalf("crash-interrupted sessions = %#v, err = %v", visible, err)
	}

	if err := restarted.trackActiveSession(session); err != nil {
		t.Fatalf("trackActiveSession after reconnect: %v", err)
	}
	visible, err = restarted.GetFailedSessions()
	if err != nil || len(visible) != 1 || visible[0].Status != FailedSessionStatusRecovering {
		t.Fatalf("reconnected recovery sessions = %#v, err = %v", visible, err)
	}

	restarted.handleSessionReleaseResult(session, nil)
	if _, err := os.Stat(restarted.failedSessionDBPath); !os.IsNotExist(err) {
		t.Fatalf("successful recovery must remove lifecycle DB, stat error: %v", err)
	}
}
