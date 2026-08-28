package service

import (
	"bytes"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

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
