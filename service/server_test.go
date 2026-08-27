package service

import (
	"bytes"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
)

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
