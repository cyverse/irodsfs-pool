package service

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestNewSessionLoggerWritesOnlyToSessionFile(t *testing.T) {
	standardLogger := log.StandardLogger()
	originalOutput := standardLogger.Out
	defer standardLogger.SetOutput(originalOutput)

	standardOutput := &bytes.Buffer{}
	standardLogger.SetOutput(standardOutput)

	logRootPath := t.TempDir()
	sessionID := "test-session"
	logger, logFile, err := newSessionLogger(logRootPath, sessionID)
	if err != nil {
		t.Fatalf("newSessionLogger: %v", err)
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
