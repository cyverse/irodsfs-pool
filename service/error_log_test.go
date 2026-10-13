package service

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_util "github.com/cyverse/irodsfs-common/util"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	log "github.com/sirupsen/logrus"
)

type failingListIRODSFSClient struct {
	irodsfs_common_irods.IRODSFSClient
	err error
}

func (client *failingListIRODSFSClient) List(string) ([]*irodsclient_fs.Entry, error) {
	return nil, client.err
}

func TestErrorLogGroupsTheSameError(t *testing.T) {
	base := time.Date(2026, time.October, 13, 8, 0, 0, 0, time.UTC)
	var errLog errorLog
	errLog.add(errorOccurrence{time: base, operation: "List", message: "connection reset", user: "alice@z", path: "/a", detail: "stack a"})
	errLog.add(errorOccurrence{time: base.Add(time.Second), operation: "Stat", message: "connection reset", user: "alice@z", path: "/s"})
	errLog.add(errorOccurrence{time: base.Add(2 * time.Second), operation: "List", message: "connection reset", user: "bob@z", path: "/b", detail: "stack b"})
	errLog.add(errorOccurrence{time: base.Add(3 * time.Second), operation: "List", message: "connection reset", user: "alice@z", path: "/c", detail: "stack c"})

	groups, total := errLog.snapshot()
	if total != 4 {
		t.Fatalf("total = %d, want 4", total)
	}
	if len(groups) != 2 {
		t.Fatalf("kept %d groups, want 2 (the same message of another operation is its own error)", len(groups))
	}

	list := groups[0]
	if list.Operation != "List" || list.Count != 3 {
		t.Fatalf("most recent group = %#v, want List seen 3 times", list)
	}
	if !list.FirstSeen.Equal(base) || !list.LastSeen.Equal(base.Add(3*time.Second)) {
		t.Fatalf("group seen from %v to %v", list.FirstSeen, list.LastSeen)
	}
	if list.Path != "/c" || list.Detail != "stack c" || list.User != "alice@z" {
		t.Fatalf("group does not carry the latest occurrence: %#v", list)
	}
	if strings.Join(list.Users, ",") != "alice@z,bob@z" {
		t.Fatalf("users = %v, want alice@z and bob@z once each", list.Users)
	}
	if groups[1].Operation != "Stat" || groups[1].Count != 1 {
		t.Fatalf("older group = %#v", groups[1])
	}
}

func TestErrorLogDropsTheLeastRecentlySeenError(t *testing.T) {
	base := time.Date(2026, time.October, 13, 8, 0, 0, 0, time.UTC)
	var errLog errorLog
	for i := 0; i < errorLogCapacity; i++ {
		errLog.add(errorOccurrence{time: base.Add(time.Duration(i) * time.Second), operation: "List", message: fmt.Sprintf("error %d", i)})
	}
	// Seeing the oldest error again keeps it, so the next oldest goes instead.
	errLog.add(errorOccurrence{time: base.Add(time.Hour), operation: "List", message: "error 0"})
	errLog.add(errorOccurrence{time: base.Add(2 * time.Hour), operation: "List", message: "new error"})

	groups, total := errLog.snapshot()
	if total != errorLogCapacity+2 {
		t.Fatalf("total = %d, want %d", total, errorLogCapacity+2)
	}
	if len(groups) != errorLogCapacity {
		t.Fatalf("kept %d groups, want %d", len(groups), errorLogCapacity)
	}
	kept := map[string]bool{}
	for _, group := range groups {
		kept[group.Message] = true
	}
	if !kept["error 0"] || !kept["new error"] || kept["error 1"] {
		t.Fatalf("kept error 0 = %v, new error = %v, error 1 = %v; want the least recently seen, error 1, dropped",
			kept["error 0"], kept["new error"], kept["error 1"])
	}
	if groups[0].Message != "new error" || groups[1].Message != "error 0" {
		t.Fatalf("groups start with %q, %q; want the most recently seen first", groups[0].Message, groups[1].Message)
	}
}

func TestErrorLogCapsTheUsersOfAnError(t *testing.T) {
	var errLog errorLog
	for i := 0; i < errorGroupMaxUsers+3; i++ {
		errLog.add(errorOccurrence{time: time.Now(), operation: "List", message: "connection reset", user: fmt.Sprintf("user%d@z", i)})
	}

	groups, _ := errLog.snapshot()
	if len(groups[0].Users) != errorGroupMaxUsers || !groups[0].MoreUsers {
		t.Fatalf("users = %v, more = %v; want %d users and more", groups[0].Users, groups[0].MoreUsers, errorGroupMaxUsers)
	}
	if !strings.HasSuffix(formatErrorGroupUsers(groups[0]), ", …") {
		t.Fatalf("formatted users %q do not say there are more", formatErrorGroupUsers(groups[0]))
	}
}

func TestErrorOccurrenceTruncatesALongStackTrace(t *testing.T) {
	session := &PoolSession{id: "session-1"}
	err := errors.New(strings.Repeat("x", errorLogMaxDetailLength*2))

	occurrence := newErrorOccurrence(session, "List", "/zone/home", err)
	if len(occurrence.detail) > errorLogMaxDetailLength+len("\n... (truncated)") {
		t.Fatalf("detail is %d bytes, want at most %d", len(occurrence.detail), errorLogMaxDetailLength)
	}
	if !strings.HasSuffix(occurrence.detail, "(truncated)") {
		t.Fatal("truncated detail is not marked")
	}
}

func TestRequestErrorIsLoggedWithItsStackAndRecorded(t *testing.T) {
	sessionOutput := &bytes.Buffer{}
	sessionLogger := log.New()
	sessionLogger.SetOutput(sessionOutput)
	sessionLogger.SetFormatter(&irodsfs_common_util.StacktraceTextFormatter{})

	session := &PoolSession{
		id:           "session-1",
		irodsAccount: &irodsclient_types.IRODSAccount{ClientUser: "alice", ClientZone: "tempZone"},
		fsClient: &failingListIRODSFSClient{
			err: errors.Wrapf(errors.New("connection reset"), "failed to receive a response message"),
		},
		logger: sessionLogger.WithField("sessionID", "session-1"),
	}
	server := &PoolServer{
		logger: log.NewEntry(log.New()),
		sessionManager: &PoolSessionManager{
			sessions: map[string]*PoolSession{session.id: session},
		},
	}

	if _, err := server.List(context.Background(), &api.ListRequest{SessionId: session.id, Path: "/tempZone/home/alice"}); err == nil {
		t.Fatal("List succeeded, want the client error")
	}

	logged := sessionOutput.String()
	for _, expected := range []string{`msg="List failed"`, "connection reset", "--- Stack Trace ---"} {
		if !strings.Contains(logged, expected) {
			t.Fatalf("session log %q does not contain %q", logged, expected)
		}
	}

	groups, total := server.GetErrors()
	if total != 1 || len(groups) != 1 {
		t.Fatalf("recorded %d errors (%d kept), want 1", total, len(groups))
	}
	group := groups[0]
	if group.Operation != "List" || group.Count != 1 || group.Path != "/tempZone/home/alice" || group.User != "alice@tempZone" || group.SessionID != "session-1" {
		t.Fatalf("unexpected error: %#v", group)
	}
	if !strings.Contains(group.Detail, "stack trace") {
		t.Fatalf("error detail has no stack trace: %q", group.Detail)
	}
}

func TestMonitoringListsErrorsWithTheirStackTrace(t *testing.T) {
	server := &PoolServer{
		sessionManager: &PoolSessionManager{
			sessions: map[string]*PoolSession{},
		},
	}
	for i := 0; i < 2; i++ {
		server.errorLog.add(errorOccurrence{
			time:      time.Now(),
			sessionID: "session-1",
			user:      "alice@tempZone",
			operation: "List",
			path:      "/tempZone/<script>",
			message:   "failed <b>",
			detail:    "failed <b>\n(1) attached stack trace",
		})
	}

	handler := NewMonitoringHandler(server, commons.NewDefaultConfig())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/monitor", nil))
	body := recorder.Body.String()

	for _, expected := range []string{
		"<h2>Errors</h2>",
		"Total: 2 since start (1 distinct errors shown)",
		"<td>2</td>",
		`onclick="showDetail('error-1')"`,
		`id="detail-error-1"`,
		`<pre class="stack-trace">failed &lt;b&gt;` + "\n(1) attached stack trace</pre>",
		"/tempZone/&lt;script&gt;",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("monitoring page does not contain %q", expected)
		}
	}
	if strings.Contains(body, "<script>\"") || strings.Contains(body, "failed <b>") {
		t.Fatal("error content was not escaped")
	}
}
