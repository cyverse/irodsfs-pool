package service

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// errorLogCapacity is how many distinct errors are kept. When a new one
	// arrives beyond it, the one seen least recently is dropped.
	errorLogCapacity = 200
	// errorLogMaxDetailLength caps the stack trace kept for an error, which
	// for an error wrapped through go-irodsclient runs to dozens of lines.
	errorLogMaxDetailLength = 8 * 1024
	// errorGroupMaxUsers caps the distinct users listed for an error.
	errorGroupMaxUsers = 10
)

// ErrorGroup is an error iRODS operations returned to clients, or a session
// failed with in the background, with every
// occurrence of the same operation failing with the same message folded into
// it. The session, path and stack trace are those of the latest occurrence.
type ErrorGroup struct {
	ID        uint64    `json:"id"`
	Operation string    `json:"operation"`
	Message   string    `json:"message"`
	Count     uint64    `json:"count"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
	Users     []string  `json:"users"`
	MoreUsers bool      `json:"more_users,omitempty"`
	SessionID string    `json:"session_id"`
	User      string    `json:"user"`
	Path      string    `json:"path,omitempty"`
	Detail    string    `json:"detail"`
}

// errorOccurrence is one error as it was reported.
type errorOccurrence struct {
	time      time.Time
	sessionID string
	user      string
	operation string
	path      string
	message   string
	detail    string
}

// errorLog keeps the distinct errors seen most recently. The zero value is
// ready to use.
type errorLog struct {
	mutex  sync.Mutex
	groups map[string]*ErrorGroup // key: operation and message
	nextID uint64
	total  uint64
}

func (errLog *errorLog) add(occurrence errorOccurrence) {
	errLog.mutex.Lock()
	defer errLog.mutex.Unlock()

	errLog.total++

	key := occurrence.operation + "\x00" + occurrence.message
	group, ok := errLog.groups[key]
	if !ok {
		if errLog.groups == nil {
			errLog.groups = map[string]*ErrorGroup{}
		}
		if len(errLog.groups) >= errorLogCapacity {
			errLog.evictLeastRecentLocked()
		}

		errLog.nextID++
		group = &ErrorGroup{
			ID:        errLog.nextID,
			Operation: occurrence.operation,
			Message:   occurrence.message,
			FirstSeen: occurrence.time,
		}
		errLog.groups[key] = group
	}

	group.Count++
	group.LastSeen = occurrence.time
	group.SessionID = occurrence.sessionID
	group.User = occurrence.user
	group.Path = occurrence.path
	group.Detail = occurrence.detail
	addErrorGroupUser(group, occurrence.user)
}

func addErrorGroupUser(group *ErrorGroup, user string) {
	if user == "" {
		return
	}
	for _, known := range group.Users {
		if known == user {
			return
		}
	}
	if len(group.Users) >= errorGroupMaxUsers {
		group.MoreUsers = true
		return
	}
	group.Users = append(group.Users, user)
}

// evictLeastRecentLocked drops the error seen least recently. The caller must
// hold errLog.mutex.
func (errLog *errorLog) evictLeastRecentLocked() {
	var oldestKey string
	var oldest *ErrorGroup
	for key, group := range errLog.groups {
		if oldest == nil || group.LastSeen.Before(oldest.LastSeen) ||
			(group.LastSeen.Equal(oldest.LastSeen) && group.ID < oldest.ID) {
			oldestKey, oldest = key, group
		}
	}
	if oldest != nil {
		delete(errLog.groups, oldestKey)
	}
}

// snapshot returns the kept errors, the most recently seen first, and how many
// occurrences were reported since start.
func (errLog *errorLog) snapshot() ([]ErrorGroup, uint64) {
	errLog.mutex.Lock()
	defer errLog.mutex.Unlock()

	result := make([]ErrorGroup, 0, len(errLog.groups))
	for _, group := range errLog.groups {
		copied := *group
		copied.Users = append([]string(nil), group.Users...)
		result = append(result, copied)
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].LastSeen.Equal(result[j].LastSeen) {
			return result[i].LastSeen.After(result[j].LastSeen)
		}
		return result[i].ID > result[j].ID
	})
	return result, errLog.total
}

// reportError logs an error an iRODS operation failed with, along with its
// stack trace, and keeps it for the monitoring page.
func (server *PoolServer) reportError(session *PoolSession, logger *log.Entry, operation string, path string, err error) {
	logger.WithError(err).Errorf("%s failed", operation)
	server.recordError(session, operation, path, err)
}

// recordError keeps an error for the monitoring page without logging it, for
// a caller that has logged it already.
func (server *PoolServer) recordError(session *PoolSession, operation string, path string, err error) {
	server.errorLog.add(newErrorOccurrence(session, operation, path, err))
}

func newErrorOccurrence(session *PoolSession, operation string, path string, err error) errorOccurrence {
	occurrence := errorOccurrence{
		time:      time.Now(),
		sessionID: session.id,
		operation: operation,
		path:      path,
		message:   err.Error(),
		detail:    fmt.Sprintf("%+v", err),
	}
	if session.irodsAccount != nil {
		occurrence.user = formatIRODSUser(session.irodsAccount.ClientUser, session.irodsAccount.ClientZone)
	}
	if len(occurrence.detail) > errorLogMaxDetailLength {
		occurrence.detail = strings.ToValidUTF8(occurrence.detail[:errorLogMaxDetailLength], "") + "\n... (truncated)"
	}
	return occurrence
}

// GetErrors returns the kept errors, the most recently seen first, and how
// many occurrences were reported since start.
func (server *PoolServer) GetErrors() ([]ErrorGroup, uint64) {
	return server.errorLog.snapshot()
}
