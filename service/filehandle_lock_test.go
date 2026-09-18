package service

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-pool/service/api"
)

const (
	lockTestPath = "/tempZone/home/rods/locked.txt"

	// two mounts, each with an id of its own
	callerA = "client-a"
	callerB = "client-b"
)

func isFileLockConflict(err error) bool {
	return err != nil && errors.Is(err, irodsfs_common_irods.ErrFileLockConflict)
}

func newLockTestHandle(t *testing.T, sessionID string, handleID string, manager *irodsfs_common_irods.FileLockManager) *PoolFileHandle {
	t.Helper()

	handle, err := NewPoolFileHandle(sessionID, "", &stubFileHandle{
		id:    handleID,
		entry: &irodsclient_fs.Entry{Path: lockTestPath},
		mode:  irodsclient_types.FileOpenModeReadWrite,
	}, manager)
	if err != nil {
		t.Fatalf("failed to create pool file handle: %v", err)
	}

	return handle
}

func wholeFileLock(lockType irodsfs_common_irods.FileLockType, owner uint64, pid uint32, flock bool) *irodsfs_common_irods.FileLock {
	return &irodsfs_common_irods.FileLock{
		Type: lockType,
		Owner: irodsfs_common_irods.FileLockOwner{
			Owner: owner,
			Flock: flock,
		},
		Pid:   pid,
		Start: 0,
		End:   irodsfs_common_irods.FileLockEndOfFile,
	}
}

// Two mounts reach the server as two sessions. The whole point of holding the
// lock table on the server is that they see each other's locks.
func TestPoolFileHandleLocksAreSharedAcrossSessions(t *testing.T) {
	manager := irodsfs_common_irods.NewFileLockManager()

	first := newLockTestHandle(t, "session-a", "handle-a", manager)
	second := newLockTestHandle(t, "session-b", "handle-b", manager)

	if err := first.Setlk(callerA, wholeFileLock(irodsfs_common_irods.FileLockTypeWrite, 1, 100, false)); err != nil {
		t.Fatalf("first handle failed to lock: %v", err)
	}

	err := second.Setlk(callerB, wholeFileLock(irodsfs_common_irods.FileLockTypeWrite, 1, 200, false))
	if err == nil {
		t.Fatal("the second session acquired a lock the first session holds")
	}
	if !isFileLockConflict(err) {
		t.Fatalf("expected a lock conflict, got %v", err)
	}

	conflict, err := second.Getlk(callerB, wholeFileLock(irodsfs_common_irods.FileLockTypeRead, 1, 200, false))
	if err != nil {
		t.Fatalf("Getlk failed: %v", err)
	}
	if conflict == nil {
		t.Fatal("Getlk reported no conflict while the first session holds a write lock")
	}
	if conflict.Pid != 100 {
		t.Fatalf("expected the holder pid 100, got %d", conflict.Pid)
	}
}

// A session is keyed by iRODS account and shared, so two mounts of one account
// arrive in the same session. The lock owner each of them reports is only
// unique within that mount, so the caller - not the session - has to scope it,
// or two unrelated owners that happen to report the same id would be taken for
// one and both would hold the same write lock.
func TestPoolFileHandleScopesLockOwnersByCaller(t *testing.T) {
	manager := irodsfs_common_irods.NewFileLockManager()

	const sharedSession = "session-of-one-account"
	firstMount := newLockTestHandle(t, sharedSession, "handle-1", manager)
	secondMount := newLockTestHandle(t, sharedSession, "handle-2", manager)

	if err := firstMount.Setlk(callerA, wholeFileLock(irodsfs_common_irods.FileLockTypeWrite, 7, 100, false)); err != nil {
		t.Fatalf("failed to lock: %v", err)
	}

	// the same owner id, reported by another mount, is another owner
	err := secondMount.Setlk(callerB, wholeFileLock(irodsfs_common_irods.FileLockTypeWrite, 7, 100, false))
	if !isFileLockConflict(err) {
		t.Fatalf("expected a lock conflict between two mounts of one session, got %v", err)
	}
}

// One mount, one process, two open files: the kernel reports one POSIX lock
// owner and it must not conflict with itself.
func TestPoolFileHandleKeepsOneCallerAsOneOwner(t *testing.T) {
	manager := irodsfs_common_irods.NewFileLockManager()

	first := newLockTestHandle(t, "session-a", "handle-1", manager)
	second := newLockTestHandle(t, "session-a", "handle-2", manager)

	if err := first.Setlk(callerA, wholeFileLock(irodsfs_common_irods.FileLockTypeWrite, 7, 100, false)); err != nil {
		t.Fatalf("failed to lock: %v", err)
	}
	if err := second.Setlk(callerA, wholeFileLock(irodsfs_common_irods.FileLockTypeWrite, 7, 100, false)); err != nil {
		t.Fatalf("the same caller conflicted with itself: %v", err)
	}
}

// An older client sends no id of its own, and then the session is all there is
// to keep its locks apart from the other sessions'
func TestPoolFileHandleFallsBackToTheSessionScope(t *testing.T) {
	manager := irodsfs_common_irods.NewFileLockManager()

	first := newLockTestHandle(t, "session-a", "handle-1", manager)
	second := newLockTestHandle(t, "session-b", "handle-2", manager)

	if err := first.Setlk("", wholeFileLock(irodsfs_common_irods.FileLockTypeWrite, 7, 100, false)); err != nil {
		t.Fatalf("failed to lock: %v", err)
	}

	err := second.Setlk("", wholeFileLock(irodsfs_common_irods.FileLockTypeWrite, 7, 100, false))
	if !isFileLockConflict(err) {
		t.Fatalf("expected a lock conflict across sessions, got %v", err)
	}
}

func TestPoolFileHandleReleasesLocksOnRelease(t *testing.T) {
	manager := irodsfs_common_irods.NewFileLockManager()

	holder := newLockTestHandle(t, "session-a", "handle-a", manager)
	other := newLockTestHandle(t, "session-b", "handle-b", manager)

	if err := holder.Setlk(callerA, wholeFileLock(irodsfs_common_irods.FileLockTypeWrite, 1, 100, true)); err != nil {
		t.Fatalf("failed to lock: %v", err)
	}

	if err := holder.Release(); err != nil {
		t.Fatalf("failed to release the handle: %v", err)
	}

	if err := other.Setlk(callerB, wholeFileLock(irodsfs_common_irods.FileLockTypeWrite, 1, 200, true)); err != nil {
		t.Fatalf("the lock outlived the handle that took it: %v", err)
	}
}

func TestPoolFileHandleSetlkwWaitsAndHonorsCancel(t *testing.T) {
	manager := irodsfs_common_irods.NewFileLockManager()

	holder := newLockTestHandle(t, "session-a", "handle-a", manager)
	waiter := newLockTestHandle(t, "session-b", "handle-b", manager)

	if err := holder.Setlk(callerA, wholeFileLock(irodsfs_common_irods.FileLockTypeWrite, 1, 100, true)); err != nil {
		t.Fatalf("failed to lock: %v", err)
	}

	// a canceled wait gives up instead of hanging
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := waiter.Setlkw(ctx, callerB, wholeFileLock(irodsfs_common_irods.FileLockTypeWrite, 1, 200, true)); err == nil {
		t.Fatal("Setlkw acquired a lock that another session holds")
	}

	// and it returns as soon as the holder lets go
	acquired := make(chan error, 1)
	go func() {
		acquired <- waiter.Setlkw(context.Background(), callerB, wholeFileLock(irodsfs_common_irods.FileLockTypeWrite, 1, 200, true))
	}()

	time.Sleep(50 * time.Millisecond)
	if err := holder.Setlk(callerA, wholeFileLock(irodsfs_common_irods.FileLockTypeUnlock, 1, 100, true)); err != nil {
		t.Fatalf("failed to unlock: %v", err)
	}

	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("Setlkw failed after the lock was released: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Setlkw did not return after the lock was released")
	}
}

func TestPoolFileHandleWithoutLockManagerFails(t *testing.T) {
	handle := newLockTestHandle(t, "session-a", "handle-a", nil)

	if err := handle.Setlk(callerA, wholeFileLock(irodsfs_common_irods.FileLockTypeWrite, 1, 100, false)); err == nil {
		t.Fatal("expected a handle without a lock manager to fail")
	}
}

func TestToFileLockConvertsTheWireFormat(t *testing.T) {
	lock, err := toFileLock(&api.FileLock{
		Type:  uint32(irodsfs_common_irods.FileLockTypeWrite),
		Start: 10,
		End:   20,
		Pid:   4242,
	}, 7, true)
	if err != nil {
		t.Fatalf("failed to convert: %v", err)
	}

	if lock.Type != irodsfs_common_irods.FileLockTypeWrite {
		t.Fatalf("expected a write lock, got %s", lock.Type.String())
	}
	if lock.Owner.Owner != 7 || !lock.Owner.Flock {
		t.Fatalf("unexpected owner %+v", lock.Owner)
	}
	if lock.Start != 10 || lock.End != 20 || lock.Pid != 4242 {
		t.Fatalf("unexpected lock %+v", lock)
	}

	// the session and the handle are filled in by the handle, not by the wire
	if lock.Owner.Scope != "" || lock.Owner.Handle != "" {
		t.Fatalf("the wire format must not carry the owner scope: %+v", lock.Owner)
	}

	if _, err := toFileLock(&api.FileLock{Type: 99}, 7, false); err == nil {
		t.Fatal("expected an unknown lock type to fail")
	}

	if _, err := toFileLock(nil, 7, false); err == nil {
		t.Fatal("expected a missing lock to fail")
	}
}

func TestToAPIFileLockRoundTrips(t *testing.T) {
	original := &irodsfs_common_irods.FileLock{
		Type:  irodsfs_common_irods.FileLockTypeRead,
		Pid:   17,
		Start: 100,
		End:   irodsfs_common_irods.FileLockEndOfFile,
	}

	wire := toAPIFileLock(original)
	restored, err := toFileLock(wire, 3, false)
	if err != nil {
		t.Fatalf("failed to convert back: %v", err)
	}

	if restored.Type != original.Type || restored.Pid != original.Pid ||
		restored.Start != original.Start || restored.End != original.End {
		t.Fatalf("round trip changed the lock: %+v -> %+v", original, restored)
	}
}
