package service

import (
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-pool/commons"
)

type gateTestClient struct {
	irodsfs_common_irods.IRODSFSClient
}

func newGateTestSession() *PoolSession {
	return &PoolSession{
		id:       "gate-test",
		fsClient: &gateTestClient{},
		logger:   log.NewEntry(log.New()),
	}
}

// The release waits for a request that already holds the client, and a request
// arriving once the release started closing gets a session-gone error rather
// than a client that is about to be closed.
func TestReleaseWaitsForRunningRequestAndRefusesNewOnes(t *testing.T) {
	session := newGateTestSession()

	client, done, err := session.AcquireIRODSFSClient()
	if err != nil || client == nil {
		t.Fatalf("acquire = %v, %v", client, err)
	}

	closed := make(chan struct{})
	go func() {
		session.closeClientAccess()
		close(closed)
	}()

	select {
	case <-closed:
		t.Fatal("close returned while a request still held the client")
	case <-time.After(100 * time.Millisecond):
	}

	if _, _, err := session.AcquireIRODSFSClient(); !commons.IsSessionNotFoundError(err) {
		t.Fatalf("acquire during close = %v, want session not found", err)
	}

	done()
	done() // calling it twice must not release a second request's hold

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("close did not return after the request finished")
	}
}

func TestAcquireAfterClientGone(t *testing.T) {
	session := newGateTestSession()
	session.fsClient = nil

	if _, _, err := session.AcquireIRODSFSClient(); !commons.IsSessionNotFoundError(err) {
		t.Fatalf("acquire without a client = %v, want session not found", err)
	}
	if n := session.clientInUse.Load(); n != 0 {
		t.Fatalf("clientInUse = %d after a refused acquire", n)
	}
}
