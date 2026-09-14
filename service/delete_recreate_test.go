package service

import (
	"sync"
	"testing"
	"time"

	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/irodsfs-common/irods/stagingfs"
	"github.com/stretchr/testify/require"
)

// recordingStagingClient is the remote iRODS side of a staging filesystem.
// Keeping it local makes this test exercise the same DIRTY state transitions
// without requiring an iRODS server or a FUSE mount.
type recordingStagingClient struct {
	mu      sync.Mutex
	uploads []string
	deletes []string
}

func (c *recordingStagingClient) DownloadFileParallel(string, string, int, irodsclient_common.TransferTrackerCallback) error {
	return nil
}

func (c *recordingStagingClient) UploadFileParallel(_ string, irodsPath string, _ int, _ irodsclient_common.TransferTrackerCallback) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.uploads = append(c.uploads, irodsPath)
	return nil
}

func (c *recordingStagingClient) RenameFileToFile(string, string) error { return nil }
func (c *recordingStagingClient) RenameDirToDir(string, string) error   { return nil }

func (c *recordingStagingClient) RemoveFile(irodsPath string, _ bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletes = append(c.deletes, irodsPath)
	return nil
}

func (c *recordingStagingClient) MakeDir(string, bool) error         { return nil }
func (c *recordingStagingClient) RemoveDir(string, bool, bool) error { return nil }

// stagingHandle stands in for the file handle that owns a staged open file.
// Staging keys the open ref to the handle, so the ref is released with
// ReleaseHandle and follows the file if it is renamed while open.
type stagingHandle struct{}

func (h *stagingHandle) UpdateStagingPath(string) {}

// TestDeleteThenImmediateRecreate exercises the reported sequence:
//
//	remote file delete -> local path disappears while DELETE is DIRTY
//	-> immediate recreation and first write at the identical path.
//
// The final pending action must be UPLOAD, not the obsolete DELETE.  In
// particular, opening the replacement file must not return an I/O error.
func TestDeleteThenImmediateRecreate(t *testing.T) {
	const irodsPath = "/test/delete-then-recreate.txt"

	remote := &recordingStagingClient{}
	staging, err := stagingfs.NewStagingFS(&stagingfs.StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        remote,
		// A long grace period keeps DELETE dirty until this test explicitly syncs.
		GracePeriod:  time.Hour,
		SyncInterval: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, staging.Close()) })

	require.NoError(t, staging.DeleteWithForce(irodsPath, true))
	deleteMeta := staging.Get(irodsPath)
	require.NotNil(t, deleteMeta)
	require.Equal(t, stagingfs.ActionDelete, deleteMeta.Action)

	handle := &stagingHandle{}
	file, err := staging.OpenForWriteFor(irodsPath, false, handle)
	require.NoError(t, err, "immediate recreation must not fail while DELETE is dirty")
	_, err = file.Write([]byte("replacement contents\n"))
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.True(t, staging.ReleaseHandle(handle))

	uploadMeta := staging.Get(irodsPath)
	require.NotNil(t, uploadMeta)
	require.Equal(t, stagingfs.ActionUpload, uploadMeta.Action)

	require.NoError(t, staging.SyncAll())
	require.Equal(t, []string{irodsPath}, remote.uploads)
	require.Empty(t, remote.deletes, "the stale delete must be cancelled by recreation")
}
