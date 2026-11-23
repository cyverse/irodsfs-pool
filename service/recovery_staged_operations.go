package service

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	irodsfs_common_stagingfs "github.com/cyverse/irodsfs-common/irods/stagingfs"
	"github.com/dgraph-io/badger/v3"
)

const stagedOperationKeyPrefix = "operation:"

// Local state of the path an operation works on, as found in the staging area.
const (
	StagedLocalPathFile      = "file"
	StagedLocalPathDirectory = "directory"
	StagedLocalPathMissing   = "missing"
	StagedLocalPathUnknown   = "unknown"
)

// StagedOperationInfo describes one operation a session still has to send to
// iRODS. LocalPath says what the staging area holds at the operation's path,
// which is what tells an upload that can run from one whose file is gone.
type StagedOperationInfo struct {
	OperationID   string    `json:"operation_id"`
	Action        string    `json:"action"`
	State         string    `json:"state"`
	Path          string    `json:"path"`
	OldPath       string    `json:"old_path,omitempty"`
	DependsOn     []string  `json:"depends_on,omitempty"`
	SyncFailCount int       `json:"sync_fail_count"`
	IsNew         bool      `json:"is_new"`
	CreatedAt     time.Time `json:"created_at"`
	LocalPath     string    `json:"local_path"`
}

// GetStagedOperations lists the operations left in the staging area of a
// session that is pending recovery.
//
// The staging database is read from a copy. A session that was not closed
// properly leaves a database that has to be replayed before it can be opened,
// which a read-only open refuses, and opening the original for writing would
// change the state a recovery is going to work from. A session that a client
// holds is refused: its database is open already, and its operations are
// reported with the live session.
func (manager *PoolSessionManager) GetStagedOperations(sessionID string) ([]StagedOperationInfo, error) {
	info, err := manager.getStoredSession(sessionID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read session from store")
	}
	if info == nil {
		return nil, errors.Newf("session %q not found in recovery store", sessionID)
	}
	if manager.sessionInUse(sessionID) {
		return nil, errors.Newf("session %q is in use by a client; see its live session for the staged operations", sessionID)
	}

	if manager.config.stagingRootPath == "" {
		return nil, errors.New("staging is not configured")
	}
	// The ID names a directory below the staging root, and it comes from a URL.
	if sessionID != filepath.Base(sessionID) || strings.ContainsAny(sessionID, `/\`) || sessionID == "." || sessionID == ".." {
		return nil, errors.Newf("invalid session ID %q", sessionID)
	}

	return readStagedOperations(filepath.Join(manager.config.stagingRootPath, sessionID))
}

// readStagedOperations reads the operations persisted under stagingRoot.
func readStagedOperations(stagingRoot string) ([]StagedOperationInfo, error) {
	metaPath := filepath.Join(stagingRoot, "meta")
	if _, err := os.Stat(metaPath); err != nil {
		if os.IsNotExist(err) {
			return []StagedOperationInfo{}, nil
		}
		return nil, errors.Wrap(err, "failed to inspect the staging database")
	}

	scratch, err := os.MkdirTemp("", "irodsfs-pool-staging-*")
	if err != nil {
		return nil, errors.Wrap(err, "failed to create a scratch directory")
	}
	defer os.RemoveAll(scratch)

	if err := copyDatabaseFiles(metaPath, scratch); err != nil {
		return nil, errors.Wrap(err, "failed to copy the staging database")
	}

	db, err := badger.Open(badger.DefaultOptions(scratch).WithLogger(nil))
	if err != nil {
		return nil, errors.Wrap(err, "failed to open the staging database")
	}
	defer db.Close()

	operations := []StagedOperationInfo{}
	err = db.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.Prefix = []byte(stagedOperationKeyPrefix)
		it := txn.NewIterator(options)
		defer it.Close()

		for it.Rewind(); it.ValidForPrefix(options.Prefix); it.Next() {
			var op irodsfs_common_stagingfs.StagingOperation
			if err := it.Item().Value(func(value []byte) error { return json.Unmarshal(value, &op) }); err != nil {
				return errors.Wrap(err, "failed to read a staged operation")
			}
			if op.Metadata == nil {
				continue
			}
			operations = append(operations, StagedOperationInfo{
				OperationID:   op.ID,
				Action:        op.Metadata.Action.String(),
				State:         string(op.State),
				Path:          op.Metadata.Path,
				OldPath:       op.Metadata.OldPath,
				DependsOn:     op.Dependencies,
				SyncFailCount: op.Metadata.SyncFailCount,
				IsNew:         op.Metadata.IsNew,
				CreatedAt:     op.CreatedAt,
				LocalPath:     stagedLocalPathState(stagingRoot, op.Metadata.Path),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(operations, func(i, j int) bool {
		if !operations[i].CreatedAt.Equal(operations[j].CreatedAt) {
			return operations[i].CreatedAt.Before(operations[j].CreatedAt)
		}
		return operations[i].OperationID < operations[j].OperationID
	})
	return operations, nil
}

// stagedLocalPathState reports what the staging area holds at an operation's
// path. Only the data directory is consulted, as the staging area itself does.
func stagedLocalPathState(stagingRoot string, irodsPath string) string {
	localPath := filepath.Join(stagingRoot, "data", filepath.FromSlash(irodsPath))
	info, err := os.Stat(localPath)
	switch {
	case err == nil && info.IsDir():
		return StagedLocalPathDirectory
	case err == nil:
		return StagedLocalPathFile
	case os.IsNotExist(err):
		return StagedLocalPathMissing
	default:
		return StagedLocalPathUnknown
	}
}

// copyDatabaseFiles copies the files of a database directory, leaving out the
// lock file of the process that owned it.
func copyDatabaseFiles(from string, to string) error {
	entries, err := os.ReadDir(from)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == "LOCK" {
			continue
		}
		if err := copyFile(filepath.Join(from, entry.Name()), filepath.Join(to, entry.Name())); err != nil {
			return fmt.Errorf("%s: %w", entry.Name(), err)
		}
	}
	return nil
}

func copyFile(from string, to string) error {
	source, err := os.Open(from)
	if err != nil {
		return err
	}
	defer source.Close()

	target, err := os.OpenFile(to, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(target, source); err != nil {
		target.Close()
		return err
	}
	return target.Close()
}
