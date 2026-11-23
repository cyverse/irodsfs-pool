package service

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	irodsfs_common_stagingfs "github.com/cyverse/irodsfs-common/irods/stagingfs"
	"github.com/dgraph-io/badger/v3"
)

func writeStagedOperations(t *testing.T, stagingRoot string, operations ...*irodsfs_common_stagingfs.StagingOperation) {
	t.Helper()

	metaPath := filepath.Join(stagingRoot, "meta")
	if err := os.MkdirAll(metaPath, 0755); err != nil {
		t.Fatal(err)
	}
	db, err := badger.Open(badger.DefaultOptions(metaPath).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(txn *badger.Txn) error {
		for _, op := range operations {
			data, err := json.Marshal(op)
			if err != nil {
				return err
			}
			if err := txn.Set([]byte(stagedOperationKeyPrefix+op.ID), data); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// The listing reports what the staging area holds behind each operation, so an
// upload whose file was replaced by a directory can be told from a good one.
func TestReadStagedOperationsReportsLocalPathState(t *testing.T) {
	stagingRoot := t.TempDir()
	dataDir := filepath.Join(stagingRoot, "data", "zone", "home")
	if err := os.MkdirAll(filepath.Join(dataDir, "badreads.txt"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "good.txt"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	writeStagedOperations(t, stagingRoot,
		&irodsfs_common_stagingfs.StagingOperation{
			ID: "op-1", CreatedAt: now, State: irodsfs_common_stagingfs.OperationFailed,
			Metadata: &irodsfs_common_stagingfs.StagingMetadata{Path: "/zone/home/badreads.txt", Action: irodsfs_common_stagingfs.ActionUpload, SyncFailCount: 3, LastSyncError: "permission denied"},
		},
		&irodsfs_common_stagingfs.StagingOperation{
			ID: "op-2", CreatedAt: now.Add(time.Second), Dependencies: []string{"op-1"},
			Metadata: &irodsfs_common_stagingfs.StagingMetadata{Path: "/zone/home/good.txt", Action: irodsfs_common_stagingfs.ActionUpload},
		},
		&irodsfs_common_stagingfs.StagingOperation{
			ID: "op-3", CreatedAt: now.Add(2 * time.Second),
			Metadata: &irodsfs_common_stagingfs.StagingMetadata{Path: "/zone/home/gone.txt", Action: irodsfs_common_stagingfs.ActionUpload},
		},
	)

	operations, err := readStagedOperations(stagingRoot)
	if err != nil {
		t.Fatalf("readStagedOperations: %v", err)
	}
	if len(operations) != 3 {
		t.Fatalf("got %d operations, want 3: %#v", len(operations), operations)
	}

	want := []struct{ id, local string }{
		{"op-1", StagedLocalPathDirectory},
		{"op-2", StagedLocalPathFile},
		{"op-3", StagedLocalPathMissing},
	}
	for i, w := range want {
		if operations[i].OperationID != w.id || operations[i].LocalPath != w.local {
			t.Fatalf("operations[%d] = %s/%s, want %s/%s", i, operations[i].OperationID, operations[i].LocalPath, w.id, w.local)
		}
	}
	if operations[0].LastSyncError != "permission denied" || !operations[0].SyncFailed() || operations[1].SyncFailed() {
		t.Fatalf("failure details lost: %#v", operations[:2])
	}
	if operations[0].SyncFailCount != 3 || operations[0].Action != "UPLOAD" || len(operations[1].DependsOn) != 1 {
		t.Fatalf("operation details lost: %#v", operations[:2])
	}
}

// Reading must leave the session's own database as it was, because the recovery
// that follows works from it.
func TestReadStagedOperationsDoesNotChangeTheDatabase(t *testing.T) {
	stagingRoot := t.TempDir()
	writeStagedOperations(t, stagingRoot, &irodsfs_common_stagingfs.StagingOperation{
		ID: "op-1", Metadata: &irodsfs_common_stagingfs.StagingMetadata{Path: "/a", Action: irodsfs_common_stagingfs.ActionMkdir},
	})

	snapshot := func() map[string]int64 {
		sizes := map[string]int64{}
		entries, err := os.ReadDir(filepath.Join(stagingRoot, "meta"))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			info, err := e.Info()
			if err != nil {
				t.Fatal(err)
			}
			sizes[e.Name()] = info.Size()
		}
		return sizes
	}
	before := snapshot()

	if _, err := readStagedOperations(stagingRoot); err != nil {
		t.Fatal(err)
	}

	after := snapshot()
	if len(before) != len(after) {
		t.Fatalf("files before %v, after %v", before, after)
	}
	for name, size := range before {
		if after[name] != size {
			t.Fatalf("%s changed from %d to %d bytes", name, size, after[name])
		}
	}
}

func TestReadStagedOperationsWithoutDatabase(t *testing.T) {
	operations, err := readStagedOperations(t.TempDir())
	if err != nil || len(operations) != 0 {
		t.Fatalf("operations = %v, err = %v, want none", operations, err)
	}
}

// The reason an operation failed is shown with it, and it is text from iRODS and
// from users, so it is escaped.
func TestRenderStagedOperationsShowsFailureReasonEscaped(t *testing.T) {
	var buffer bytes.Buffer
	renderStagedOperationsTable(&buffer, []StagedOperationInfo{{
		OperationID:   "op-1",
		Action:        "UPLOAD",
		State:         "FAILED",
		Path:          "/z/home/u/<img src=x onerror=alert(1)>.dat",
		SyncFailCount: 2,
		LastSyncError: "failed to upload <script>alert(2)</script>",
		LocalPath:     StagedLocalPathDirectory,
	}})

	body := buffer.String()
	for _, injected := range []string{"<img src=x", "<script>"} {
		if strings.Contains(body, injected) {
			t.Fatalf("operations table renders %q as markup", injected)
		}
	}
	for _, expected := range []string{"&lt;script&gt;alert(2)&lt;/script&gt;", "&lt;img src=x", `class="dirty"`, "directory"} {
		if !strings.Contains(body, expected) {
			t.Fatalf("operations table does not contain %q: %s", expected, body)
		}
	}
}
