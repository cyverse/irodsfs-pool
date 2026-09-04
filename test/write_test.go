package tests

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"path"
	"testing"
	"time"

	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
)

const (
	defaultTestWriteDir = "/iplant/home/iychoi"
)

var (
	writeDir string
)

func init() {
	writeDir = defaultTestWriteDir
}

// TestWrite creates a file with various write patterns (tiny, block-boundary,
// large, unaligned), then verifies data integrity by reading back immediately
// and again after a 20-second delay.
func TestWrite(t *testing.T) {
	session, cleanup := setupSession(t)
	defer cleanup()

	testFileName := fmt.Sprintf("write_test_%d.bin", time.Now().UnixNano())
	testPath := path.Join(writeDir, testFileName)
	t.Logf("test file: %s", testPath)

	// Generate write segments with diverse sizes
	type segment struct {
		name   string
		offset int64
		data   []byte
	}

	blockSize := int64(blkSize)

	segments := []segment{
		{"tiny_1byte", 0, randBytes(1)},
		{"tiny_7bytes", 1, randBytes(7)},
		{"small_100bytes", 8, randBytes(100)},
		{"sub_block_half", 108, randBytes(int(blockSize / 2))},
		{"exact_one_block", blockSize, randBytes(int(blockSize))},
		{"cross_boundary", blockSize + blockSize/3, randBytes(int(blockSize))},
		{"two_blocks", blockSize * 3, randBytes(int(blockSize * 2))},
		{"large_3_5_blocks", blockSize * 5, randBytes(int(blockSize*3 + blockSize/2))},
		{"unaligned_end", blockSize*8 + 999, randBytes(12345)},
	}

	// Calculate total file size and build expected content
	var totalSize int64
	for _, seg := range segments {
		end := seg.offset + int64(len(seg.data))
		if end > totalSize {
			totalSize = end
		}
	}

	expected := make([]byte, totalSize)
	for _, seg := range segments {
		copy(expected[seg.offset:], seg.data)
	}

	expectedHash := sha256.Sum256(expected)
	t.Logf("expected file size: %d bytes, sha256: %x", totalSize, expectedHash)

	// Create file and write all segments
	handle, err := session.CreateFile(testPath, "w")
	if err != nil {
		t.Fatalf("failed to create file %q: %v", testPath, err)
	}

	for _, seg := range segments {
		n, err := handle.WriteAt(seg.data, seg.offset)
		if err != nil {
			handle.Close()
			session.RemoveFile(testPath, true)
			t.Fatalf("WriteAt %q (offset=%d, size=%d) failed: %v", seg.name, seg.offset, len(seg.data), err)
		}
		if n != len(seg.data) {
			handle.Close()
			session.RemoveFile(testPath, true)
			t.Fatalf("WriteAt %q: short write %d/%d", seg.name, n, len(seg.data))
		}
		t.Logf("wrote segment %q: offset=%d, size=%d", seg.name, seg.offset, len(seg.data))
	}

	err = handle.Close()
	if err != nil {
		session.RemoveFile(testPath, true)
		t.Fatalf("failed to close write handle: %v", err)
	}

	// Cleanup file at end of test
	defer func() {
		session.RemoveFile(testPath, true)
	}()

	// Read back immediately and verify hash
	t.Log("pass 1: immediate read-back verification")
	verifyFileHash(t, session, testPath, totalSize, expectedHash)

	// Sync staged files
	err = session.Sync()
	if err != nil {
		t.Fatalf("failed to sync: %v", err)
	}

	// Wait 3 seconds and read back again
	t.Log("waiting 3 seconds before second read...")
	time.Sleep(3 * time.Second)

	t.Log("pass 2: read-back after sync")
	verifyFileHash(t, session, testPath, totalSize, expectedHash)

	t.Logf("write test passed: %d bytes, %d segments, hash verified twice", totalSize, len(segments))
}

// TestWriteOverwrite writes a file, then overwrites parts of it and verifies.
func TestWriteOverwrite(t *testing.T) {
	session, cleanup := setupSession(t)
	defer cleanup()

	testFileName := fmt.Sprintf("write_overwrite_test_%d.bin", time.Now().UnixNano())
	testPath := path.Join(writeDir, testFileName)
	t.Logf("test file: %s", testPath)

	blockSize := int64(blkSize)
	fileSize := blockSize * 4

	// Write initial data
	initialData := randBytes(int(fileSize))
	handle, err := session.CreateFile(testPath, "w")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	n, err := handle.WriteAt(initialData, 0)
	if err != nil || n != len(initialData) {
		handle.Close()
		session.RemoveFile(testPath, true)
		t.Fatalf("initial write failed: n=%d, err=%v", n, err)
	}
	handle.Close()

	defer func() {
		session.RemoveFile(testPath, true)
	}()

	// Overwrite specific regions
	type overwrite struct {
		name   string
		offset int64
		data   []byte
	}

	overwrites := []overwrite{
		{"start_tiny", 0, randBytes(16)},
		{"mid_block_cross", blockSize - 100, randBytes(200)},
		{"block_aligned", blockSize * 2, randBytes(int(blockSize))},
		{"end_region", fileSize - 1000, randBytes(1000)},
	}

	// Apply overwrites to expected data
	expected := make([]byte, fileSize)
	copy(expected, initialData)
	for _, ow := range overwrites {
		copy(expected[ow.offset:], ow.data)
	}
	expectedHash := sha256.Sum256(expected)

	// Open for write and apply overwrites
	wHandle, err := session.OpenFile(testPath, "w")
	if err != nil {
		t.Fatalf("failed to open file for overwrite: %v", err)
	}

	for _, ow := range overwrites {
		n, err := wHandle.WriteAt(ow.data, ow.offset)
		if err != nil || n != len(ow.data) {
			wHandle.Close()
			t.Fatalf("overwrite %q failed: n=%d, err=%v", ow.name, n, err)
		}
		t.Logf("overwrote %q: offset=%d, size=%d", ow.name, ow.offset, len(ow.data))
	}
	wHandle.Close()

	// Verify immediately
	t.Log("pass 1: immediate read-back after overwrite")
	verifyFileHash(t, session, testPath, fileSize, expectedHash)

	// Sync staged files
	err = session.Sync()
	if err != nil {
		t.Fatalf("failed to sync: %v", err)
	}

	// Wait 3 seconds and read back again
	t.Log("waiting 3 seconds before second read...")
	time.Sleep(3 * time.Second)

	t.Log("pass 2: read-back after sync")
	verifyFileHash(t, session, testPath, fileSize, expectedHash)

	t.Logf("overwrite test passed: %d bytes, %d overwrites", fileSize, len(overwrites))
}

func verifyFileHash(t *testing.T, session irodsfs_common_irods.IRODSFSClient, filePath string, expectedSize int64, expectedHash [32]byte) {
	t.Helper()

	entry, err := session.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat %q: %v", filePath, err)
	}
	if entry.Size != expectedSize {
		t.Fatalf("file size mismatch: got %d, want %d", entry.Size, expectedSize)
	}

	handle, err := session.OpenFile(filePath, "r")
	if err != nil {
		t.Fatalf("failed to open file for read: %v", err)
	}
	defer handle.Close()

	data := make([]byte, expectedSize)
	offset := int64(0)
	buf := make([]byte, blkSize)

	for offset < expectedSize {
		readLen := int64(len(buf))
		if offset+readLen > expectedSize {
			readLen = expectedSize - offset
		}
		n, err := handle.ReadAt(buf[:readLen], offset)
		if err != nil && n == 0 {
			t.Fatalf("ReadAt(offset=%d) failed: %v", offset, err)
		}
		copy(data[offset:offset+int64(n)], buf[:n])
		offset += int64(n)
	}

	actualHash := sha256.Sum256(data)
	if actualHash != expectedHash {
		t.Fatalf("hash mismatch: got %x, want %x", actualHash, expectedHash)
	}
	t.Logf("hash verified: %x (%d bytes)", actualHash, expectedSize)
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("rand.Read failed: %v", err))
	}
	return b
}
