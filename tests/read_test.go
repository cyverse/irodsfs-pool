package tests

import (
	"crypto/rand"
	"flag"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/config"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-pool/client"
	log "github.com/sirupsen/logrus"
)

const (
	defaultTestFilePath = "/iplant/home/iychoi/test_70MB.bin"
	defaultBlockSize    = 4 * 1024 * 1024 // 4MB
	defaultPoolAddress  = ":12020"
)

var (
	accountFile string
	testFile    string
	poolAddr    string
	blkSize     int
)

func TestMain(m *testing.M) {
	flag.StringVar(&accountFile, "account", "account.yml", "Path to account YAML file")
	flag.StringVar(&testFile, "file", defaultTestFilePath, "iRODS path to test file")
	flag.StringVar(&poolAddr, "pool", defaultPoolAddress, "Pool server address")
	flag.IntVar(&blkSize, "blocksize", defaultBlockSize, "Block size in bytes")
	flag.Parse()

	os.Exit(m.Run())
}

func setupSession(t *testing.T) (irodsfs_common_irods.IRODSFSClient, func()) {
	t.Helper()

	logger := log.WithFields(log.Fields{})

	cfg, err := config.NewConfigFromYAMLFile(config.GetDefaultConfig(), accountFile)
	if err != nil {
		t.Fatalf("failed to read account config %q: %v", accountFile, err)
	}

	account := cfg.ToIRODSAccount()

	poolClient := client.NewPoolServiceClient(poolAddr, 5*time.Minute, "read_test", logger)
	if err := poolClient.Connect(); err != nil {
		t.Fatalf("failed to connect to pool server at %q: %v", poolAddr, err)
	}

	session, err := poolClient.NewSession(account, "read_test")
	if err != nil {
		poolClient.Disconnect()
		t.Fatalf("failed to create session: %v", err)
	}

	cleanup := func() {
		session.Release()
		poolClient.Disconnect()
	}

	return session, cleanup
}

// TestRandomRead opens the file on the pool server and reads the entire file
// using random offsets and random sizes that cross block boundaries.
// Verifies that all bytes are read and consistent across reads.
func TestRandomRead(t *testing.T) {
	session, cleanup := setupSession(t)
	defer cleanup()

	entry, err := session.Stat(testFile)
	if err != nil {
		t.Fatalf("failed to stat %q: %v", testFile, err)
	}
	fileSize := entry.Size
	if fileSize == 0 {
		t.Fatalf("test file %q is empty", testFile)
	}
	t.Logf("file: %s, size: %d bytes", testFile, fileSize)

	// Read entire file to get reference data
	refHandle, err := session.OpenFile(testFile, "r")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	referenceData := readEntireFile(t, refHandle, fileSize)
	refHandle.Close()

	// Open a fresh handle for the random read test
	handle, err := session.OpenFile(testFile, "r")
	if err != nil {
		t.Fatalf("failed to reopen file: %v", err)
	}

	// Random read pass with full coverage
	coverage := make([]bool, fileSize)
	readRandomCoverage(t, handle, referenceData, coverage, fileSize)
	handle.Close()

	uncovered := countUncovered(coverage)
	if uncovered > 0 {
		t.Fatalf("incomplete coverage: %d bytes not read out of %d", uncovered, fileSize)
	}
	t.Logf("full coverage verified: %d bytes", fileSize)
}

// TestRandomReadCached opens the file and performs two full random-read passes.
// The first pass populates the server-side cache; the second pass reads from cache.
// Both passes verify data integrity.
func TestRandomReadCached(t *testing.T) {
	session, cleanup := setupSession(t)
	defer cleanup()

	entry, err := session.Stat(testFile)
	if err != nil {
		t.Fatalf("failed to stat %q: %v", testFile, err)
	}
	fileSize := entry.Size
	if fileSize == 0 {
		t.Fatalf("test file %q is empty", testFile)
	}
	t.Logf("file: %s, size: %d bytes", testFile, fileSize)

	// Read entire file to get reference data
	refHandle, err := session.OpenFile(testFile, "r")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	referenceData := readEntireFile(t, refHandle, fileSize)
	refHandle.Close()

	// Pass 1: populate cache
	t.Log("pass 1: populating cache")
	handle1, err := session.OpenFile(testFile, "r")
	if err != nil {
		t.Fatalf("failed to open file for pass 1: %v", err)
	}
	coverage1 := make([]bool, fileSize)
	readRandomCoverage(t, handle1, referenceData, coverage1, fileSize)
	handle1.Close()
	if uncovered := countUncovered(coverage1); uncovered > 0 {
		t.Fatalf("pass 1 incomplete: %d bytes not read", uncovered)
	}

	// Pass 2: read from cache
	t.Log("pass 2: reading from cache")
	handle2, err := session.OpenFile(testFile, "r")
	if err != nil {
		t.Fatalf("failed to open file for pass 2: %v", err)
	}
	coverage2 := make([]bool, fileSize)
	readRandomCoverage(t, handle2, referenceData, coverage2, fileSize)
	handle2.Close()
	if uncovered := countUncovered(coverage2); uncovered > 0 {
		t.Fatalf("pass 2 incomplete: %d bytes not read", uncovered)
	}

	t.Logf("cache test verified: 2 full passes over %d bytes", fileSize)
}

func readEntireFile(t *testing.T, handle irodsfs_common_irods.IRODSFSFileHandle, fileSize int64) []byte {
	t.Helper()
	data := make([]byte, fileSize)
	offset := int64(0)
	buf := make([]byte, blkSize)

	for offset < fileSize {
		readLen := int64(len(buf))
		if offset+readLen > fileSize {
			readLen = fileSize - offset
		}
		n, err := handle.ReadAt(buf[:readLen], offset)
		if err != nil && n == 0 {
			t.Fatalf("readEntireFile: ReadAt(offset=%d) failed: %v", offset, err)
		}
		copy(data[offset:offset+int64(n)], buf[:n])
		offset += int64(n)
	}
	t.Logf("reference data read: %d bytes", fileSize)
	return data
}

// readRandomCoverage reads the file using random offsets and random sizes that
// cross block boundaries, then fills any gaps.
func readRandomCoverage(t *testing.T, handle irodsfs_common_irods.IRODSFSFileHandle, referenceData []byte, coverage []bool, fileSize int64) {
	t.Helper()

	totalRead := int64(0)

	// Phase 1: random reads
	numReads := int(fileSize/int64(blkSize)*4) + 16
	for i := 0; i < numReads; i++ {
		offset := randInt64(fileSize)

		remaining := fileSize - offset
		maxRead := int64(blkSize) + int64(blkSize)/2
		if remaining < maxRead {
			maxRead = remaining
		}
		if maxRead <= 0 {
			continue
		}

		// Random size biased toward crossing block boundaries
		minRead := int64(1)
		quarter := int64(blkSize) / 4
		if quarter > 0 && maxRead > quarter {
			minRead = quarter
		}
		readSize := int(minRead + randInt64(maxRead-minRead+1))

		buf := make([]byte, readSize)
		n, err := handle.ReadAt(buf, offset)
		if err != nil && n == 0 {
			t.Fatalf("ReadAt(offset=%d, size=%d) failed: %v", offset, readSize, err)
		}

		verifyAndMark(t, buf[:n], offset, referenceData, coverage)
		totalRead += int64(n)
	}

	// Phase 2: fill uncovered gaps
	gapStart := int64(-1)
	for i := int64(0); i <= fileSize; i++ {
		if i < fileSize && !coverage[i] {
			if gapStart < 0 {
				gapStart = i
			}
		} else if gapStart >= 0 {
			readOffset := gapStart
			if gapStart > 0 {
				shift := int64(blkSize) / 8
				if shift > gapStart {
					shift = gapStart
				}
				if shift > 0 {
					readOffset = gapStart - randInt64(shift)
				}
			}

			gapEnd := i
			readEnd := gapEnd + int64(blkSize)/8
			if readEnd > fileSize {
				readEnd = fileSize
			}

			readLen := readEnd - readOffset
			buf := make([]byte, readLen)
			n, err := handle.ReadAt(buf, readOffset)
			if err != nil && n == 0 {
				t.Fatalf("gap fill ReadAt(offset=%d, size=%d) failed: %v", readOffset, readLen, err)
			}

			verifyAndMark(t, buf[:n], readOffset, referenceData, coverage)
			totalRead += int64(n)
			gapStart = -1
		}
	}

	t.Logf("total bytes read: %s (file size: %s, ratio: %.2fx)",
		formatSize(totalRead), formatSize(fileSize), float64(totalRead)/float64(fileSize))
}

func verifyAndMark(t *testing.T, data []byte, offset int64, referenceData []byte, coverage []bool) {
	t.Helper()
	for i := 0; i < len(data); i++ {
		pos := offset + int64(i)
		if data[i] != referenceData[pos] {
			t.Fatalf("data mismatch at byte %d: got 0x%02x, want 0x%02x", pos, data[i], referenceData[pos])
		}
		coverage[pos] = true
	}
}

func countUncovered(coverage []bool) int64 {
	var count int64
	for _, c := range coverage {
		if !c {
			count++
		}
	}
	return count
}

func randInt64(max int64) int64 {
	if max <= 0 {
		return 0
	}
	n, _ := rand.Int(rand.Reader, big.NewInt(max))
	return n.Int64()
}

func formatSize(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
