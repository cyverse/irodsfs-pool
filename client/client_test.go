package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/irodsfs-pool/service/api"
	"google.golang.org/grpc"
)

type readAtPoolAPIClient struct {
	api.PoolAPIClient
	fileSize       int64
	activeReads    int32
	maxActiveReads int32
	totalReads     int32
	cacheRequests  int32
}

func (client *readAtPoolAPIClient) CacheFile(_ context.Context, _ *api.CacheFileRequest, _ ...grpc.CallOption) (*api.Empty, error) {
	atomic.AddInt32(&client.cacheRequests, 1)
	return &api.Empty{}, nil
}

type writeAtPoolAPIClient struct {
	api.PoolAPIClient
}

func (client *writeAtPoolAPIClient) WriteAt(_ context.Context, request *api.WriteAtRequest, _ ...grpc.CallOption) (*api.WriteAtResponse, error) {
	return &api.WriteAtResponse{Length: int32(len(request.Data))}, nil
}

func TestPoolServiceFileHandleWriteAtPublishesSizeBeforeFlush(t *testing.T) {
	const (
		writeOffset = int64(606208)
		writeLength = 1430
	)

	poolClient := &PoolServiceClient{
		operationTimeout: time.Minute,
		apiClient:        &writeAtPoolAPIClient{},
		fsCache:          NewMetadataCache(time.Minute, time.Minute),
	}
	session := &PoolServiceSession{
		id:                "test-session",
		poolServiceClient: poolClient,
		loggedIn:          true,
	}
	entry := &irodsclient_fs.Entry{Path: "/zone/home/user/tmp_pack", Size: 0}
	poolClient.fsCache.AddEntryCache(entry)
	handle := &PoolServiceFileHandle{
		id:                 "test-handle",
		poolServiceClient:  poolClient,
		poolServiceSession: session,
		entry:              entry,
	}

	n, err := handle.WriteAt(make([]byte, writeLength), writeOffset)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if n != writeLength {
		t.Fatalf("WriteAt length: got %d, want %d", n, writeLength)
	}

	wantSize := writeOffset + writeLength
	cachedEntry := poolClient.fsCache.GetEntryCache(entry.Path)
	if cachedEntry == nil {
		t.Fatal("written entry was not published to metadata cache")
	}
	if cachedEntry.Size != wantSize {
		t.Fatalf("cached size before Flush: got %d, want %d", cachedEntry.Size, wantSize)
	}
}

func TestMetadataCacheStoresEntrySnapshots(t *testing.T) {
	metadataCache := NewMetadataCache(time.Minute, time.Minute)
	entry := &irodsclient_fs.Entry{Path: "/zone/home/user/file", Size: 10}
	metadataCache.AddEntryCache(entry)

	entry.Size = 20
	if got := metadataCache.GetEntryCache(entry.Path).Size; got != 10 {
		t.Fatalf("cached entry changed through source pointer: got %d, want 10", got)
	}

	cachedEntry := metadataCache.GetEntryCache(entry.Path)
	cachedEntry.Size = 30
	if got := metadataCache.GetEntryCache(entry.Path).Size; got != 10 {
		t.Fatalf("cached entry changed through returned pointer: got %d, want 10", got)
	}
}

func (client *readAtPoolAPIClient) ReadAt(_ context.Context, request *api.ReadAtRequest, _ ...grpc.CallOption) (*api.ReadAtResponse, error) {
	atomic.AddInt32(&client.totalReads, 1)
	active := atomic.AddInt32(&client.activeReads, 1)
	for {
		maximum := atomic.LoadInt32(&client.maxActiveReads)
		if active <= maximum || atomic.CompareAndSwapInt32(&client.maxActiveReads, maximum, active) {
			break
		}
	}
	defer atomic.AddInt32(&client.activeReads, -1)

	// Make overlapping cache misses deterministic enough for the race to
	// surface while keeping the test quick.
	time.Sleep(10 * time.Millisecond)
	length := int64(request.Length)
	if remaining := client.fileSize - request.Offset; length > remaining {
		length = remaining
	}
	data := make([]byte, length)
	for i := range data {
		data[i] = byte((request.Offset + int64(i)) % 251)
	}
	return &api.ReadAtResponse{Data: data}, nil
}

// TestPoolServiceFileHandleConcurrentReadAt verifies that concurrent FUSE
// reads on one handle cannot race while updating the double-buffered prefetch
// state. Without handle-level serialization, all callers can observe the
// initially empty cache and overlap synchronous fills of the shared state.
func TestPoolServiceFileHandleConcurrentReadAt(t *testing.T) {
	const (
		fileSize = int64(4 * prefetchBlockSize)
		readSize = 64 * 1024
	)

	apiClient := &readAtPoolAPIClient{fileSize: fileSize}
	poolClient := &PoolServiceClient{
		operationTimeout: time.Minute,
		apiClient:        apiClient,
	}
	session := &PoolServiceSession{
		id:                "test-session",
		poolServiceClient: poolClient,
		loggedIn:          true,
	}
	handle := &PoolServiceFileHandle{
		id:                 "test-handle",
		poolServiceClient:  poolClient,
		poolServiceSession: session,
		entry:              &irodsclient_fs.Entry{Size: fileSize},
		prefetch: &prefetchState{
			buf:     make([]byte, prefetchBlockSize),
			nextBuf: make([]byte, prefetchBlockSize),
		},
	}

	offsets := []int64{0, readSize, 2 * readSize, 3 * readSize}
	start := make(chan struct{})
	errs := make(chan error, len(offsets))
	var wg sync.WaitGroup
	for _, offset := range offsets {
		offset := offset
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			buffer := make([]byte, readSize)
			n, err := handle.ReadAt(buffer, offset)
			if err != nil && err != io.EOF {
				errs <- err
				return
			}
			if n != len(buffer) {
				errs <- io.ErrUnexpectedEOF
				return
			}
			for i, value := range buffer {
				expected := byte((offset + int64(i)) % 251)
				if value != expected {
					errs <- fmt.Errorf("data mismatch at offset %d: got %d, want %d", offset+int64(i), value, expected)
					return
				}
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	if maximum := atomic.LoadInt32(&apiClient.maxActiveReads); maximum != 1 {
		t.Fatalf("server reads overlapped for one file handle: max concurrency = %d", maximum)
	}
}

func TestPoolServiceFileHandleReadAtAcrossPrefetchBoundary(t *testing.T) {
	const (
		fileSize = int64(2 * prefetchBlockSize)
		readSize = 128 * 1024
		offset   = int64(prefetchBlockSize - readSize/2)
	)

	apiClient := &readAtPoolAPIClient{fileSize: fileSize}
	poolClient := &PoolServiceClient{
		operationTimeout: time.Minute,
		apiClient:        apiClient,
	}
	session := &PoolServiceSession{
		id:                "test-session",
		poolServiceClient: poolClient,
		loggedIn:          true,
	}
	handle := &PoolServiceFileHandle{
		id:                 "test-handle",
		poolServiceClient:  poolClient,
		poolServiceSession: session,
		entry:              &irodsclient_fs.Entry{Size: fileSize},
		prefetch: &prefetchState{
			buf:     make([]byte, prefetchBlockSize),
			nextBuf: make([]byte, prefetchBlockSize),
		},
	}

	buffer := make([]byte, readSize)
	n, err := handle.ReadAt(buffer, offset)
	if err != nil {
		t.Fatalf("ReadAt returned an internal block boundary as EOF: n=%d, err=%v", n, err)
	}
	if n != len(buffer) {
		t.Fatalf("short read across prefetch boundary: got %d, want %d", n, len(buffer))
	}
	for i, value := range buffer {
		expected := byte((offset + int64(i)) % 251)
		if value != expected {
			t.Fatalf("data mismatch at offset %d: got %d, want %d", offset+int64(i), value, expected)
		}
	}
}

func TestPoolServiceFileHandleDoesNotRestartReadyPrefetch(t *testing.T) {
	const fileSize = int64(2 * prefetchBlockSize)

	apiClient := &readAtPoolAPIClient{fileSize: fileSize}
	poolClient := &PoolServiceClient{
		operationTimeout: time.Minute,
		apiClient:        apiClient,
	}
	session := &PoolServiceSession{
		id:                "test-session",
		poolServiceClient: poolClient,
		loggedIn:          true,
	}
	handle := &PoolServiceFileHandle{
		id:                 "test-handle",
		poolServiceClient:  poolClient,
		poolServiceSession: session,
		entry:              &irodsclient_fs.Entry{Size: fileSize},
		prefetch: &prefetchState{
			buf:     make([]byte, prefetchBlockSize),
			nextBuf: make([]byte, prefetchBlockSize),
		},
	}

	buffer := make([]byte, 128*1024)
	if _, err := handle.ReadAt(buffer, int64(prefetchBlockSize/2)); err != nil {
		t.Fatalf("initial read failed: %v", err)
	}

	handle.prefetch.mu.Lock()
	ready := handle.prefetch.nextReady
	handle.prefetch.mu.Unlock()
	if ready == nil {
		t.Fatal("initial read did not start prefetch")
	}
	<-ready

	// These reads remain in the current block after the next block is ready.
	// They must not repeatedly overwrite the already prepared next buffer.
	for _, offset := range []int64{
		int64(5 * prefetchBlockSize / 8),
		int64(6 * prefetchBlockSize / 8),
		int64(7 * prefetchBlockSize / 8),
	} {
		if _, err := handle.ReadAt(buffer, offset); err != nil {
			t.Fatalf("read at offset %d failed: %v", offset, err)
		}
	}

	const rpcCallsPerBlock = prefetchBlockSize / fileRWLengthMax
	wantCalls := int32(2 * rpcCallsPerBlock)
	if calls := atomic.LoadInt32(&apiClient.totalReads); calls != wantCalls {
		t.Fatalf("ready block was prefetched again: got %d RPC reads, want %d", calls, wantCalls)
	}
}

func TestPoolServiceFileHandleCachesAfterReadThreshold(t *testing.T) {
	const fileSize = prefetchCacheThreshold + int64(prefetchBlockSize)

	apiClient := &readAtPoolAPIClient{fileSize: fileSize}
	poolClient := &PoolServiceClient{
		operationTimeout: time.Minute,
		apiClient:        apiClient,
	}
	session := &PoolServiceSession{
		id:                "test-session",
		poolServiceClient: poolClient,
		loggedIn:          true,
	}
	handle := &PoolServiceFileHandle{
		id:                 "test-handle",
		poolServiceClient:  poolClient,
		poolServiceSession: session,
		entry:              &irodsclient_fs.Entry{Path: "/zone/home/user/file", Size: fileSize},
		prefetch: &prefetchState{
			buf:     make([]byte, prefetchBlockSize),
			nextBuf: make([]byte, prefetchBlockSize),
		},
	}

	buffer := make([]byte, prefetchCacheThreshold)
	if n, err := handle.ReadAt(buffer, 0); err != nil || n != len(buffer) {
		t.Fatalf("read through threshold: n=%d, err=%v", n, err)
	}
	if requests := atomic.LoadInt32(&apiClient.cacheRequests); requests != 0 {
		t.Fatalf("cache requested at threshold: got %d requests, want 0", requests)
	}

	if n, err := handle.ReadAt(buffer[:1], prefetchCacheThreshold); err != nil || n != 1 {
		t.Fatalf("read beyond threshold: n=%d, err=%v", n, err)
	}
	if requests := atomic.LoadInt32(&apiClient.cacheRequests); requests != 1 {
		t.Fatalf("cache requests after threshold: got %d, want 1", requests)
	}

	handle.prefetch.mu.Lock()
	disabled := handle.prefetch.disabled
	bytesRead := handle.prefetch.bytesRead
	buf := handle.prefetch.buf
	nextBuf := handle.prefetch.nextBuf
	handle.prefetch.mu.Unlock()
	if !disabled {
		t.Fatal("prefetch remained enabled after crossing read threshold")
	}
	if bytesRead != prefetchCacheThreshold+1 {
		t.Fatalf("tracked read bytes: got %d, want %d", bytesRead, prefetchCacheThreshold+1)
	}
	if buf != nil || nextBuf != nil {
		t.Fatal("prefetch buffers were retained after disabling prefetch")
	}

	if n, err := handle.ReadAt(buffer[:1], prefetchCacheThreshold+1); err != nil || n != 1 {
		t.Fatalf("read after disabling prefetch: n=%d, err=%v", n, err)
	}
	if requests := atomic.LoadInt32(&apiClient.cacheRequests); requests != 1 {
		t.Fatalf("cache was requested more than once: got %d requests, want 1", requests)
	}
}
