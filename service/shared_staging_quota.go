package service

import (
	"sync"

	"github.com/cockroachdb/errors"
	irodsfs_common_stagingfs "github.com/cyverse/irodsfs-common/irods/stagingfs"
)

// SharedStagingQuota enforces a single staging disk limit across all sessions.
// It implements stagingfs.GlobalQuota.
type SharedStagingQuota struct {
	mu      sync.Mutex
	used    int64
	maxSize int64
}

// NewSharedStagingQuota creates a new SharedStagingQuota with the given limit.
func NewSharedStagingQuota(maxSize int64) *SharedStagingQuota {
	return &SharedStagingQuota{
		maxSize: maxSize,
	}
}

func (q *SharedStagingQuota) ReserveSpace(size int64) error {
	if size <= 0 {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.used+size > q.maxSize {
		return errors.Mark(
			errors.Newf("global staging quota exceeded: used %d + requested %d > max %d", q.used, size, q.maxSize),
			irodsfs_common_stagingfs.ErrQuotaExceeded,
		)
	}
	q.used += size
	return nil
}

func (q *SharedStagingQuota) ReleaseSpace(size int64) {
	if size <= 0 {
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	q.used -= size
	if q.used < 0 {
		q.used = 0
	}
}

// GetUsed returns the current total usage tracked by this quota.
func (q *SharedStagingQuota) GetUsed() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used
}

// GetMaxSize returns the configured maximum.
func (q *SharedStagingQuota) GetMaxSize() int64 {
	return q.maxSize
}
