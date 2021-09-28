package io

import (
	"fmt"

	"github.com/cyverse/irodsfs-pool/utils"
	log "github.com/sirupsen/logrus"
)

// CacheReader helps read through cache
type CacheReader struct {
	Path string

	Cache       Cache
	Reader      Reader
	BlockHelper *utils.FileBlockHelper
}

const (
	BlockSize int = 1024 * 1024 // 1MB
)

// NewCacheReader create a new CacheReader
func NewCacheReader(path string, cache Cache, reader Reader) *CacheReader {
	cacheReader := &CacheReader{
		Path: path,

		Cache:       cache,
		Reader:      reader,
		BlockHelper: utils.NewFileBlockHelper(BlockSize),
	}

	return cacheReader
}

// Release releases all resources
func (reader *CacheReader) Release() {
	if reader.Reader != nil {
		reader.Reader.Release()
		reader.Reader = nil
	}
}

func (reader *CacheReader) getCacheEntryKey(blockID int64) string {
	return fmt.Sprintf("%s:%d", reader.Path, blockID)
}

func (reader *CacheReader) getBlockIDs(offset int64, length int) []int64 {
	first, last := reader.BlockHelper.GetFirstAndLastBlockIDForRW(offset, length)

	ids := []int64{}
	for i := first; i <= last; i++ {
		ids = append(ids, i)
	}
	return ids
}

// ReadAt reads data
func (reader *CacheReader) ReadAt(offset int64, length int) ([]byte, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "CacheReader",
		"function": "ReadAt",
	})

	if length <= 0 || offset < 0 {
		return []byte{}, nil
	}

	logger.Infof("Cache-through Reading - %s, offset %d, length %d", reader.Path, offset, length)

	blockIDs := reader.getBlockIDs(offset, length)
	dataRead := 0
	readBuffer := make([]byte, length)
	for _, blockID := range blockIDs {
		blockKey := reader.getCacheEntryKey(blockID)
		cacheEntry := reader.Cache.GetEntry(blockKey)
		var cacheData []byte
		if cacheEntry == nil {
			blockOffset := reader.BlockHelper.GetBlockStartOffsetForBlockID(blockID)
			blockData, err := reader.Reader.ReadAt(blockOffset, BlockSize)
			if err != nil {
				return nil, err
			}

			if len(blockData) == 0 {
				// EOF?
				break
			}

			cacheData = blockData
			reader.Cache.CreateEntry(blockKey, blockData) // deliverately ignore result
		} else {
			cacheEntryData, err := cacheEntry.GetData()
			if err != nil {
				return nil, err
			}

			cacheData = cacheEntryData
		}

		inBlockOffset, _ := reader.BlockHelper.GetInBlockOffsetAndLength(offset+int64(dataRead), length-dataRead)
		inBlockLength := length - dataRead
		if inBlockLength > (len(cacheData) - inBlockOffset) {
			inBlockLength = len(cacheData) - inBlockOffset
		}

		copy(readBuffer[dataRead:], cacheData[inBlockOffset:inBlockOffset+inBlockLength])
		dataRead += inBlockLength

		if len(cacheData) != BlockSize {
			// EOF
			break
		}
	}

	return readBuffer[:dataRead], nil
}

func (reader *CacheReader) GetPendingError() error {
	return nil
}
