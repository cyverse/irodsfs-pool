package io

import (
	"fmt"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
)

// NilReader does nothing for read
type NilReader struct {
	path       string
	fileHandle *irodsfs.FileHandle
}

// NewNilReader create a new NilReader
func NewNilReader(path string, fileHandle *irodsfs.FileHandle) *NilReader {
	nilReader := &NilReader{
		path:       path,
		fileHandle: fileHandle,
	}

	return nilReader
}

// Release releases all resources
func (reader *NilReader) Release() {
}

// ReadAt reads data
func (reader *NilReader) ReadAt(offset int64, length int) ([]byte, error) {
	return nil, fmt.Errorf("failed to read data using NilReader - %s, offset %d, length %d", reader.path, offset, length)
}

func (reader *NilReader) GetPendingError() error {
	return nil
}
