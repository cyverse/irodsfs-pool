package io

import (
	"fmt"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
)

// NilWriter does nothing for write
type NilWriter struct {
	path       string
	fileHandle *irodsfs.FileHandle
}

// NewNilWriter create a new NilWriter
func NewNilWriter(path string, fileHandle *irodsfs.FileHandle) *NilWriter {
	nilWriter := &NilWriter{
		path:       path,
		fileHandle: fileHandle,
	}

	return nilWriter
}

// Release releases all resources
func (writer *NilWriter) Release() {
}

// WriteAt writes data
func (writer *NilWriter) WriteAt(offset int64, data []byte) error {
	return fmt.Errorf("failed to write data using NilWriter - %s, offset %d, length %d", writer.path, offset, len(data))
}

func (writer *NilWriter) Flush() error {
	return nil
}

func (writer *NilWriter) GetPendingError() error {
	return nil
}
