package service

import (
	"fmt"
	"sync"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-pool/service/io"
)

type FileHandle struct {
	id           string
	sessionID    string
	connectionID string

	writer          io.Writer
	reader          io.Reader
	irodsFileHandle *irodsclient_fs.FileHandle
	mutex           *sync.Mutex // mutex to access writer, reader, irodsFileHandle
}

func NewFileHandle(handleID string, sessionID string, connectionID string, writer io.Writer, reader io.Reader, fileHandle *irodsclient_fs.FileHandle, mutex *sync.Mutex) *FileHandle {
	return &FileHandle{
		id:           handleID,
		sessionID:    sessionID,
		connectionID: connectionID,

		writer:          writer,
		reader:          reader,
		irodsFileHandle: fileHandle,
		mutex:           mutex,
	}
}

func (handle *FileHandle) Release() error {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	errs := []error{}

	if handle.writer != nil {
		err := handle.writer.Flush()
		if err != nil {
			errs = append(errs, err)
		}

		handle.writer.Release()
		handle.writer = nil
	}

	if handle.reader != nil {
		err := handle.reader.GetPendingError()
		if err != nil {
			errs = append(errs, err)
		}

		handle.reader.Release()
		handle.reader = nil
	}

	if handle.irodsFileHandle != nil {
		err := handle.irodsFileHandle.Close()
		if err != nil {
			errs = append(errs, err)
		}

		handle.irodsFileHandle = nil
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (handle *FileHandle) GetID() string {
	return handle.id
}

func (handle *FileHandle) Flush() error {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	if handle.writer != nil {
		return handle.writer.Flush()
	}
	return nil
}

func (handle *FileHandle) GetFileOpenMode() types.FileOpenMode {
	return handle.irodsFileHandle.OpenMode
}

func (handle *FileHandle) GetEntryPath() string {
	return handle.irodsFileHandle.Entry.Path
}

func (handle *FileHandle) GetOffset() int64 {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	if handle.writer != nil {
		handle.writer.Flush()
	}

	return handle.irodsFileHandle.GetOffset()
}

func (handle *FileHandle) ReadAt(offset int64, len int) ([]byte, error) {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	if handle.reader != nil {
		return handle.reader.ReadAt(offset, len)
	}
	return nil, fmt.Errorf("reader is not initialized")
}

func (handle *FileHandle) WriteAt(offset int64, data []byte) error {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	if handle.writer != nil {
		return handle.writer.WriteAt(offset, data)
	}
	return fmt.Errorf("writer is not initialized")
}
