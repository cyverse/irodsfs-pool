package service

import "fmt"

// FileHandleNotFoundError ...
type FileHandleNotFoundError struct {
	message string
}

// NewFileHandleNotFoundError creates FileHandleNotFoundError struct
func NewFileHandleNotFoundError(message string) *FileHandleNotFoundError {
	return &FileHandleNotFoundError{
		message: message,
	}
}

// NewFileHandleNotFoundErrorf creates FileHandleNotFoundError struct
func NewFileHandleNotFoundErrorf(format string, v ...interface{}) *FileHandleNotFoundError {
	return &FileHandleNotFoundError{
		message: fmt.Sprintf(format, v...),
	}
}

func (e *FileHandleNotFoundError) Error() string {
	return e.message
}

// IsFileHandleNotFoundError evaluates if the given error is FileHandleNotFoundError
func IsFileHandleNotFoundError(err error) bool {
	if _, ok := err.(*FileHandleNotFoundError); ok {
		return true
	}

	return false
}
