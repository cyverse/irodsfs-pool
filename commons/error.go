package commons

import (
	"errors"
	"fmt"
)

// FileHandleNotFoundError contains file handle not found error information
type FileHandleNotFoundError struct {
	HandleID string
}

// NewFileHandleNotFoundError creates an error for file handle not found error
func NewFileHandleNotFoundError(handleID string) error {
	return &FileHandleNotFoundError{
		HandleID: handleID,
	}
}

// Error returns error message
func (err *FileHandleNotFoundError) Error() string {
	return fmt.Sprintf("file handle %q not found error", err.HandleID)
}

// Is tests type of error
func (err *FileHandleNotFoundError) Is(other error) bool {
	_, ok := other.(*FileHandleNotFoundError)
	return ok
}

// ToString stringifies the object
func (err *FileHandleNotFoundError) ToString() string {
	return "<FileHandleNotFoundError>"
}

// IsFileHandleNotFoundError evaluates if the given error is file handle not found error
func IsFileHandleNotFoundError(err error) bool {
	var fileHandleNotFoundErr *FileHandleNotFoundError
	return errors.As(err, &fileHandleNotFoundErr)
}

// SessionNotFoundError contains session not found error information
type SessionNotFoundError struct {
	SessionID string
}

// NewSessionNotFoundError creates SessionNotFoundError struct
func NewSessionNotFoundError(sessionID string) error {
	return &SessionNotFoundError{
		SessionID: sessionID,
	}
}

// Error returns error message
func (err *SessionNotFoundError) Error() string {
	return fmt.Sprintf("session %q not found", err.SessionID)
}

// Is tests type of error
func (err *SessionNotFoundError) Is(other error) bool {
	_, ok := other.(*SessionNotFoundError)
	return ok
}

// ToString stringifies the object
func (err *SessionNotFoundError) ToString() string {
	return "<SessionNotFoundError>"
}

// IsSessionNotFoundError evaluates if the given error is session not found error
func IsSessionNotFoundError(err error) bool {
	var sessionNotFoundErr *SessionNotFoundError
	return errors.As(err, &sessionNotFoundErr)
}
