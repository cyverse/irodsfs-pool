package service

import "fmt"

// SessionNotFoundError ...
type SessionNotFoundError struct {
	message string
}

// NewSessionNotFoundError creates SessionNotFoundError struct
func NewSessionNotFoundError(message string) *SessionNotFoundError {
	return &SessionNotFoundError{
		message: message,
	}
}

// NewSessionNotFoundErrorf creates SessionNotFoundError struct
func NewSessionNotFoundErrorf(format string, v ...interface{}) *SessionNotFoundError {
	return &SessionNotFoundError{
		message: fmt.Sprintf(format, v...),
	}
}

func (e *SessionNotFoundError) Error() string {
	return e.message
}

// IsSessionNotFoundError evaluates if the given error is SessionNotFoundError
func IsSessionNotFoundError(err error) bool {
	if _, ok := err.(*SessionNotFoundError); ok {
		return true
	}

	return false
}
