package service

import "fmt"

// IrodsFsClientInstanceNotFoundError ...
type IrodsFsClientInstanceNotFoundError struct {
	message string
}

// NewIrodsFsClientInstanceNotFoundError creates IrodsFsClientInstanceNotFoundError struct
func NewIrodsFsClientInstanceNotFoundError(message string) *IrodsFsClientInstanceNotFoundError {
	return &IrodsFsClientInstanceNotFoundError{
		message: message,
	}
}

// NewIrodsFsClientInstanceNotFoundErrorf creates IrodsFsClientInstanceNotFoundError struct
func NewIrodsFsClientInstanceNotFoundErrorf(format string, v ...interface{}) *IrodsFsClientInstanceNotFoundError {
	return &IrodsFsClientInstanceNotFoundError{
		message: fmt.Sprintf(format, v...),
	}
}

func (e *IrodsFsClientInstanceNotFoundError) Error() string {
	return e.message
}

// IsIrodsFsClientInstanceNotFoundError evaluates if the given error is IrodsFsClientInstanceNotFoundError
func IsIrodsFsClientInstanceNotFoundError(err error) bool {
	if _, ok := err.(*IrodsFsClientInstanceNotFoundError); ok {
		return true
	}

	return false
}
