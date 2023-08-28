package service

import (
	"errors"

	"golang.org/x/xerrors"
)

var (
	fileHandleNotFoundError            error = xerrors.New("file handle not found")
	irodsFsClientInstanceNotFoundError error = xerrors.New("iRODS FS Client Instance not found")
	sessionNotFoundError               error = xerrors.New("session not found")
)

// NewFileHandleNotFoundError creates an error for file handle not found error
func NewFileHandleNotFoundError() error {
	return fileHandleNotFoundError
}

// IsFileHandleNotFoundError evaluates if the given error is file handle not found error
func IsFileHandleNotFoundError(err error) bool {
	return errors.Is(err, fileHandleNotFoundError)
}

// NewIrodsFsClientInstanceNotFoundError creates IrodsFsClientInstanceNotFoundError struct
func NewIrodsFsClientInstanceNotFoundError() error {
	return irodsFsClientInstanceNotFoundError
}

// IsIrodsFsClientInstanceNotFoundError evaluates if the given error is irods fs client instance not found error
func IsIrodsFsClientInstanceNotFoundError(err error) bool {
	return errors.Is(err, irodsFsClientInstanceNotFoundError)
}

// NewSessionNotFoundError creates an error for session not found error
func NewSessionNotFoundError() error {
	return sessionNotFoundError
}

// IsSessionNotFoundError evaluates if the given error is session not found error
func IsSessionNotFoundError(err error) bool {
	return errors.Is(err, sessionNotFoundError)
}
