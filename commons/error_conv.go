package commons

import (
	"errors"
	"strconv"
	"strings"

	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	errorTypeDelimiter             string = ";"
	errorTypeEscape                string = "\\"
	errorTypeSessionNotFound       string = "session_not_found"
	errorTypeFileHandleNotFound    string = "filehandle_not_found"
	errorTypeConnectionConfigError string = "connection_config_error"
	errorTypeConnectionError       string = "connection_error"
	errorTypeConnectionPoolFull    string = "connection_pool_full"
	errorTypeAuthenticationError   string = "authentication_error"
	errorTypeFileNotFound          string = "file_not_found"
	errorTypeCollectionNotEmpty    string = "collection_not_empty"
	errorTypeFileAlreadyExist      string = "file_already_exist"
	errorTypeTicketNotFound        string = "ticket_not_found"
	errorTypeUserNotFound          string = "user_not_found"
	errorTypeIRODSError            string = "irods_error"
	errorTypeInternalError         string = "internal_error"
)

// escapeErrorDetail escapes the delimiter in a detail so that a detail containing
// the delimiter (an iRODS path or an error message, for example) does not split
// the message into more fields than it has
func escapeErrorDetail(detail string) string {
	detail = strings.ReplaceAll(detail, errorTypeEscape, errorTypeEscape+errorTypeEscape)
	return strings.ReplaceAll(detail, errorTypeDelimiter, errorTypeEscape+errorTypeDelimiter)
}

// splitErrorMessage splits a message at unescaped delimiters and unescapes each part
func splitErrorMessage(msg string) []string {
	parts := []string{}
	part := strings.Builder{}
	escaped := false

	for _, char := range msg {
		switch {
		case escaped:
			part.WriteRune(char)
			escaped = false
		case string(char) == errorTypeEscape:
			escaped = true
		case string(char) == errorTypeDelimiter:
			parts = append(parts, part.String())
			part.Reset()
		default:
			part.WriteRune(char)
		}
	}

	if escaped {
		// dangling escape character, keep it as it is
		part.WriteString(errorTypeEscape)
	}

	return append(parts, part.String())
}

func addErrorTypeToMessage(prefix string, details ...string) string {
	parts := make([]string, 0, len(details)+1)
	parts = append(parts, prefix)

	for _, detail := range details {
		parts = append(parts, escapeErrorDetail(detail))
	}

	return strings.Join(parts, errorTypeDelimiter)
}

func extractErrorInfoFromMessage(msg string) (string, []string, string) {
	msgarr := splitErrorMessage(msg)
	if len(msgarr) == 2 {
		return msgarr[0], []string{}, msgarr[1]
	} else if len(msgarr) >= 3 {
		return msgarr[0], msgarr[1 : len(msgarr)-1], msgarr[len(msgarr)-1]
	}
	return errorTypeInternalError, []string{}, ""
}

// accountToErrorDetails serializes the account fields that an error message carries.
// New fields must be appended at the end, so that a peer running an older version,
// which reads only the fields it knows about, still parses the message correctly.
func accountToErrorDetails(account *irodsclient_types.IRODSAccount) []string {
	if account == nil {
		return []string{"", "0", "", "", "", "", ""}
	}

	return []string{
		account.Host,
		strconv.Itoa(account.Port),
		account.ClientZone,
		account.ClientUser,
		string(account.AuthenticationScheme),
		account.ProxyUser,
		account.ProxyZone,
	}
}

// accountFromErrorDetails restores the account from error details. Details sent by a
// peer running an older version carry only the leading fields, so every field is optional.
func accountFromErrorDetails(details []string) *irodsclient_types.IRODSAccount {
	account := &irodsclient_types.IRODSAccount{}

	if len(details) >= 4 {
		account.Host = details[0]
		port, _ := strconv.Atoi(details[1])
		account.Port = port
		account.ClientZone = details[2]
		account.ClientUser = details[3]
	}

	if len(details) >= 7 {
		account.AuthenticationScheme = irodsclient_types.GetAuthScheme(details[4])
		account.ProxyUser = details[5]
		account.ProxyZone = details[6]
	}

	return account
}

// remoteError keeps the error message reported by the remote side, including the
// context the remote side wrapped it with, while leaving the restored error
// inspectable via errors.Is and errors.As
type remoteError struct {
	message string
	err     error
}

// newRemoteError attaches the remote error message to the restored error
func newRemoteError(err error, message string) error {
	if err == nil {
		return nil
	}

	if len(message) == 0 || message == err.Error() {
		return err
	}

	return &remoteError{
		message: message,
		err:     err,
	}
}

// Error returns error message
func (err *remoteError) Error() string {
	return err.message
}

// Unwrap returns the restored error
func (err *remoteError) Unwrap() error {
	return err.err
}

// ErrorToStatus converts error to grpc status error
func ErrorToStatus(err error) error {
	if err == nil {
		return nil
	}

	// the full message, including the context the error was wrapped with
	message := err.Error()

	if IsSessionNotFoundError(err) {
		var sessionNotFoundErr *SessionNotFoundError
		if errors.As(err, &sessionNotFoundErr) {
			return status.Error(codes.Unauthenticated, addErrorTypeToMessage(errorTypeSessionNotFound, sessionNotFoundErr.SessionID, message))
		}
		return status.Error(codes.Unauthenticated, addErrorTypeToMessage(errorTypeSessionNotFound, message))
	} else if IsFileHandleNotFoundError(err) {
		var fileHandleNotFoundErr *FileHandleNotFoundError
		if errors.As(err, &fileHandleNotFoundErr) {
			return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeFileHandleNotFound, fileHandleNotFoundErr.HandleID, message))
		}
		return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeFileHandleNotFound, message))
	} else if irodsclient_types.IsConnectionConfigError(err) {
		var connectionConfigError *irodsclient_types.ConnectionConfigError
		if errors.As(err, &connectionConfigError) {
			details := append(accountToErrorDetails(connectionConfigError.Account), message)
			return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeConnectionConfigError, details...))
		}
		return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeConnectionConfigError, message))
	} else if irodsclient_types.IsConnectionError(err) {
		return status.Error(codes.Unavailable, addErrorTypeToMessage(errorTypeConnectionError, message))
	} else if irodsclient_types.IsConnectionPoolFullError(err) {
		var connectionPoolFullError *irodsclient_types.ConnectionPoolFullError
		if errors.As(err, &connectionPoolFullError) {
			return status.Error(codes.ResourceExhausted, addErrorTypeToMessage(errorTypeConnectionPoolFull, strconv.Itoa(connectionPoolFullError.Occupied), strconv.Itoa(connectionPoolFullError.Max), message))
		}
		return status.Error(codes.ResourceExhausted, addErrorTypeToMessage(errorTypeConnectionPoolFull, message))
	} else if irodsclient_types.IsAuthError(err) {
		var authError *irodsclient_types.AuthError
		if errors.As(err, &authError) {
			details := append(accountToErrorDetails(authError.Config), message)
			return status.Error(codes.Unauthenticated, addErrorTypeToMessage(errorTypeAuthenticationError, details...))
		}
		return status.Error(codes.Unauthenticated, addErrorTypeToMessage(errorTypeAuthenticationError, message))
	} else if irodsclient_types.IsFileNotFoundError(err) {
		var fileNotFoundError *irodsclient_types.FileNotFoundError
		if errors.As(err, &fileNotFoundError) {
			return status.Error(codes.NotFound, addErrorTypeToMessage(errorTypeFileNotFound, fileNotFoundError.Path, message))
		}
		return status.Error(codes.NotFound, addErrorTypeToMessage(errorTypeFileNotFound, message))
	} else if irodsclient_types.IsCollectionNotEmptyError(err) {
		var collectionNotEmptyError *irodsclient_types.CollectionNotEmptyError
		if errors.As(err, &collectionNotEmptyError) {
			return status.Error(codes.FailedPrecondition, addErrorTypeToMessage(errorTypeCollectionNotEmpty, collectionNotEmptyError.Path, message))
		}
		return status.Error(codes.FailedPrecondition, addErrorTypeToMessage(errorTypeCollectionNotEmpty, message))
	} else if irodsclient_types.IsFileAlreadyExistError(err) {
		var fileAlreadyExistError *irodsclient_types.FileAlreadyExistError
		if errors.As(err, &fileAlreadyExistError) {
			return status.Error(codes.AlreadyExists, addErrorTypeToMessage(errorTypeFileAlreadyExist, fileAlreadyExistError.Path, message))
		}
		return status.Error(codes.AlreadyExists, addErrorTypeToMessage(errorTypeFileAlreadyExist, message))
	} else if irodsclient_types.IsTicketNotFoundError(err) {
		var ticketNotFoundError *irodsclient_types.TicketNotFoundError
		if errors.As(err, &ticketNotFoundError) {
			return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeTicketNotFound, ticketNotFoundError.Ticket, message))
		}
		return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeTicketNotFound, message))
	} else if irodsclient_types.IsUserNotFoundError(err) {
		var userNotFoundError *irodsclient_types.UserNotFoundError
		if errors.As(err, &userNotFoundError) {
			return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeUserNotFound, userNotFoundError.Name, message))
		}
		return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeUserNotFound, message))
	} else if irodsclient_types.IsIRODSError(err) {
		var irodsError *irodsclient_types.IRODSError
		if errors.As(err, &irodsError) {
			return status.Error(codes.Internal, addErrorTypeToMessage(errorTypeIRODSError, strconv.Itoa(int(irodsError.Code)), irodsError.ContextualMessage, message))
		}
		return status.Error(codes.Internal, addErrorTypeToMessage(errorTypeIRODSError, message))
	}

	return status.Error(codes.Internal, addErrorTypeToMessage(errorTypeInternalError, message))
}

// StatusToError converts grpc status error to error
func StatusToError(err error) error {
	if err == nil {
		return nil
	}

	st, _ := status.FromError(err)
	if st != nil {
		// Preserve transport errors before any message parsing so that
		// isTransportError() in client code can still detect them via
		// status.FromError().  Raw gRPC transport messages have no ";"
		// delimiter so they fall through to errorTypeInternalError otherwise,
		// which strips the gRPC status code.
		if st.Code() == codes.Unavailable {
			return err
		}

		errType, errContent, errMessage := extractErrorInfoFromMessage(st.Message())
		switch errType {
		case errorTypeSessionNotFound:
			if len(errContent) > 0 {
				return newRemoteError(NewSessionNotFoundError(errContent[0]), errMessage)
			}
			return newRemoteError(NewSessionNotFoundError("<unknown>"), errMessage)
		case errorTypeFileHandleNotFound:
			if len(errContent) > 0 {
				return newRemoteError(NewFileHandleNotFoundError(errContent[0]), errMessage)
			}
			return newRemoteError(NewFileHandleNotFoundError("<unknown>"), errMessage)
		case errorTypeConnectionConfigError:
			return newRemoteError(irodsclient_types.NewConnectionConfigError(accountFromErrorDetails(errContent)), errMessage)
		case errorTypeConnectionError:
			return newRemoteError(irodsclient_types.NewConnectionError(), errMessage)
		case errorTypeConnectionPoolFull:
			if len(errContent) >= 2 {
				o, _ := strconv.Atoi(errContent[0])
				m, _ := strconv.Atoi(errContent[1])
				return newRemoteError(irodsclient_types.NewConnectionPoolFullError(o, m), errMessage)
			}
			return newRemoteError(irodsclient_types.NewConnectionPoolFullError(-1, -1), errMessage)
		case errorTypeAuthenticationError:
			return newRemoteError(irodsclient_types.NewAuthError(accountFromErrorDetails(errContent)), errMessage)
		case errorTypeFileNotFound:
			if len(errContent) > 0 {
				return newRemoteError(irodsclient_types.NewFileNotFoundError(errContent[0]), errMessage)
			}
			return newRemoteError(irodsclient_types.NewFileNotFoundError("<unknown>"), errMessage)
		case errorTypeCollectionNotEmpty:
			if len(errContent) > 0 {
				return newRemoteError(irodsclient_types.NewCollectionNotEmptyError(errContent[0]), errMessage)
			}
			return newRemoteError(irodsclient_types.NewCollectionNotEmptyError("<unknown>"), errMessage)
		case errorTypeFileAlreadyExist:
			if len(errContent) > 0 {
				return newRemoteError(irodsclient_types.NewFileAlreadyExistError(errContent[0]), errMessage)
			}
			return newRemoteError(irodsclient_types.NewFileAlreadyExistError("<unknown>"), errMessage)
		case errorTypeTicketNotFound:
			if len(errContent) > 0 {
				return newRemoteError(irodsclient_types.NewTicketNotFoundError(errContent[0]), errMessage)
			}
			return newRemoteError(irodsclient_types.NewTicketNotFoundError("<unknown>"), errMessage)
		case errorTypeUserNotFound:
			if len(errContent) > 0 {
				return newRemoteError(irodsclient_types.NewUserNotFoundError(errContent[0]), errMessage)
			}
			return newRemoteError(irodsclient_types.NewUserNotFoundError("<unknown>"), errMessage)
		case errorTypeIRODSError:
			if len(errContent) >= 2 {
				c, _ := strconv.Atoi(errContent[0])
				return newRemoteError(irodsclient_types.NewIRODSErrorWithString(irodsclient_common.ErrorCode(c), errContent[1]), errMessage)
			}
			return newRemoteError(irodsclient_types.NewIRODSError(irodsclient_common.SYS_UNKNOWN_ERROR), errMessage)
		case errorTypeInternalError:
			if len(errMessage) > 0 {
				return errors.New(errMessage)
			}
			// a raw message that carries no error type, keep it as it is
			return errors.New(st.Message())
		default:
			switch st.Code() {
			case codes.NotFound:
				return newRemoteError(irodsclient_types.NewFileNotFoundError("<unknown>"), st.Message())
			case codes.AlreadyExists:
				return newRemoteError(irodsclient_types.NewFileAlreadyExistError("<unknown>"), st.Message())
			case codes.Unauthenticated:
				return newRemoteError(irodsclient_types.NewAuthError(&irodsclient_types.IRODSAccount{}), st.Message())
			case codes.Internal:
				return errors.New(st.Message())
			case codes.Unavailable:
				// Preserve as gRPC status so callers can detect transport errors.
				return err
			default:
				return errors.New(st.Message())
			}
		}
	}

	return err
}

// IsReloginRequiredError returns true if relogin can solve the issue
func IsReloginRequiredError(err error) bool {
	if err == nil {
		return false
	}

	st, _ := status.FromError(err)
	if st != nil {
		errType, _, _ := extractErrorInfoFromMessage(st.Message())
		switch errType {
		case errorTypeSessionNotFound, errorTypeConnectionError:
			return true
		case errorTypeFileHandleNotFound, errorTypeConnectionConfigError, errorTypeConnectionPoolFull, errorTypeAuthenticationError, errorTypeFileNotFound, errorTypeCollectionNotEmpty, errorTypeFileAlreadyExist, errorTypeTicketNotFound, errorTypeUserNotFound, errorTypeIRODSError:
			return false
		default:
			switch st.Code() {
			case codes.Unauthenticated, codes.Unavailable:
				return true
			default:
				return false
			}
		}
	}

	return false
}
