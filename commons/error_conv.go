package commons

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	errorTypeDelimiter             string = ";"
	errorTypeSessionNotFound       string = "session_not_found"
	errorTypeIRodsFSClientNotFound string = "fs_client_instance_not_found"
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

func addErrorTypeToMessage(prefix string, details ...string) string {
	detailsStr := strings.Join(details, errorTypeDelimiter)
	return fmt.Sprintf("%s%s%s", prefix, errorTypeDelimiter, detailsStr)
}

func extractErrorInfoFromMessage(msg string) (string, []string, string) {
	msgarr := strings.Split(msg, errorTypeDelimiter)
	if len(msgarr) == 2 {
		return msgarr[0], []string{}, msgarr[1]
	} else if len(msgarr) >= 3 {
		return msgarr[0], msgarr[1 : len(msgarr)-1], msgarr[len(msgarr)-1]
	}
	return errorTypeInternalError, []string{}, ""
}

// ErrorToStatus converts error to grpc status error
func ErrorToStatus(err error) error {
	if err == nil {
		return nil
	}

	if IsSessionNotFoundError(err) {
		var sessionNotFoundErr *SessionNotFoundError
		if errors.As(err, &sessionNotFoundErr) {
			return status.Error(codes.Unauthenticated, addErrorTypeToMessage(errorTypeSessionNotFound, sessionNotFoundErr.SessionID, sessionNotFoundErr.Error()))
		}
		return status.Error(codes.Unauthenticated, addErrorTypeToMessage(errorTypeSessionNotFound, err.Error()))
	} else if IsIRODSFSClientInstanceNotFoundError(err) {
		var irodsFSClientInstanceNotFoundErr *IRODSFSClientInstanceNotFoundError
		if errors.As(err, &irodsFSClientInstanceNotFoundErr) {
			return status.Error(codes.Unauthenticated, addErrorTypeToMessage(errorTypeIRodsFSClientNotFound, irodsFSClientInstanceNotFoundErr.InstanceID, irodsFSClientInstanceNotFoundErr.Error()))
		}
		return status.Error(codes.Unauthenticated, addErrorTypeToMessage(errorTypeIRodsFSClientNotFound, err.Error()))
	} else if IsFileHandleNotFoundError(err) {
		var fileHandleNotFoundErr *FileHandleNotFoundError
		if errors.As(err, &fileHandleNotFoundErr) {
			return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeFileHandleNotFound, fileHandleNotFoundErr.HandleID, fileHandleNotFoundErr.Error()))
		}
		return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeFileHandleNotFound, err.Error()))
	} else if irodsclient_types.IsConnectionConfigError(err) {
		var connectionConfigError *irodsclient_types.ConnectionConfigError
		if errors.As(err, &connectionConfigError) {
			return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeConnectionConfigError, connectionConfigError.Config.Host, strconv.Itoa(connectionConfigError.Config.Port), connectionConfigError.Config.ClientZone, connectionConfigError.Config.ClientUser, connectionConfigError.Error()))
		}
		return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeConnectionConfigError, err.Error()))
	} else if irodsclient_types.IsConnectionError(err) {
		return status.Error(codes.Unavailable, addErrorTypeToMessage(errorTypeConnectionError, err.Error()))
	} else if irodsclient_types.IsConnectionPoolFullError(err) {
		var connectionPoolFullError *irodsclient_types.ConnectionPoolFullError
		if errors.As(err, &connectionPoolFullError) {
			return status.Error(codes.ResourceExhausted, addErrorTypeToMessage(errorTypeConnectionPoolFull, strconv.Itoa(connectionPoolFullError.Occupied), strconv.Itoa(connectionPoolFullError.Max), connectionPoolFullError.Error()))
		}
		return status.Error(codes.ResourceExhausted, addErrorTypeToMessage(errorTypeConnectionPoolFull, err.Error()))
	} else if irodsclient_types.IsAuthError(err) {
		var authError *irodsclient_types.AuthError
		if errors.As(err, &authError) {
			return status.Error(codes.Unauthenticated, addErrorTypeToMessage(errorTypeAuthenticationError, authError.Config.Host, strconv.Itoa(authError.Config.Port), authError.Config.ClientZone, authError.Config.ClientUser, authError.Error()))
		}
		return status.Error(codes.Unauthenticated, addErrorTypeToMessage(errorTypeAuthenticationError, err.Error()))
	} else if irodsclient_types.IsFileNotFoundError(err) {
		var fileNotFoundError *irodsclient_types.FileNotFoundError
		if errors.As(err, &fileNotFoundError) {
			return status.Error(codes.NotFound, addErrorTypeToMessage(errorTypeFileNotFound, fileNotFoundError.Path, fileNotFoundError.Error()))
		}
		return status.Error(codes.NotFound, addErrorTypeToMessage(errorTypeFileNotFound, err.Error()))
	} else if irodsclient_types.IsCollectionNotEmptyError(err) {
		var collectionNotEmptyError *irodsclient_types.CollectionNotEmptyError
		if errors.As(err, &collectionNotEmptyError) {
			return status.Error(codes.FailedPrecondition, addErrorTypeToMessage(errorTypeCollectionNotEmpty, collectionNotEmptyError.Path, collectionNotEmptyError.Error()))
		}
		return status.Error(codes.FailedPrecondition, addErrorTypeToMessage(errorTypeCollectionNotEmpty, err.Error()))
	} else if irodsclient_types.IsFileAlreadyExistError(err) {
		var fileAlreadyExistError *irodsclient_types.FileAlreadyExistError
		if errors.As(err, &fileAlreadyExistError) {
			return status.Error(codes.AlreadyExists, addErrorTypeToMessage(errorTypeFileAlreadyExist, fileAlreadyExistError.Path, fileAlreadyExistError.Error()))
		}
		return status.Error(codes.AlreadyExists, addErrorTypeToMessage(errorTypeFileAlreadyExist, err.Error()))
	} else if irodsclient_types.IsTicketNotFoundError(err) {
		var ticketNotFoundError *irodsclient_types.TicketNotFoundError
		if errors.As(err, &ticketNotFoundError) {
			return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeTicketNotFound, ticketNotFoundError.Ticket, ticketNotFoundError.Error()))
		}
		return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeTicketNotFound, err.Error()))
	} else if irodsclient_types.IsUserNotFoundError(err) {
		var userNotFoundError *irodsclient_types.UserNotFoundError
		if errors.As(err, &userNotFoundError) {
			return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeUserNotFound, userNotFoundError.Name, userNotFoundError.Error()))
		}
		return status.Error(codes.InvalidArgument, addErrorTypeToMessage(errorTypeUserNotFound, err.Error()))
	} else if irodsclient_types.IsIRODSError(err) {
		var irodsError *irodsclient_types.IRODSError
		if errors.As(err, &irodsError) {
			return status.Error(codes.Internal, addErrorTypeToMessage(errorTypeIRODSError, strconv.Itoa(int(irodsError.Code)), irodsError.ContextualMessage, irodsError.Error()))
		}
		return status.Error(codes.Internal, addErrorTypeToMessage(errorTypeIRODSError, err.Error()))
	}

	return status.Error(codes.Internal, addErrorTypeToMessage(errorTypeInternalError, err.Error()))
}

// StatusToError converts grpc status error to error
func StatusToError(err error) error {
	if err == nil {
		return nil
	}

	st, _ := status.FromError(err)
	if st != nil {
		errType, errContent, _ := extractErrorInfoFromMessage(st.Message())
		switch errType {
		case errorTypeSessionNotFound:
			if len(errContent) > 0 {
				return NewSessionNotFoundError(errContent[0])
			}
			return NewSessionNotFoundError("<unknown>")
		case errorTypeIRodsFSClientNotFound:
			if len(errContent) > 0 {
				return NewIRODSFSClientInstanceNotFoundError(errContent[0])
			}
			return NewIRODSFSClientInstanceNotFoundError("<unknown>")
		case errorTypeFileHandleNotFound:
			if len(errContent) > 0 {
				return NewFileHandleNotFoundError(errContent[0])
			}
			return NewFileHandleNotFoundError("<unknown>")
		case errorTypeConnectionConfigError:
			account := irodsclient_types.IRODSAccount{}
			if len(errContent) >= 4 {
				account.Host = errContent[0]
				p, _ := strconv.Atoi(errContent[1])
				account.Port = p
				account.ClientZone = errContent[2]
				account.ClientUser = errContent[3]
			}
			return irodsclient_types.NewConnectionConfigError(&account)
		case errorTypeConnectionError:
			return irodsclient_types.NewConnectionError()
		case errorTypeConnectionPoolFull:
			if len(errContent) >= 2 {
				o, _ := strconv.Atoi(errContent[0])
				m, _ := strconv.Atoi(errContent[1])
				return irodsclient_types.NewConnectionPoolFullError(o, m)
			}
			return irodsclient_types.NewConnectionPoolFullError(-1, -1)
		case errorTypeAuthenticationError:
			account := irodsclient_types.IRODSAccount{}
			if len(errContent) >= 4 {
				account.Host = errContent[0]
				p, _ := strconv.Atoi(errContent[1])
				account.Port = p
				account.ClientZone = errContent[2]
				account.ClientUser = errContent[3]
			}
			return irodsclient_types.NewAuthError(&account)
		case errorTypeFileNotFound:
			if len(errContent) > 0 {
				return irodsclient_types.NewFileNotFoundError(errContent[0])
			}
			return irodsclient_types.NewFileNotFoundError("<unknown>")
		case errorTypeCollectionNotEmpty:
			if len(errContent) > 0 {
				return irodsclient_types.NewCollectionNotEmptyError(errContent[0])
			}
			return irodsclient_types.NewCollectionNotEmptyError("<unknown>")
		case errorTypeFileAlreadyExist:
			if len(errContent) > 0 {
				return irodsclient_types.NewFileAlreadyExistError(errContent[0])
			}
			return irodsclient_types.NewFileAlreadyExistError("<unknown>")
		case errorTypeTicketNotFound:
			if len(errContent) > 0 {
				return irodsclient_types.NewTicketNotFoundError(errContent[0])
			}
			return irodsclient_types.NewTicketNotFoundError("<unknown>")
		case errorTypeUserNotFound:
			if len(errContent) > 0 {
				return irodsclient_types.NewUserNotFoundError(errContent[0])
			}
			return irodsclient_types.NewUserNotFoundError("<unknown>")
		case errorTypeIRODSError:
			if len(errContent) >= 2 {
				c, _ := strconv.Atoi(errContent[0])
				return irodsclient_types.NewIRODSErrorWithString(irodsclient_common.ErrorCode(c), errContent[1])
			}
			return irodsclient_types.NewIRODSError(irodsclient_common.SYS_UNKNOWN_ERROR)
		case errorTypeInternalError:
			return xerrors.Errorf(st.Message())
		default:
			switch st.Code() {
			case codes.NotFound:
				irodsclient_types.NewFileNotFoundError("<unknown>")
			case codes.AlreadyExists:
				return irodsclient_types.NewFileAlreadyExistError("<unknown>")
			case codes.Unauthenticated:
				account := irodsclient_types.IRODSAccount{}
				return irodsclient_types.NewAuthError(&account)
			case codes.Internal:
				return xerrors.Errorf(st.Message())
			default:
				return xerrors.Errorf(st.Message())
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
		case errorTypeSessionNotFound, errorTypeIRodsFSClientNotFound, errorTypeConnectionError:
			return true
		case errorTypeFileHandleNotFound, errorTypeConnectionConfigError, errorTypeConnectionPoolFull, errorTypeAuthenticationError, errorTypeFileNotFound, errorTypeCollectionNotEmpty, errorTypeFileAlreadyExist, errorTypeTicketNotFound, errorTypeUserNotFound, errorTypeIRODSError:
			return false
		default:
			switch st.Code() {
			case codes.Unauthenticated:
				return true
			default:
				return false
			}
		}
	}

	return false
}

// IsDisconnectedError returns true if connection is unavailable
func IsDisconnectedError(err error) bool {
	if err == nil {
		return false
	}

	st, _ := status.FromError(err)
	if st != nil {
		if st.Code() == codes.Unavailable {
			return true
		}
	}

	return false
}
