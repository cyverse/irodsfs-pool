package commons

import (
	"fmt"
	"testing"

	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAuthErrorRoundTrip(t *testing.T) {
	account := &irodsclient_types.IRODSAccount{
		AuthenticationScheme: irodsclient_types.AuthSchemeNative,
		Host:                 "data.cyverse.org",
		Port:                 1247,
		ClientUser:           "client_user",
		ClientZone:           "client_zone",
		ProxyUser:            "proxy_user",
		ProxyZone:            "proxy_zone",
		Password:             "secret",
	}

	origin := fmt.Errorf("failed to login to irods: %w", irodsclient_types.NewAuthError(account))
	restored := StatusToError(ErrorToStatus(origin))

	var authError *irodsclient_types.AuthError
	assert.ErrorAs(t, restored, &authError)
	assert.Equal(t, irodsclient_types.AuthSchemeNative, authError.Config.AuthenticationScheme)
	assert.Equal(t, "data.cyverse.org", authError.Config.Host)
	assert.Equal(t, 1247, authError.Config.Port)
	assert.Equal(t, "client_user", authError.Config.ClientUser)
	assert.Equal(t, "client_zone", authError.Config.ClientZone)
	assert.Equal(t, "proxy_user", authError.Config.ProxyUser)
	assert.Equal(t, "proxy_zone", authError.Config.ProxyZone)

	// the context the server wrapped the error with must survive too
	assert.Equal(t, origin.Error(), restored.Error())
	assert.NotContains(t, restored.Error(), "secret")
}

func TestConnectionConfigErrorRoundTrip(t *testing.T) {
	account := &irodsclient_types.IRODSAccount{
		AuthenticationScheme: irodsclient_types.AuthSchemePAMPassword,
		Host:                 "data.cyverse.org",
		Port:                 1247,
		ClientUser:           "client_user",
		ClientZone:           "client_zone",
		ProxyUser:            "proxy_user",
		ProxyZone:            "proxy_zone",
	}

	origin := irodsclient_types.NewConnectionConfigError(account)
	restored := StatusToError(ErrorToStatus(origin))

	var configError *irodsclient_types.ConnectionConfigError
	assert.ErrorAs(t, restored, &configError)
	assert.Equal(t, irodsclient_types.AuthSchemePAMPassword, configError.Account.AuthenticationScheme)
	assert.Equal(t, "proxy_user", configError.Account.ProxyUser)
	assert.Equal(t, "proxy_zone", configError.Account.ProxyZone)
	assert.Equal(t, origin.Error(), restored.Error())
}

func TestNilAccountDoesNotPanic(t *testing.T) {
	for _, origin := range []error{
		irodsclient_types.NewAuthError(nil),
		irodsclient_types.NewConnectionConfigError(nil),
	} {
		assert.NotPanics(t, func() {
			restored := StatusToError(ErrorToStatus(origin))
			assert.Error(t, restored)
		})
	}
}

func TestDelimiterInDetailsIsPreserved(t *testing.T) {
	path := "/zone/home/user/a;b;c.txt"

	origin := fmt.Errorf("failed to stat %q: %w", path, irodsclient_types.NewFileNotFoundError(path))
	restored := StatusToError(ErrorToStatus(origin))

	var notFoundError *irodsclient_types.FileNotFoundError
	assert.ErrorAs(t, restored, &notFoundError)
	assert.Equal(t, path, notFoundError.Path)
	assert.Equal(t, origin.Error(), restored.Error())
}

func TestIRODSErrorRoundTrip(t *testing.T) {
	origin := irodsclient_types.NewIRODSErrorWithString(irodsclient_common.CAT_NO_ROWS_FOUND, "no rows; found")
	restored := StatusToError(ErrorToStatus(origin))

	var irodsError *irodsclient_types.IRODSError
	assert.ErrorAs(t, restored, &irodsError)
	assert.Equal(t, irodsclient_common.CAT_NO_ROWS_FOUND, irodsError.Code)
	assert.Equal(t, "no rows; found", irodsError.ContextualMessage)
	assert.Equal(t, origin.Error(), restored.Error())
}

func TestFileLockConflictRoundTrip(t *testing.T) {
	origin := fmt.Errorf("write lock on %q [0, 10] conflicts with a write lock held by pid 42: %w", "/zone/home/file", irodsfs_common_irods.ErrFileLockConflict)
	restored := StatusToError(ErrorToStatus(origin))

	// the client turns a conflict into EAGAIN, so it has to survive the wire
	assert.ErrorIs(t, restored, irodsfs_common_irods.ErrFileLockConflict)
	assert.Equal(t, origin.Error(), restored.Error())
	assert.Equal(t, codes.FailedPrecondition, status.Code(ErrorToStatus(origin)))
}

func TestSessionNotFoundRoundTrip(t *testing.T) {
	origin := fmt.Errorf("relogin needed: %w", NewSessionNotFoundError("session_id"))
	restored := StatusToError(ErrorToStatus(origin))

	var sessionNotFoundError *SessionNotFoundError
	assert.ErrorAs(t, restored, &sessionNotFoundError)
	assert.Equal(t, "session_id", sessionNotFoundError.SessionID)
	assert.Equal(t, origin.Error(), restored.Error())
	assert.True(t, IsReloginRequiredError(ErrorToStatus(origin)))
}

// an older peer sends the leading account fields only, without escaping
func TestLegacyAuthErrorMessageIsAccepted(t *testing.T) {
	legacy := status.Error(codes.Unauthenticated, "authentication_error;data.cyverse.org;1247;client_zone;client_user;authentication error")
	restored := StatusToError(legacy)

	var authError *irodsclient_types.AuthError
	assert.ErrorAs(t, restored, &authError)
	assert.Equal(t, "data.cyverse.org", authError.Config.Host)
	assert.Equal(t, 1247, authError.Config.Port)
	assert.Equal(t, "client_user", authError.Config.ClientUser)
	assert.Equal(t, "client_zone", authError.Config.ClientZone)
	assert.Equal(t, irodsclient_types.AuthSchemeUnknown, authError.Config.AuthenticationScheme)
}

// an older peer reads the fields it knows about and ignores the ones it does not
func TestNewAuthErrorMessageIsReadableByLegacyPeer(t *testing.T) {
	account := &irodsclient_types.IRODSAccount{
		AuthenticationScheme: irodsclient_types.AuthSchemeNative,
		Host:                 "data.cyverse.org",
		Port:                 1247,
		ClientUser:           "client_user",
		ClientZone:           "client_zone",
		ProxyUser:            "proxy_user",
		ProxyZone:            "proxy_zone",
	}

	st, _ := status.FromError(ErrorToStatus(irodsclient_types.NewAuthError(account)))
	errType, errContent, _ := extractErrorInfoFromMessage(st.Message())

	assert.Equal(t, errorTypeAuthenticationError, errType)
	assert.GreaterOrEqual(t, len(errContent), 4)
	assert.Equal(t, []string{"data.cyverse.org", "1247", "client_zone", "client_user"}, errContent[:4])
}

func TestInternalErrorDropsTypePrefix(t *testing.T) {
	origin := fmt.Errorf("something went wrong")
	restored := StatusToError(ErrorToStatus(origin))

	assert.Equal(t, "something went wrong", restored.Error())
}

// a raw gRPC message carries no error type and must be kept as it is
func TestRawStatusMessageIsKept(t *testing.T) {
	raw := status.Error(codes.Internal, "some raw transport message")
	assert.Equal(t, "some raw transport message", StatusToError(raw).Error())
}
