package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-pool/service/api"
)

func TestConvertAccountFromIRODSToAPI(t *testing.T) {
	account := &irodsclient_types.IRODSAccount{
		AuthenticationScheme:    irodsclient_types.AuthScheme("pam"),
		ClientServerNegotiation: true,
		CSNegotiationPolicy:     irodsclient_types.CSNegotiationPolicyRequest("CS_NEG_REQUIRE"),
		Host:                    "irods.example.org",
		Port:                    1247,
		ProxyUser:               "proxy",
		ProxyZone:               "tempZone",
		ClientUser:              "client",
		ClientZone:              "otherZone",
		Password:                "secret",
		SSLConfiguration: &irodsclient_types.IRODSSSLConfig{
			EncryptionAlgorithm: "AES-256-CBC",
			EncryptionKeySize:   32,
			VerifyServer:        irodsclient_types.SSLVerifyServerHostname,
		},
	}

	converted := convertAccountFromIRODSToAPI(account)

	require.Equal(t, "irods.example.org", converted.IrodsHost)
	require.Equal(t, int32(1247), converted.IrodsPort)
	require.Equal(t, "proxy", converted.IrodsUserName)
	require.Equal(t, "tempZone", converted.IrodsZoneName)
	require.Equal(t, "client", converted.GetIrodsClientUserName())
	require.Equal(t, "otherZone", converted.GetIrodsClientZoneName())
	require.Equal(t, "secret", converted.GetIrodsUserPassword())
	require.Equal(t, "AES-256-CBC", converted.GetIrodsEncryptionAlgorithm())
	require.Equal(t, int32(32), converted.GetIrodsEncryptionKeySize())
	require.Equal(t, "hostname", converted.GetIrodsSslVerifyServer())
}

func TestConvertEntryFromAPIToIRODS(t *testing.T) {
	createTime := time.Date(2026, time.September, 2, 12, 34, 56, 123456789, time.UTC)
	modifyTime := createTime.Add(time.Minute)
	accessTime := modifyTime.Add(time.Minute)
	entry := &api.Entry{
		Id:         42,
		Path:       "/tempZone/home/rods/file.txt",
		CreateTime: timestamppb.New(createTime),
		ModifyTime: timestamppb.New(modifyTime),
		AccessTime: timestamppb.New(accessTime),
	}

	converted, err := convertEntryFromAPIToIRODS(entry)

	require.NoError(t, err)
	require.Equal(t, createTime, converted.CreateTime)
	require.Equal(t, modifyTime, converted.ModifyTime)
	require.Equal(t, accessTime, converted.AccessTime)
}

func TestConvertEntryFromAPIToIRODSRejectsMissingTimestamp(t *testing.T) {
	_, err := convertEntryFromAPIToIRODS(&api.Entry{
		CreateTime: timestamppb.Now(),
		ModifyTime: timestamppb.Now(),
	})

	require.ErrorContains(t, err, "access_time is required")
}
