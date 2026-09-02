package service

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cyverse/irodsfs-pool/service/api"
)

func TestConvertAccountFromAPIToIRODS(t *testing.T) {
	clientUser := "client"
	clientZone := "otherZone"
	encryptionAlgorithm := "AES-256-CBC"
	encryptionKeySize := int32(32)
	verifyServer := "hostname"
	account := &api.Account{
		IrodsHost:                "irods.example.org",
		IrodsPort:                1247,
		IrodsUserName:            "proxy",
		IrodsZoneName:            "tempZone",
		IrodsClientUserName:      &clientUser,
		IrodsClientZoneName:      &clientZone,
		IrodsEncryptionAlgorithm: &encryptionAlgorithm,
		IrodsEncryptionKeySize:   &encryptionKeySize,
		IrodsSslVerifyServer:     &verifyServer,
	}

	converted := convertAccountFromAPIToIRODS(account)

	require.Equal(t, "irods.example.org", converted.Host)
	require.Equal(t, 1247, converted.Port)
	require.Equal(t, "proxy", converted.ProxyUser)
	require.Equal(t, "tempZone", converted.ProxyZone)
	require.Equal(t, "client", converted.ClientUser)
	require.Equal(t, "otherZone", converted.ClientZone)
	require.NotNil(t, converted.SSLConfiguration)
	require.Equal(t, "AES-256-CBC", converted.SSLConfiguration.EncryptionAlgorithm)
	require.Equal(t, 32, converted.SSLConfiguration.EncryptionKeySize)
	require.Equal(t, "hostname", string(converted.SSLConfiguration.VerifyServer))
}

func TestConvertAccountFromAPIToIRODSDefaultsClientIdentity(t *testing.T) {
	account := &api.Account{
		IrodsUserName: "rods",
		IrodsZoneName: "tempZone",
	}

	converted := convertAccountFromAPIToIRODS(account)

	require.Equal(t, "rods", converted.ProxyUser)
	require.Equal(t, "rods", converted.ClientUser)
	require.Equal(t, "tempZone", converted.ProxyZone)
	require.Equal(t, "tempZone", converted.ClientZone)
	require.Nil(t, converted.SSLConfiguration)
}
