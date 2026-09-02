package service

import (
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-pool/service/api"
)

func convertAccountFromAPIToIRODS(account *api.Account) *irodsclient_types.IRODSAccount {
	var sslConf *irodsclient_types.IRODSSSLConfig
	if hasSSLConfiguration(account) {
		sslConf = &irodsclient_types.IRODSSSLConfig{
			CACertificateFile:       account.GetIrodsSslCaCertificateFile(),
			CACertificatePath:       account.GetIrodsSslCaCertificatePath(),
			EncryptionKeySize:       int(account.GetIrodsEncryptionKeySize()),
			EncryptionAlgorithm:     account.GetIrodsEncryptionAlgorithm(),
			EncryptionSaltSize:      int(account.GetIrodsEncryptionSaltSize()),
			EncryptionNumHashRounds: int(account.GetIrodsEncryptionNumHashRounds()),
			VerifyServer:            irodsclient_types.SSLVerifyServer(account.GetIrodsSslVerifyServer()),
			DHParamsFile:            account.GetIrodsSslDhParamsFile(),
			ServerName:              account.GetIrodsSslServerName(),
		}
	}

	return &irodsclient_types.IRODSAccount{
		AuthenticationScheme:    irodsclient_types.AuthScheme(account.GetIrodsAuthenticationScheme()),
		ClientServerNegotiation: account.GetIrodsClientServerNegotiation(),
		CSNegotiationPolicy:     irodsclient_types.CSNegotiationPolicyRequest(account.GetIrodsClientServerPolicy()),
		Host:                    account.IrodsHost,
		Port:                    int(account.IrodsPort),
		ClientUser:              clientUserFromAPI(account),
		ClientZone:              clientZoneFromAPI(account),
		ProxyUser:               account.IrodsUserName,
		ProxyZone:               account.IrodsZoneName,
		Password:                account.GetIrodsUserPassword(),
		Ticket:                  account.GetIrodsTicket(),
		DefaultResource:         account.GetIrodsDefaultResource(),
		DefaultHashScheme:       account.GetIrodsDefaultHashScheme(),
		PamTTL:                  int(account.GetIrodsPamTtl()),
		PAMToken:                account.GetIrodsPamToken(),
		SSLConfiguration:        sslConf,
	}
}

func clientUserFromAPI(account *api.Account) string {
	if account.IrodsClientUserName != nil {
		return account.GetIrodsClientUserName()
	}
	return account.IrodsUserName
}

func clientZoneFromAPI(account *api.Account) string {
	if account.IrodsClientZoneName != nil {
		return account.GetIrodsClientZoneName()
	}
	return account.IrodsZoneName
}

func hasSSLConfiguration(account *api.Account) bool {
	return account.IrodsEncryptionAlgorithm != nil ||
		account.IrodsEncryptionKeySize != nil ||
		account.IrodsEncryptionSaltSize != nil ||
		account.IrodsEncryptionNumHashRounds != nil ||
		account.IrodsSslCaCertificateFile != nil ||
		account.IrodsSslCaCertificatePath != nil ||
		account.IrodsSslVerifyServer != nil ||
		account.IrodsSslCertificateChainFile != nil ||
		account.IrodsSslCertificateKeyFile != nil ||
		account.IrodsSslDhParamsFile != nil ||
		account.IrodsSslServerName != nil
}
