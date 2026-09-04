package client

import (
	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-pool/service/api"
)

func convertAccountFromIRODSToAPI(account *irodsclient_types.IRODSAccount) *api.Account {
	proxyUser := account.ProxyUser
	if proxyUser == "" {
		proxyUser = account.ClientUser
	}
	proxyZone := account.ProxyZone
	if proxyZone == "" {
		proxyZone = account.ClientZone
	}

	apiAccount := &api.Account{
		IrodsAuthenticationScheme:    stringPointer(string(account.AuthenticationScheme)),
		IrodsClientServerNegotiation: boolPointer(account.ClientServerNegotiation),
		IrodsClientServerPolicy:      stringPointer(string(account.CSNegotiationPolicy)),
		IrodsHost:                    account.Host,
		IrodsPort:                    int32(account.Port),
		IrodsZoneName:                proxyZone,
		IrodsClientZoneName:          stringPointer(account.ClientZone),
		IrodsUserName:                proxyUser,
		IrodsClientUserName:          stringPointer(account.ClientUser),
		IrodsDefaultResource:         stringPointer(account.DefaultResource),
		IrodsDefaultHashScheme:       stringPointer(account.DefaultHashScheme),
		IrodsUserPassword:            stringPointer(account.Password),
		IrodsTicket:                  stringPointer(account.Ticket),
		IrodsPamToken:                stringPointer(account.PAMToken),
		IrodsPamTtl:                  int32Pointer(int32(account.PamTTL)),
	}

	if ssl := account.SSLConfiguration; ssl != nil {
		apiAccount.IrodsEncryptionAlgorithm = stringPointer(ssl.EncryptionAlgorithm)
		apiAccount.IrodsEncryptionKeySize = int32Pointer(int32(ssl.EncryptionKeySize))
		apiAccount.IrodsEncryptionSaltSize = int32Pointer(int32(ssl.EncryptionSaltSize))
		apiAccount.IrodsEncryptionNumHashRounds = int32Pointer(int32(ssl.EncryptionNumHashRounds))
		apiAccount.IrodsSslCaCertificateFile = stringPointer(ssl.CACertificateFile)
		apiAccount.IrodsSslCaCertificatePath = stringPointer(ssl.CACertificatePath)
		apiAccount.IrodsSslVerifyServer = stringPointer(string(ssl.VerifyServer))
		apiAccount.IrodsSslDhParamsFile = stringPointer(ssl.DHParamsFile)
		apiAccount.IrodsSslServerName = stringPointer(ssl.ServerName)
	}

	return apiAccount
}

func convertEntryFromAPIToIRODS(entry *api.Entry) (*irodsclient_fs.Entry, error) {
	if entry == nil {
		return nil, errors.New("entry is required")
	}
	if entry.CreateTime == nil {
		return nil, errors.New("create_time is required")
	}
	if err := entry.CreateTime.CheckValid(); err != nil {
		return nil, errors.Wrap(err, "invalid create_time")
	}
	if entry.ModifyTime == nil {
		return nil, errors.New("modify_time is required")
	}
	if err := entry.ModifyTime.CheckValid(); err != nil {
		return nil, errors.Wrap(err, "invalid modify_time")
	}
	if entry.AccessTime == nil {
		return nil, errors.New("access_time is required")
	}
	if err := entry.AccessTime.CheckValid(); err != nil {
		return nil, errors.Wrap(err, "invalid access_time")
	}

	return &irodsclient_fs.Entry{
		ID:                entry.Id,
		Type:              irodsclient_fs.EntryType(entry.Type),
		Name:              entry.Name,
		Path:              entry.Path,
		Owner:             entry.Owner,
		Size:              entry.Size,
		DataType:          entry.DataType,
		CreateTime:        entry.CreateTime.AsTime(),
		ModifyTime:        entry.ModifyTime.AsTime(),
		AccessTime:        entry.AccessTime.AsTime(),
		CheckSumAlgorithm: irodsclient_types.ChecksumAlgorithm(entry.ChecksumAlgorithm),
		CheckSum:          entry.Checksum,
	}, nil
}

func stringPointer(value string) *string { return &value }
func boolPointer(value bool) *bool       { return &value }
func int32Pointer(value int32) *int32    { return &value }
