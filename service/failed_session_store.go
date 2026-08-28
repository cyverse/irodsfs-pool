package service

import (
	"encoding/json"
	"os"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/dgraph-io/badger/v3"
)

const (
	failedSessionDBDirectoryName = "failed_sessions.db"
	failedSessionKeyPrefix       = "failed-session:"
)

type FailedSessionStatus string

const (
	FailedSessionStatusActive        FailedSessionStatus = "active"
	FailedSessionStatusInterrupted   FailedSessionStatus = "interrupted"
	FailedSessionStatusRecovering    FailedSessionStatus = "recovering"
	FailedSessionStatusReleaseFailed FailedSessionStatus = "release_failed"
)

type FailedSessionInfo struct {
	ID             string                        `json:"id"`
	Status         FailedSessionStatus           `json:"status"`
	AccountKey     string                        `json:"account_key"`
	IRODSAccount   RedactedIRODSAccount          `json:"irods_account"`
	Connections    []FailedSessionConnectionInfo `json:"connections"`
	LastAccessTime time.Time                     `json:"last_access_time"`
}

type FailedSessionConnectionInfo struct {
	ConnectionID string `json:"connection_id"`
	Application  string `json:"application"`
	Description  string `json:"description,omitempty"`
}

// RedactedIRODSAccount intentionally excludes Password, Ticket, and PAMToken.
type RedactedIRODSAccount struct {
	AuthenticationScheme    string                  `json:"authentication_scheme"`
	ClientServerNegotiation bool                    `json:"client_server_negotiation"`
	CSNegotiationPolicy     string                  `json:"cs_negotiation_policy"`
	Host                    string                  `json:"host"`
	Port                    int                     `json:"port"`
	ClientUser              string                  `json:"client_user"`
	ClientZone              string                  `json:"client_zone"`
	ProxyUser               string                  `json:"proxy_user,omitempty"`
	ProxyZone               string                  `json:"proxy_zone,omitempty"`
	DefaultResource         string                  `json:"default_resource,omitempty"`
	DefaultHashScheme       string                  `json:"default_hash_scheme,omitempty"`
	PAMTTL                  int                     `json:"pam_ttl"`
	SSLConfiguration        *RedactedIRODSSSLConfig `json:"ssl_configuration,omitempty"`
}

type RedactedIRODSSSLConfig struct {
	CACertificateFile       string `json:"ca_certificate_file,omitempty"`
	CACertificatePath       string `json:"ca_certificate_path,omitempty"`
	EncryptionKeySize       int    `json:"encryption_key_size"`
	EncryptionAlgorithm     string `json:"encryption_algorithm,omitempty"`
	EncryptionSaltSize      int    `json:"encryption_salt_size"`
	EncryptionNumHashRounds int    `json:"encryption_num_hash_rounds"`
	VerifyServer            string `json:"verify_server"`
	DHParamsFile            string `json:"dh_params_file,omitempty"`
	ServerName              string `json:"server_name,omitempty"`
}

func (manager *PoolSessionManager) handleSessionReleaseResult(session *PoolSession, releaseErr error) {
	if releaseErr != nil {
		if err := manager.saveFailedSession(snapshotFailedSession(session, FailedSessionStatusReleaseFailed)); err != nil {
			manager.logger.WithError(errors.CombineErrors(releaseErr, err)).Errorf(
				"Failed to release pool session %q cleanly and persist its recovery information", session.id)
			return
		}
		manager.logger.WithError(releaseErr).Errorf(
			"Failed to release pool session %q cleanly; recovery information was persisted", session.id)
		return
	}

	if err := manager.RemoveFailedSession(session.id); err != nil {
		manager.logger.WithError(err).Errorf("Failed to remove recovered session %q from failed session store", session.id)
	}
}

func snapshotFailedSession(session *PoolSession, status FailedSessionStatus) FailedSessionInfo {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	info := FailedSessionInfo{
		ID:             session.id,
		Status:         status,
		AccountKey:     session.accountKey,
		IRODSAccount:   redactIRODSAccount(session.irodsAccount),
		Connections:    make([]FailedSessionConnectionInfo, 0, len(session.connections)),
		LastAccessTime: session.lastAccessTime,
	}
	for connectionID, connection := range session.connections {
		info.Connections = append(info.Connections, FailedSessionConnectionInfo{
			ConnectionID: connectionID,
			Application:  connection.appName,
			Description:  connection.description,
		})
	}
	sort.Slice(info.Connections, func(i, j int) bool {
		return info.Connections[i].ConnectionID < info.Connections[j].ConnectionID
	})
	return info
}

func (manager *PoolSessionManager) trackActiveSession(session *PoolSession) error {
	status := FailedSessionStatusActive
	existing, err := manager.getStoredSession(session.id)
	if err != nil {
		return err
	}
	if existing != nil && existing.Status != FailedSessionStatusActive {
		status = FailedSessionStatusRecovering
	}
	return manager.saveFailedSession(snapshotFailedSession(session, status))
}

func (manager *PoolSessionManager) checkpointSession(session *PoolSession) {
	existing, err := manager.getStoredSession(session.id)
	if err != nil {
		manager.logger.WithError(err).Errorf("Failed to read lifecycle record for session %q", session.id)
		return
	}
	status := FailedSessionStatusActive
	if existing != nil {
		status = existing.Status
	}
	if err := manager.saveFailedSession(snapshotFailedSession(session, status)); err != nil {
		manager.logger.WithError(err).Errorf("Failed to checkpoint lifecycle record for session %q", session.id)
	}
}

func redactIRODSAccount(account *irodsclient_types.IRODSAccount) RedactedIRODSAccount {
	if account == nil {
		return RedactedIRODSAccount{}
	}

	redacted := RedactedIRODSAccount{
		AuthenticationScheme:    string(account.AuthenticationScheme),
		ClientServerNegotiation: account.ClientServerNegotiation,
		CSNegotiationPolicy:     string(account.CSNegotiationPolicy),
		Host:                    account.Host,
		Port:                    account.Port,
		ClientUser:              account.ClientUser,
		ClientZone:              account.ClientZone,
		ProxyUser:               account.ProxyUser,
		ProxyZone:               account.ProxyZone,
		DefaultResource:         account.DefaultResource,
		DefaultHashScheme:       account.DefaultHashScheme,
		PAMTTL:                  account.PamTTL,
	}
	if account.SSLConfiguration != nil {
		redacted.SSLConfiguration = &RedactedIRODSSSLConfig{
			CACertificateFile:       account.SSLConfiguration.CACertificateFile,
			CACertificatePath:       account.SSLConfiguration.CACertificatePath,
			EncryptionKeySize:       account.SSLConfiguration.EncryptionKeySize,
			EncryptionAlgorithm:     account.SSLConfiguration.EncryptionAlgorithm,
			EncryptionSaltSize:      account.SSLConfiguration.EncryptionSaltSize,
			EncryptionNumHashRounds: account.SSLConfiguration.EncryptionNumHashRounds,
			VerifyServer:            string(account.SSLConfiguration.VerifyServer),
			DHParamsFile:            account.SSLConfiguration.DHParamsFile,
			ServerName:              account.SSLConfiguration.ServerName,
		}
	}
	return redacted
}

func (manager *PoolSessionManager) loadFailedSessionStore() error {
	manager.failedSessionMutex.Lock()
	defer manager.failedSessionMutex.Unlock()

	if _, err := os.Stat(manager.failedSessionDBPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Wrap(err, "failed to stat failed session store")
	}

	if err := manager.openFailedSessionStoreLocked(); err != nil {
		return err
	}
	sessions, err := manager.getAllStoredSessionsLocked()
	if err != nil {
		_ = manager.failedSessionDB.Close()
		manager.failedSessionDB = nil
		return err
	}
	if len(sessions) == 0 {
		return manager.removeFailedSessionStoreLocked()
	}

	changed := false
	for i := range sessions {
		switch sessions[i].Status {
		case "":
			// Records written before lifecycle tracking represented release failures.
			sessions[i].Status = FailedSessionStatusReleaseFailed
			changed = true
		case FailedSessionStatusActive, FailedSessionStatusRecovering:
			sessions[i].Status = FailedSessionStatusInterrupted
			changed = true
		}
	}
	if changed {
		if err := manager.failedSessionDB.Update(func(transaction *badger.Txn) error {
			for _, session := range sessions {
				data, err := json.Marshal(session)
				if err != nil {
					return err
				}
				if err := transaction.Set([]byte(failedSessionKeyPrefix+session.ID), data); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			_ = manager.failedSessionDB.Close()
			manager.failedSessionDB = nil
			return errors.Wrap(err, "failed to mark interrupted sessions")
		}
	}

	manager.logger.Infof("Loaded %d pool session lifecycle records from %q", len(sessions), manager.failedSessionDBPath)
	return nil
}

func (manager *PoolSessionManager) openFailedSessionStoreLocked() error {
	if manager.failedSessionDB != nil {
		return nil
	}
	if manager.failedSessionDBPath == "" {
		return errors.New("failed session DB path is required")
	}
	if err := os.MkdirAll(manager.config.dataRootPath, 0755); err != nil {
		return errors.Wrap(err, "failed to create data root for failed session store")
	}

	options := badger.DefaultOptions(manager.failedSessionDBPath).WithLogger(nil).WithSyncWrites(true)
	db, err := badger.Open(options)
	if err != nil {
		return errors.Wrap(err, "failed to open failed session store")
	}
	manager.failedSessionDB = db
	return nil
}

func (manager *PoolSessionManager) closeFailedSessionStore() error {
	manager.failedSessionMutex.Lock()
	defer manager.failedSessionMutex.Unlock()

	if manager.failedSessionDB == nil {
		return nil
	}
	err := manager.failedSessionDB.Close()
	manager.failedSessionDB = nil
	return errors.Wrap(err, "failed to close failed session store")
}

func (manager *PoolSessionManager) saveFailedSession(info FailedSessionInfo) error {
	manager.failedSessionMutex.Lock()
	defer manager.failedSessionMutex.Unlock()

	if err := manager.openFailedSessionStoreLocked(); err != nil {
		return err
	}
	data, err := json.Marshal(info)
	if err != nil {
		return errors.Wrap(err, "failed to marshal failed session")
	}
	return manager.failedSessionDB.Update(func(transaction *badger.Txn) error {
		return transaction.Set([]byte(failedSessionKeyPrefix+info.ID), data)
	})
}

func (manager *PoolSessionManager) GetFailedSessions() ([]FailedSessionInfo, error) {
	manager.failedSessionMutex.Lock()
	defer manager.failedSessionMutex.Unlock()

	sessions, err := manager.getAllStoredSessionsLocked()
	if err != nil {
		return nil, err
	}
	result := make([]FailedSessionInfo, 0, len(sessions))
	for _, session := range sessions {
		if session.Status != FailedSessionStatusActive {
			result = append(result, session)
		}
	}
	return result, nil
}

func (manager *PoolSessionManager) GetFailedSession(sessionID string) (*FailedSessionInfo, error) {
	session, err := manager.getStoredSession(sessionID)
	if err != nil || session == nil {
		return session, err
	}
	if session.Status == FailedSessionStatusActive {
		return nil, nil
	}
	return session, nil
}

func (manager *PoolSessionManager) getStoredSession(sessionID string) (*FailedSessionInfo, error) {
	manager.failedSessionMutex.Lock()
	defer manager.failedSessionMutex.Unlock()

	if manager.failedSessionDB == nil {
		return nil, nil
	}

	var info FailedSessionInfo
	err := manager.failedSessionDB.View(func(transaction *badger.Txn) error {
		item, err := transaction.Get([]byte(failedSessionKeyPrefix + sessionID))
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			return json.Unmarshal(value, &info)
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to read failed session")
	}
	return &info, nil
}

func (manager *PoolSessionManager) getAllStoredSessionsLocked() ([]FailedSessionInfo, error) {
	result := []FailedSessionInfo{}
	if manager.failedSessionDB == nil {
		return result, nil
	}

	err := manager.failedSessionDB.View(func(transaction *badger.Txn) error {
		iteratorOptions := badger.DefaultIteratorOptions
		iteratorOptions.Prefix = []byte(failedSessionKeyPrefix)
		iterator := transaction.NewIterator(iteratorOptions)
		defer iterator.Close()

		prefix := []byte(failedSessionKeyPrefix)
		for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
			item := iterator.Item()
			if err := item.Value(func(value []byte) error {
				var info FailedSessionInfo
				if err := json.Unmarshal(value, &info); err != nil {
					return errors.Wrap(err, "failed to unmarshal failed session")
				}
				result = append(result, info)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (manager *PoolSessionManager) RemoveFailedSession(sessionID string) error {
	manager.failedSessionMutex.Lock()
	defer manager.failedSessionMutex.Unlock()

	if manager.failedSessionDB == nil {
		return nil
	}
	if err := manager.failedSessionDB.Update(func(transaction *badger.Txn) error {
		return transaction.Delete([]byte(failedSessionKeyPrefix + sessionID))
	}); err != nil {
		return errors.Wrap(err, "failed to delete failed session")
	}

	sessions, err := manager.getAllStoredSessionsLocked()
	if err != nil {
		return err
	}
	if len(sessions) == 0 {
		return manager.removeFailedSessionStoreLocked()
	}
	return nil
}

func (manager *PoolSessionManager) removeFailedSessionStoreLocked() error {
	var closeErr error
	if manager.failedSessionDB != nil {
		closeErr = manager.failedSessionDB.Close()
		manager.failedSessionDB = nil
	}
	if closeErr != nil {
		return errors.Wrap(closeErr, "failed to close empty failed session store")
	}
	if err := os.RemoveAll(manager.failedSessionDBPath); err != nil {
		return errors.Wrap(err, "failed to remove empty failed session store")
	}
	return nil
}
