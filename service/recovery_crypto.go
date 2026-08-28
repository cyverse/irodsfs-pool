package service

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
)

const (
	recoveryAccountEncryptionVersion   = 1
	recoveryAccountEncryptionAlgorithm = "AES-256-GCM"
)

type EncryptedIRODSAccount struct {
	Version    int    `json:"version"`
	Algorithm  string `json:"algorithm"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

type recoveryAccountCipher struct {
	aead cipher.AEAD
}

func newRecoveryAccountCipher(key []byte) (*recoveryAccountCipher, error) {
	if len(key) != 32 {
		return nil, errors.Errorf("recovery encryption key must be exactly 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create recovery account cipher")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create recovery account AEAD")
	}
	return &recoveryAccountCipher{aead: aead}, nil
}

func (c *recoveryAccountCipher) Encrypt(account *irodsclient_types.IRODSAccount, sessionID string, accountKey string) (EncryptedIRODSAccount, error) {
	if account == nil {
		return EncryptedIRODSAccount{}, errors.New("iRODS account is required")
	}
	plaintext, err := json.Marshal(account)
	if err != nil {
		return EncryptedIRODSAccount{}, errors.Wrap(err, "failed to marshal iRODS account")
	}
	defer clear(plaintext)

	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return EncryptedIRODSAccount{}, errors.Wrap(err, "failed to generate recovery account nonce")
	}
	ciphertext := c.aead.Seal(nil, nonce, plaintext, recoveryAccountAAD(sessionID, accountKey))
	return EncryptedIRODSAccount{
		Version:    recoveryAccountEncryptionVersion,
		Algorithm:  recoveryAccountEncryptionAlgorithm,
		Nonce:      base64.StdEncoding.EncodeToString(nonce),
		Ciphertext: base64.StdEncoding.EncodeToString(ciphertext),
	}, nil
}

func (c *recoveryAccountCipher) Decrypt(encrypted EncryptedIRODSAccount, sessionID string, accountKey string) (*irodsclient_types.IRODSAccount, error) {
	if encrypted.Version != recoveryAccountEncryptionVersion {
		return nil, errors.Errorf("unsupported recovery account encryption version %d", encrypted.Version)
	}
	if encrypted.Algorithm != recoveryAccountEncryptionAlgorithm {
		return nil, errors.Errorf("unsupported recovery account encryption algorithm %q", encrypted.Algorithm)
	}
	nonce, err := base64.StdEncoding.DecodeString(encrypted.Nonce)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode recovery account nonce")
	}
	if len(nonce) != c.aead.NonceSize() {
		return nil, errors.Errorf("invalid recovery account nonce size %d", len(nonce))
	}
	ciphertext, err := base64.StdEncoding.DecodeString(encrypted.Ciphertext)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode recovery account ciphertext")
	}
	plaintext, err := c.aead.Open(nil, nonce, ciphertext, recoveryAccountAAD(sessionID, accountKey))
	if err != nil {
		return nil, errors.Wrap(err, "failed to decrypt recovery account")
	}
	defer clear(plaintext)

	var account irodsclient_types.IRODSAccount
	if err := json.Unmarshal(plaintext, &account); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal recovery account")
	}
	return &account, nil
}

func recoveryAccountAAD(sessionID string, accountKey string) []byte {
	return []byte(fmt.Sprintf("irodsfs-pool/recovery-account/v%d\x00%s\x00%s", recoveryAccountEncryptionVersion, sessionID, accountKey))
}
