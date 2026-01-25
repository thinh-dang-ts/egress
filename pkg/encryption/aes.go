// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
)

const (
	// AES256KeySize is the required key size for AES-256
	AES256KeySize = 32

	// NonceSize is the standard GCM nonce size
	NonceSize = 12

	// DEKSize is the size of the Data Encryption Key
	DEKSize = 32

	// HeaderMagic is the magic bytes for encrypted file header
	HeaderMagic = "LKAE" // LiveKit Audio Encrypted

	// HeaderVersion is the current envelope format version
	HeaderVersion = uint8(1)

	// MinEncryptedHeaderSize is minimum size of the encrypted file header
	// Magic (4) + Version (1) + Encrypted DEK length (2) + Nonce (12) + min encrypted DEK (~48)
	MinEncryptedHeaderSize = 67
)

var (
	ErrInvalidKeySize        = errors.New("invalid key size: must be 32 bytes for AES-256")
	ErrInvalidHeader         = errors.New("invalid encrypted file header")
	ErrInvalidMagic          = errors.New("invalid magic bytes in header")
	ErrUnsupportedVersion    = errors.New("unsupported encryption version")
	ErrDecryptionFailed      = errors.New("decryption failed")
	ErrEncryptionFailed      = errors.New("encryption failed")
)

// EnvelopeEncryptor provides AES-256-GCM envelope encryption
// It generates a random Data Encryption Key (DEK) for each file,
// encrypts the data with the DEK, then encrypts the DEK with the Master Key (KEK)
type EnvelopeEncryptor struct {
	masterKey []byte
	masterGCM cipher.AEAD
}

// EncryptedHeader is the header prepended to encrypted files
type EncryptedHeader struct {
	Magic        [4]byte // "LKAE"
	Version      uint8
	EncryptedDEK []byte // DEK encrypted with master key
	Nonce        []byte // Nonce used for DEK encryption
}

// NewEnvelopeEncryptor creates a new envelope encryptor with the given master key
// The master key must be 32 bytes (256 bits) for AES-256
func NewEnvelopeEncryptor(masterKey []byte) (*EnvelopeEncryptor, error) {
	if len(masterKey) != AES256KeySize {
		return nil, ErrInvalidKeySize
	}

	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return &EnvelopeEncryptor{
		masterKey: masterKey,
		masterGCM: gcm,
	}, nil
}

// NewEnvelopeEncryptorFromBase64 creates a new envelope encryptor from a base64-encoded master key
func NewEnvelopeEncryptorFromBase64(masterKeyBase64 string) (*EnvelopeEncryptor, error) {
	masterKey, err := base64.StdEncoding.DecodeString(masterKeyBase64)
	if err != nil {
		return nil, err
	}
	return NewEnvelopeEncryptor(masterKey)
}

// GenerateMasterKey generates a new random master key
func GenerateMasterKey() ([]byte, error) {
	key := make([]byte, AES256KeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return key, nil
}

// GenerateMasterKeyBase64 generates a new random master key and returns it as base64
func GenerateMasterKeyBase64() (string, error) {
	key, err := GenerateMasterKey()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(key), nil
}

// Encrypt encrypts data using envelope encryption
// Returns the encrypted data with header prepended
func (e *EnvelopeEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	// Generate random DEK
	dek := make([]byte, DEKSize)
	if _, err := rand.Read(dek); err != nil {
		return nil, err
	}

	// Encrypt data with DEK
	dekBlock, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}

	dekGCM, err := cipher.NewGCM(dekBlock)
	if err != nil {
		return nil, err
	}

	dataNonce := make([]byte, NonceSize)
	if _, err := rand.Read(dataNonce); err != nil {
		return nil, err
	}

	encryptedData := dekGCM.Seal(nil, dataNonce, plaintext, nil)

	// Encrypt DEK with master key
	dekNonce := make([]byte, NonceSize)
	if _, err := rand.Read(dekNonce); err != nil {
		return nil, err
	}

	encryptedDEK := e.masterGCM.Seal(nil, dekNonce, dek, nil)

	// Build output: header + data nonce + encrypted data
	// Header: magic (4) + version (1) + encrypted DEK length (2) + encrypted DEK + DEK nonce
	headerSize := 4 + 1 + 2 + len(encryptedDEK) + NonceSize
	output := make([]byte, headerSize+NonceSize+len(encryptedData))

	offset := 0
	copy(output[offset:], []byte(HeaderMagic))
	offset += 4

	output[offset] = HeaderVersion
	offset++

	binary.BigEndian.PutUint16(output[offset:], uint16(len(encryptedDEK)))
	offset += 2

	copy(output[offset:], encryptedDEK)
	offset += len(encryptedDEK)

	copy(output[offset:], dekNonce)
	offset += NonceSize

	// Data nonce + encrypted data
	copy(output[offset:], dataNonce)
	offset += NonceSize

	copy(output[offset:], encryptedData)

	return output, nil
}

// Decrypt decrypts envelope-encrypted data
func (e *EnvelopeEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < MinEncryptedHeaderSize {
		return nil, ErrInvalidHeader
	}

	offset := 0

	// Check magic
	if string(ciphertext[offset:offset+4]) != HeaderMagic {
		return nil, ErrInvalidMagic
	}
	offset += 4

	// Check version
	version := ciphertext[offset]
	if version != HeaderVersion {
		return nil, ErrUnsupportedVersion
	}
	offset++

	// Read encrypted DEK length
	encryptedDEKLen := binary.BigEndian.Uint16(ciphertext[offset:])
	offset += 2

	if len(ciphertext) < offset+int(encryptedDEKLen)+NonceSize+NonceSize+16 {
		return nil, ErrInvalidHeader
	}

	// Read encrypted DEK
	encryptedDEK := ciphertext[offset : offset+int(encryptedDEKLen)]
	offset += int(encryptedDEKLen)

	// Read DEK nonce
	dekNonce := ciphertext[offset : offset+NonceSize]
	offset += NonceSize

	// Decrypt DEK
	dek, err := e.masterGCM.Open(nil, dekNonce, encryptedDEK, nil)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	// Create cipher for data decryption
	dekBlock, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}

	dekGCM, err := cipher.NewGCM(dekBlock)
	if err != nil {
		return nil, err
	}

	// Read data nonce
	dataNonce := ciphertext[offset : offset+NonceSize]
	offset += NonceSize

	// Decrypt data
	encryptedData := ciphertext[offset:]
	plaintext, err := dekGCM.Open(nil, dataNonce, encryptedData, nil)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	return plaintext, nil
}

// EncryptReader wraps a reader to encrypt data on-the-fly
type EncryptReader struct {
	r         io.Reader
	encryptor *EnvelopeEncryptor
	buf       []byte
	encrypted []byte
	offset    int
	done      bool
}

// NewEncryptReader creates a new encrypting reader
// Note: This buffers the entire input to encrypt it, suitable for smaller files
// For streaming encryption of large files, use EncryptedWriter instead
func NewEncryptReader(r io.Reader, encryptor *EnvelopeEncryptor) *EncryptReader {
	return &EncryptReader{
		r:         r,
		encryptor: encryptor,
	}
}

// Read implements io.Reader
func (r *EncryptReader) Read(p []byte) (n int, err error) {
	if !r.done && r.encrypted == nil {
		// Read all data first
		r.buf, err = io.ReadAll(r.r)
		if err != nil {
			return 0, err
		}

		// Encrypt
		r.encrypted, err = r.encryptor.Encrypt(r.buf)
		if err != nil {
			return 0, err
		}
		r.done = true
	}

	if r.offset >= len(r.encrypted) {
		return 0, io.EOF
	}

	n = copy(p, r.encrypted[r.offset:])
	r.offset += n
	return n, nil
}
