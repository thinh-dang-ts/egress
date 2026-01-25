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
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateMasterKey(t *testing.T) {
	key, err := GenerateMasterKey()
	if err != nil {
		t.Fatalf("failed to generate master key: %v", err)
	}

	if len(key) != AES256KeySize {
		t.Errorf("expected key size %d, got %d", AES256KeySize, len(key))
	}

	// Generate another key and verify they're different
	key2, err := GenerateMasterKey()
	if err != nil {
		t.Fatalf("failed to generate second master key: %v", err)
	}

	if bytes.Equal(key, key2) {
		t.Error("two generated keys should not be equal")
	}
}

func TestGenerateMasterKeyBase64(t *testing.T) {
	keyBase64, err := GenerateMasterKeyBase64()
	if err != nil {
		t.Fatalf("failed to generate master key base64: %v", err)
	}

	// Verify it's valid base64
	decoded, err := base64.StdEncoding.DecodeString(keyBase64)
	if err != nil {
		t.Fatalf("failed to decode base64 key: %v", err)
	}

	if len(decoded) != AES256KeySize {
		t.Errorf("expected decoded key size %d, got %d", AES256KeySize, len(decoded))
	}

	t.Logf("Generated base64 key: %s", keyBase64)
}

func TestEnvelopeEncryptorInvalidKeySize(t *testing.T) {
	shortKey := make([]byte, 16) // Too short
	_, err := NewEnvelopeEncryptor(shortKey)
	if err != ErrInvalidKeySize {
		t.Errorf("expected ErrInvalidKeySize, got %v", err)
	}
}

func TestEnvelopeEncryptDecrypt(t *testing.T) {
	// Generate a master key
	masterKey, err := GenerateMasterKey()
	if err != nil {
		t.Fatalf("failed to generate master key: %v", err)
	}

	encryptor, err := NewEnvelopeEncryptor(masterKey)
	if err != nil {
		t.Fatalf("failed to create encryptor: %v", err)
	}

	// Test data
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("Hello, World!")},
		{"medium", bytes.Repeat([]byte("test data "), 100)},
		{"large", bytes.Repeat([]byte("x"), 1024*1024)}, // 1MB
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Encrypt
			encrypted, err := encryptor.Encrypt(tc.data)
			if err != nil {
				t.Fatalf("failed to encrypt: %v", err)
			}

			// Encrypted data should be longer due to header
			if len(encrypted) < len(tc.data) {
				t.Errorf("encrypted data should be longer than plaintext")
			}

			// Decrypt
			decrypted, err := encryptor.Decrypt(encrypted)
			if err != nil {
				t.Fatalf("failed to decrypt: %v", err)
			}

			// Verify
			if !bytes.Equal(decrypted, tc.data) {
				t.Errorf("decrypted data doesn't match original")
			}
		})
	}
}

func TestEnvelopeEncryptorFromBase64(t *testing.T) {
	keyBase64, err := GenerateMasterKeyBase64()
	if err != nil {
		t.Fatalf("failed to generate master key: %v", err)
	}

	encryptor, err := NewEnvelopeEncryptorFromBase64(keyBase64)
	if err != nil {
		t.Fatalf("failed to create encryptor from base64: %v", err)
	}

	// Test encrypt/decrypt
	plaintext := []byte("test data for base64 key")
	encrypted, err := encryptor.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	decrypted, err := encryptor.Decrypt(encrypted)
	if err != nil {
		t.Fatalf("failed to decrypt: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Error("decrypted data doesn't match original")
	}
}

func TestDecryptInvalidData(t *testing.T) {
	masterKey, _ := GenerateMasterKey()
	encryptor, _ := NewEnvelopeEncryptor(masterKey)

	testCases := []struct {
		name string
		data []byte
		err  error
	}{
		{"too_short", []byte("short"), ErrInvalidHeader},
		{"invalid_magic", append([]byte("XXXX"), make([]byte, 100)...), ErrInvalidMagic},
		{"invalid_version", append([]byte("LKAE\x99"), make([]byte, 100)...), ErrUnsupportedVersion},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := encryptor.Decrypt(tc.data)
			if err != tc.err {
				t.Errorf("expected error %v, got %v", tc.err, err)
			}
		})
	}
}

func TestEncryptedTempFile(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_encrypted.bin")

	// Generate master key and create encryptor
	masterKey, _ := GenerateMasterKey()
	encryptor, _ := NewEnvelopeEncryptor(masterKey)

	// Test data - write multiple chunks
	testData := bytes.Repeat([]byte("This is test data for encrypted temp file. "), 10000)

	// Write encrypted file
	writer, err := NewEncryptedTempFile(testFile, encryptor)
	if err != nil {
		t.Fatalf("failed to create encrypted temp file: %v", err)
	}

	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if n != len(testData) {
		t.Errorf("expected to write %d bytes, wrote %d", len(testData), n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Verify file exists and has content
	info, err := os.Stat(testFile)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	t.Logf("Encrypted file size: %d (plaintext was %d)", info.Size(), len(testData))

	// Read and decrypt
	reader, err := NewEncryptedTempFileReader(testFile, encryptor)
	if err != nil {
		t.Fatalf("failed to open encrypted file: %v", err)
	}
	defer reader.Close()

	var decrypted bytes.Buffer
	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			decrypted.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}

	if !bytes.Equal(decrypted.Bytes(), testData) {
		t.Errorf("decrypted data doesn't match original (got %d bytes, expected %d)",
			decrypted.Len(), len(testData))
	}
}

func TestEncryptReader(t *testing.T) {
	masterKey, _ := GenerateMasterKey()
	encryptor, _ := NewEnvelopeEncryptor(masterKey)

	testData := []byte("Test data for encrypt reader")
	reader := NewEncryptReader(bytes.NewReader(testData), encryptor)

	var encrypted bytes.Buffer
	buf := make([]byte, 1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			encrypted.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}

	// Decrypt to verify
	decrypted, err := encryptor.Decrypt(encrypted.Bytes())
	if err != nil {
		t.Fatalf("failed to decrypt: %v", err)
	}

	if !bytes.Equal(decrypted, testData) {
		t.Error("decrypted data doesn't match original")
	}
}
