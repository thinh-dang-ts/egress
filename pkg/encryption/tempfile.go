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
	"encoding/binary"
	"io"
	"os"
)

// EncryptedTempFile provides streaming encryption for temporary files
// It writes encrypted data chunk by chunk to avoid buffering entire files in memory
type EncryptedTempFile struct {
	file      *os.File
	encryptor *EnvelopeEncryptor
	dek       []byte
	dekGCM    cipher.AEAD
	nonce     []byte
	counter   uint64
	written   int64
	closed    bool

	// Buffer for chunked encryption
	chunkSize int
	buffer    []byte
	bufOffset int
}

const (
	// DefaultChunkSize is the default size for encryption chunks
	DefaultChunkSize = 64 * 1024 // 64KB chunks

	// ChunkHeaderSize is the size of each chunk's header (nonce + length)
	ChunkHeaderSize = NonceSize + 4
)

// NewEncryptedTempFile creates a new encrypted temporary file
func NewEncryptedTempFile(filepath string, encryptor *EnvelopeEncryptor) (*EncryptedTempFile, error) {
	return NewEncryptedTempFileWithChunkSize(filepath, encryptor, DefaultChunkSize)
}

// NewEncryptedTempFileWithChunkSize creates a new encrypted temporary file with custom chunk size
func NewEncryptedTempFileWithChunkSize(filepath string, encryptor *EnvelopeEncryptor, chunkSize int) (*EncryptedTempFile, error) {
	file, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}

	// Generate random DEK for this file
	dek := make([]byte, DEKSize)
	if _, err := rand.Read(dek); err != nil {
		file.Close()
		return nil, err
	}

	dekBlock, err := aes.NewCipher(dek)
	if err != nil {
		file.Close()
		return nil, err
	}

	dekGCM, err := cipher.NewGCM(dekBlock)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Generate base nonce
	nonce := make([]byte, NonceSize)
	if _, err := rand.Read(nonce); err != nil {
		file.Close()
		return nil, err
	}

	etf := &EncryptedTempFile{
		file:      file,
		encryptor: encryptor,
		dek:       dek,
		dekGCM:    dekGCM,
		nonce:     nonce,
		chunkSize: chunkSize,
		buffer:    make([]byte, chunkSize),
	}

	// Write file header
	if err := etf.writeHeader(); err != nil {
		file.Close()
		return nil, err
	}

	return etf, nil
}

// writeHeader writes the encrypted file header
func (f *EncryptedTempFile) writeHeader() error {
	// Encrypt DEK with master key
	dekNonce := make([]byte, NonceSize)
	if _, err := rand.Read(dekNonce); err != nil {
		return err
	}

	encryptedDEK := f.encryptor.masterGCM.Seal(nil, dekNonce, f.dek, nil)

	// Write header: magic (4) + version (1) + encrypted DEK length (2) + encrypted DEK + DEK nonce + base data nonce
	header := make([]byte, 4+1+2+len(encryptedDEK)+NonceSize+NonceSize)
	offset := 0

	copy(header[offset:], []byte(HeaderMagic))
	offset += 4

	header[offset] = HeaderVersion
	offset++

	binary.BigEndian.PutUint16(header[offset:], uint16(len(encryptedDEK)))
	offset += 2

	copy(header[offset:], encryptedDEK)
	offset += len(encryptedDEK)

	copy(header[offset:], dekNonce)
	offset += NonceSize

	copy(header[offset:], f.nonce)

	_, err := f.file.Write(header)
	return err
}

// Write implements io.Writer
func (f *EncryptedTempFile) Write(p []byte) (n int, err error) {
	if f.closed {
		return 0, os.ErrClosed
	}

	totalWritten := 0

	for len(p) > 0 {
		// Fill buffer
		space := f.chunkSize - f.bufOffset
		toCopy := min(space, len(p))
		copy(f.buffer[f.bufOffset:], p[:toCopy])
		f.bufOffset += toCopy
		p = p[toCopy:]
		totalWritten += toCopy

		// Flush if buffer is full
		if f.bufOffset >= f.chunkSize {
			if err := f.flushChunk(); err != nil {
				return totalWritten, err
			}
		}
	}

	f.written += int64(totalWritten)
	return totalWritten, nil
}

// flushChunk encrypts and writes the current buffer
func (f *EncryptedTempFile) flushChunk() error {
	if f.bufOffset == 0 {
		return nil
	}

	// Create unique nonce for this chunk by XORing base nonce with counter
	chunkNonce := make([]byte, NonceSize)
	copy(chunkNonce, f.nonce)
	counterBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(counterBytes, f.counter)
	for i := 0; i < 8; i++ {
		chunkNonce[i] ^= counterBytes[i]
	}

	// Encrypt chunk
	encrypted := f.dekGCM.Seal(nil, chunkNonce, f.buffer[:f.bufOffset], nil)

	// Write chunk header: nonce + length
	header := make([]byte, ChunkHeaderSize)
	copy(header[:NonceSize], chunkNonce)
	binary.BigEndian.PutUint32(header[NonceSize:], uint32(len(encrypted)))

	if _, err := f.file.Write(header); err != nil {
		return err
	}

	if _, err := f.file.Write(encrypted); err != nil {
		return err
	}

	f.counter++
	f.bufOffset = 0
	return nil
}

// Close flushes remaining data and closes the file
func (f *EncryptedTempFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true

	// Flush remaining buffer
	if err := f.flushChunk(); err != nil {
		f.file.Close()
		return err
	}

	return f.file.Close()
}

// Sync syncs the file to disk
func (f *EncryptedTempFile) Sync() error {
	return f.file.Sync()
}

// Name returns the file path
func (f *EncryptedTempFile) Name() string {
	return f.file.Name()
}

// BytesWritten returns the number of plaintext bytes written
func (f *EncryptedTempFile) BytesWritten() int64 {
	return f.written
}

// EncryptedTempFileReader reads and decrypts an encrypted temp file
type EncryptedTempFileReader struct {
	file      *os.File
	encryptor *EnvelopeEncryptor
	dekGCM    cipher.AEAD
	buffer    []byte
	bufOffset int
	bufLen    int
	eof       bool
}

// NewEncryptedTempFileReader opens an encrypted temp file for reading
func NewEncryptedTempFileReader(filepath string, encryptor *EnvelopeEncryptor) (*EncryptedTempFileReader, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	reader := &EncryptedTempFileReader{
		file:      file,
		encryptor: encryptor,
	}

	// Read and parse header
	if err := reader.readHeader(); err != nil {
		file.Close()
		return nil, err
	}

	return reader, nil
}

// readHeader reads and validates the file header
func (r *EncryptedTempFileReader) readHeader() error {
	// Read magic + version + DEK length
	header := make([]byte, 7)
	if _, err := io.ReadFull(r.file, header); err != nil {
		return err
	}

	if string(header[:4]) != HeaderMagic {
		return ErrInvalidMagic
	}

	if header[4] != HeaderVersion {
		return ErrUnsupportedVersion
	}

	encryptedDEKLen := binary.BigEndian.Uint16(header[5:])

	// Read encrypted DEK + nonces
	data := make([]byte, int(encryptedDEKLen)+NonceSize+NonceSize)
	if _, err := io.ReadFull(r.file, data); err != nil {
		return err
	}

	encryptedDEK := data[:encryptedDEKLen]
	dekNonce := data[encryptedDEKLen : encryptedDEKLen+uint16(NonceSize)]
	// baseDataNonce is stored but we read per-chunk nonces
	// _ = data[encryptedDEKLen+uint16(NonceSize):]

	// Decrypt DEK
	dek, err := r.encryptor.masterGCM.Open(nil, dekNonce, encryptedDEK, nil)
	if err != nil {
		return ErrDecryptionFailed
	}

	// Create cipher for data decryption
	dekBlock, err := aes.NewCipher(dek)
	if err != nil {
		return err
	}

	r.dekGCM, err = cipher.NewGCM(dekBlock)
	if err != nil {
		return err
	}

	return nil
}

// Read implements io.Reader
func (r *EncryptedTempFileReader) Read(p []byte) (n int, err error) {
	if r.eof && r.bufOffset >= r.bufLen {
		return 0, io.EOF
	}

	totalRead := 0

	for len(p) > 0 {
		// If buffer is empty, read next chunk
		if r.bufOffset >= r.bufLen {
			if err := r.readChunk(); err != nil {
				if err == io.EOF {
					r.eof = true
					if totalRead > 0 {
						return totalRead, nil
					}
					return 0, io.EOF
				}
				return totalRead, err
			}
		}

		// Copy from buffer
		n := copy(p, r.buffer[r.bufOffset:r.bufLen])
		r.bufOffset += n
		p = p[n:]
		totalRead += n
	}

	return totalRead, nil
}

// readChunk reads and decrypts the next chunk
func (r *EncryptedTempFileReader) readChunk() error {
	// Read chunk header
	header := make([]byte, ChunkHeaderSize)
	if _, err := io.ReadFull(r.file, header); err != nil {
		return err
	}

	chunkNonce := header[:NonceSize]
	encryptedLen := binary.BigEndian.Uint32(header[NonceSize:])

	// Read encrypted data
	encrypted := make([]byte, encryptedLen)
	if _, err := io.ReadFull(r.file, encrypted); err != nil {
		return err
	}

	// Decrypt
	plaintext, err := r.dekGCM.Open(nil, chunkNonce, encrypted, nil)
	if err != nil {
		return ErrDecryptionFailed
	}

	r.buffer = plaintext
	r.bufOffset = 0
	r.bufLen = len(plaintext)
	return nil
}

// Close closes the reader
func (r *EncryptedTempFileReader) Close() error {
	return r.file.Close()
}
