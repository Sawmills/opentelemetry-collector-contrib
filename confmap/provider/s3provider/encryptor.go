// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package s3provider // import "github.com/open-telemetry/opentelemetry-collector-contrib/confmap/provider/s3provider"

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

type Encryptor interface {
	Encrypt(content []byte) ([]byte, error)
	Decrypt(encrypted []byte) ([]byte, error)
	EncryptFile(srcPath, dstPath string) error
	DecryptFile(srcPath, dstPath string) error
}

type FileEncryptor struct {
	key     []byte
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

var _ Encryptor = (*FileEncryptor)(nil)

func NewFileEncryptor(keyString string) (Encryptor, error) {
	if len(keyString) < 32 {
		return nil, fmt.Errorf(
			"key string too short: minimum length is 32 characters, got %d",
			len(keyString),
		)
	}

	hasher := sha256.New()
	hasher.Write([]byte(keyString))
	key := hasher.Sum(nil)

	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &FileEncryptor{
		key:     key,
		encoder: encoder,
		decoder: decoder,
	}, nil
}

func NewFileEncryptorFromKey(key []byte) (Encryptor, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf(
			"invalid key length: expected 32 bytes for AES-256, got %d",
			len(key),
		)
	}

	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &FileEncryptor{
		key:     key,
		encoder: encoder,
		decoder: decoder,
	}, nil
}

func (f *FileEncryptor) EncryptFile(srcPath, dstPath string) error {
	content, err := os.ReadFile(srcPath)
	if err != nil {
		return fmt.Errorf("failed to read source file: %w", err)
	}

	encrypted, err := f.Encrypt(content)
	if err != nil {
		return fmt.Errorf("failed to encrypt content: %w", err)
	}

	if err := os.WriteFile(dstPath, encrypted, 0o600); err != nil {
		return fmt.Errorf("failed to write encrypted file: %w", err)
	}

	return nil
}

func (f *FileEncryptor) Encrypt(content []byte) ([]byte, error) {
	compressed := f.encoder.EncodeAll(content, make([]byte, 0, len(content)))

	block, err := aes.NewCipher(f.key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, aesgcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	return aesgcm.Seal(nonce, nonce, compressed, nil), nil
}

func (f *FileEncryptor) DecryptFile(srcPath, dstPath string) error {
	encrypted, err := os.ReadFile(srcPath)
	if err != nil {
		return fmt.Errorf("failed to read encrypted file: %w", err)
	}

	decrypted, err := f.Decrypt(encrypted)
	if err != nil {
		return fmt.Errorf("failed to decrypt content: %w", err)
	}

	if err := os.WriteFile(dstPath, decrypted, 0o600); err != nil {
		return fmt.Errorf("failed to write decrypted file: %w", err)
	}

	return nil
}

func (f *FileEncryptor) Decrypt(encrypted []byte) ([]byte, error) {
	if len(encrypted) < 12 {
		return nil, errors.New("encrypted content too short")
	}
	nonce := encrypted[:12]
	ciphertext := encrypted[12:]

	block, err := aes.NewCipher(f.key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	compressed, err := aesgcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt content: %w", err)
	}

	decompressed, err := f.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress content: %w", err)
	}

	return decompressed, nil
}

func GenerateKey() (string, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return "", fmt.Errorf("failed to generate random key: %w", err)
	}
	return base64.StdEncoding.EncodeToString(key), nil
}

func (f *FileEncryptor) Close() error {
	f.encoder.Close()
	f.decoder.Close()
	return nil
}
