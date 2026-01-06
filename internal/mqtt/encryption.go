package mqtt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
)

// PasswordEncryptor handles password encryption/decryption
type PasswordEncryptor interface {
	Encrypt(plaintext string) (string, error)
	Decrypt(ciphertext string) (string, error)
}

// AESEncryptor implements AES-256-GCM encryption
type AESEncryptor struct {
	gcm cipher.AEAD
}

// NewAESEncryptor creates a new AES-256-GCM encryptor
// Key must be 32 bytes (256 bits)
func NewAESEncryptor(key []byte) (*AESEncryptor, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("encryption key must be 32 bytes, got %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	return &AESEncryptor{gcm: gcm}, nil
}

// Encrypt encrypts plaintext using AES-256-GCM
// Returns base64-encoded ciphertext with prepended nonce
func (e *AESEncryptor) Encrypt(plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}

	// Generate random nonce
	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt and prepend nonce
	ciphertext := e.gcm.Seal(nonce, nonce, []byte(plaintext), nil)

	// Return base64 encoded
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt decrypts base64-encoded ciphertext using AES-256-GCM
func (e *AESEncryptor) Decrypt(ciphertext string) (string, error) {
	if ciphertext == "" {
		return "", nil
	}

	// Decode base64
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("failed to decode base64: %w", err)
	}

	nonceSize := e.gcm.NonceSize()
	if len(data) < nonceSize {
		return "", errors.New("ciphertext too short")
	}

	// Extract nonce and ciphertext
	nonce, encrypted := data[:nonceSize], data[nonceSize:]

	// Decrypt
	plaintext, err := e.gcm.Open(nil, nonce, encrypted, nil)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt: %w", err)
	}

	return string(plaintext), nil
}

// NoPasswordEncryptor refuses to encrypt passwords (used when no key is configured)
// This enforces that passwords require encryption - no plaintext storage
type NoPasswordEncryptor struct{}

// Encrypt returns an error if plaintext is provided (passwords require encryption key)
func (e *NoPasswordEncryptor) Encrypt(plaintext string) (string, error) {
	if plaintext != "" {
		return "", errors.New("encryption key required for passwords - set ARC_ENCRYPTION_KEY environment variable")
	}
	return "", nil
}

// Decrypt returns an error (can't decrypt without key)
func (e *NoPasswordEncryptor) Decrypt(ciphertext string) (string, error) {
	if ciphertext != "" {
		return "", errors.New("encryption key required to decrypt passwords - set ARC_ENCRYPTION_KEY environment variable")
	}
	return "", nil
}

// NewPasswordEncryptor creates an appropriate encryptor based on whether a key is provided
// If key is nil, returns NoPasswordEncryptor which refuses to encrypt passwords
// If key is provided, returns AESEncryptor for secure password storage
func NewPasswordEncryptor(key []byte) (PasswordEncryptor, error) {
	if len(key) == 0 {
		return &NoPasswordEncryptor{}, nil
	}
	return NewAESEncryptor(key)
}

// ParseEncryptionKey parses a base64-encoded encryption key
func ParseEncryptionKey(encoded string) ([]byte, error) {
	if encoded == "" {
		return nil, nil
	}

	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 encoding for encryption key: %w", err)
	}

	if len(key) != 32 {
		return nil, fmt.Errorf("encryption key must be 32 bytes when decoded, got %d", len(key))
	}

	return key, nil
}

// GenerateEncryptionKey generates a random 32-byte encryption key
// Returns the key as base64-encoded string
func GenerateEncryptionKey() (string, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return "", fmt.Errorf("failed to generate random key: %w", err)
	}
	return base64.StdEncoding.EncodeToString(key), nil
}

// GetEncryptionKey reads the encryption key from environment variable
// Returns nil if not set (passwords will be rejected)
// Returns error if set but invalid
func GetEncryptionKey() ([]byte, error) {
	encoded := os.Getenv("ARC_ENCRYPTION_KEY")
	if encoded == "" {
		return nil, nil
	}
	return ParseEncryptionKey(encoded)
}
