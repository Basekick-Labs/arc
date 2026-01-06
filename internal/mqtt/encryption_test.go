package mqtt

import (
	"testing"
)

func TestAESEncryptor_EncryptDecrypt(t *testing.T) {
	// Generate a valid 32-byte key
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	encryptor, err := NewAESEncryptor(key)
	if err != nil {
		t.Fatalf("Failed to create encryptor: %v", err)
	}

	tests := []struct {
		name      string
		plaintext string
	}{
		{"empty", ""},
		{"simple", "password123"},
		{"unicode", "пароль密码🔐"},
		{"long", "this is a very long password that exceeds typical length limits for testing purposes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encrypted, err := encryptor.Encrypt(tt.plaintext)
			if err != nil {
				t.Fatalf("Encrypt failed: %v", err)
			}

			// Empty plaintext returns empty ciphertext
			if tt.plaintext == "" {
				if encrypted != "" {
					t.Errorf("Expected empty ciphertext for empty plaintext")
				}
				return
			}

			// Ciphertext should be different from plaintext
			if encrypted == tt.plaintext {
				t.Errorf("Ciphertext should not equal plaintext")
			}

			// Decrypt should return original
			decrypted, err := encryptor.Decrypt(encrypted)
			if err != nil {
				t.Fatalf("Decrypt failed: %v", err)
			}

			if decrypted != tt.plaintext {
				t.Errorf("Decrypted = %q, want %q", decrypted, tt.plaintext)
			}
		})
	}
}

func TestAESEncryptor_InvalidKeySize(t *testing.T) {
	invalidKeys := [][]byte{
		nil,
		{},
		make([]byte, 16), // AES-128
		make([]byte, 24), // AES-192
		make([]byte, 31), // One byte short
		make([]byte, 33), // One byte too long
	}

	for _, key := range invalidKeys {
		_, err := NewAESEncryptor(key)
		if err == nil {
			t.Errorf("Expected error for key length %d, got nil", len(key))
		}
	}
}

func TestNoPasswordEncryptor(t *testing.T) {
	encryptor := &NoPasswordEncryptor{}

	// Empty string should succeed
	encrypted, err := encryptor.Encrypt("")
	if err != nil {
		t.Errorf("Empty encrypt should succeed: %v", err)
	}
	if encrypted != "" {
		t.Errorf("Expected empty result")
	}

	// Non-empty string should fail
	_, err = encryptor.Encrypt("password")
	if err == nil {
		t.Error("Expected error for non-empty password")
	}

	// Decrypt empty should succeed
	decrypted, err := encryptor.Decrypt("")
	if err != nil {
		t.Errorf("Empty decrypt should succeed: %v", err)
	}
	if decrypted != "" {
		t.Errorf("Expected empty result")
	}

	// Decrypt non-empty should fail
	_, err = encryptor.Decrypt("somedata")
	if err == nil {
		t.Error("Expected error for non-empty ciphertext")
	}
}

func TestNewPasswordEncryptor(t *testing.T) {
	// Nil key returns NoPasswordEncryptor
	enc, err := NewPasswordEncryptor(nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, ok := enc.(*NoPasswordEncryptor); !ok {
		t.Error("Expected NoPasswordEncryptor for nil key")
	}

	// Empty key returns NoPasswordEncryptor
	enc, err = NewPasswordEncryptor([]byte{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, ok := enc.(*NoPasswordEncryptor); !ok {
		t.Error("Expected NoPasswordEncryptor for empty key")
	}

	// Valid key returns AESEncryptor
	key := make([]byte, 32)
	enc, err = NewPasswordEncryptor(key)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, ok := enc.(*AESEncryptor); !ok {
		t.Error("Expected AESEncryptor for valid key")
	}
}

func TestParseEncryptionKey(t *testing.T) {
	tests := []struct {
		name    string
		encoded string
		wantErr bool
		wantNil bool
	}{
		{"empty", "", false, true},
		{"valid_base64_32bytes", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", false, false},
		{"invalid_base64", "not-valid-base64!", true, false},
		{"wrong_length", "AAAA", true, false}, // Valid base64 but wrong decoded length
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := ParseEncryptionKey(tt.encoded)

			if tt.wantErr && err == nil {
				t.Error("Expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if tt.wantNil && key != nil {
				t.Error("Expected nil key")
			}
			if !tt.wantNil && !tt.wantErr && key == nil {
				t.Error("Expected non-nil key")
			}
		})
	}
}

func TestGenerateEncryptionKey(t *testing.T) {
	key1, err := GenerateEncryptionKey()
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	if key1 == "" {
		t.Error("Generated key should not be empty")
	}

	// Should be valid base64
	decoded, err := ParseEncryptionKey(key1)
	if err != nil {
		t.Errorf("Generated key should be valid: %v", err)
	}
	if len(decoded) != 32 {
		t.Errorf("Decoded key should be 32 bytes, got %d", len(decoded))
	}

	// Two generated keys should be different
	key2, _ := GenerateEncryptionKey()
	if key1 == key2 {
		t.Error("Two generated keys should be different")
	}
}
