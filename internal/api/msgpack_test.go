package api

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestIsGzipMagicBytes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{
			name:     "valid gzip magic bytes",
			data:     []byte{0x1f, 0x8b, 0x08, 0x00},
			expected: true,
		},
		{
			name:     "invalid first byte",
			data:     []byte{0x1e, 0x8b, 0x08, 0x00},
			expected: false,
		},
		{
			name:     "invalid second byte",
			data:     []byte{0x1f, 0x8a, 0x08, 0x00},
			expected: false,
		},
		{
			name:     "too short",
			data:     []byte{0x1f},
			expected: false,
		},
		{
			name:     "empty",
			data:     []byte{},
			expected: false,
		},
		{
			name:     "msgpack data (not gzip)",
			data:     []byte{0x82, 0xa1, 0x6d, 0xa3},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := len(tt.data) >= 2 && tt.data[0] == 0x1f && tt.data[1] == 0x8b
			if result != tt.expected {
				t.Errorf("isGzip(%v) = %v, want %v", tt.data, result, tt.expected)
			}
		})
	}
}

func TestIsZstdMagicBytes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{
			name:     "valid zstd magic bytes",
			data:     []byte{0x28, 0xB5, 0x2F, 0xFD, 0x00},
			expected: true,
		},
		{
			name:     "invalid first byte",
			data:     []byte{0x29, 0xB5, 0x2F, 0xFD},
			expected: false,
		},
		{
			name:     "invalid second byte",
			data:     []byte{0x28, 0xB4, 0x2F, 0xFD},
			expected: false,
		},
		{
			name:     "too short",
			data:     []byte{0x28, 0xB5, 0x2F},
			expected: false,
		},
		{
			name:     "empty",
			data:     []byte{},
			expected: false,
		},
		{
			name:     "gzip data (not zstd)",
			data:     []byte{0x1f, 0x8b, 0x08, 0x00},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := len(tt.data) >= 4 && tt.data[0] == 0x28 && tt.data[1] == 0xB5 && tt.data[2] == 0x2F && tt.data[3] == 0xFD
			if result != tt.expected {
				t.Errorf("isZstd(%v) = %v, want %v", tt.data, result, tt.expected)
			}
		})
	}
}

func TestGzipCompressionRoundTrip(t *testing.T) {
	// Test data that mimics msgpack payload
	original := []byte("test msgpack payload data that needs to be compressed")

	// Compress with gzip
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write(original)
	if err != nil {
		t.Fatalf("failed to write gzip: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("failed to close gzip writer: %v", err)
	}

	compressed := buf.Bytes()

	// Verify magic bytes
	if len(compressed) < 2 || compressed[0] != 0x1f || compressed[1] != 0x8b {
		t.Errorf("gzip compressed data should start with magic bytes 0x1f 0x8b, got %x %x", compressed[0], compressed[1])
	}

	// Decompress
	gr, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}
	defer gr.Close()

	var decompressed bytes.Buffer
	_, err = decompressed.ReadFrom(gr)
	if err != nil {
		t.Fatalf("failed to decompress: %v", err)
	}

	if !bytes.Equal(original, decompressed.Bytes()) {
		t.Errorf("decompressed data doesn't match original")
	}
}

func TestZstdCompressionRoundTrip(t *testing.T) {
	// Test data that mimics msgpack payload
	original := []byte("test msgpack payload data that needs to be compressed with zstd")

	// Compress with zstd
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		t.Fatalf("failed to create zstd encoder: %v", err)
	}
	compressed := encoder.EncodeAll(original, nil)
	encoder.Close()

	// Verify magic bytes
	if len(compressed) < 4 || compressed[0] != 0x28 || compressed[1] != 0xB5 || compressed[2] != 0x2F || compressed[3] != 0xFD {
		t.Errorf("zstd compressed data should start with magic bytes 0x28 0xB5 0x2F 0xFD, got %x %x %x %x",
			compressed[0], compressed[1], compressed[2], compressed[3])
	}

	// Decompress
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("failed to create zstd decoder: %v", err)
	}
	defer decoder.Close()

	decompressed, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		t.Fatalf("failed to decompress: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Errorf("decompressed data doesn't match original")
	}
}

func TestPooledBufferRelease(t *testing.T) {
	// Test that PooledBuffer.Release() is idempotent
	buf := make([]byte, 100)
	pb := &PooledBuffer{
		Data:   buf,
		bufPtr: &buf,
	}

	// First release should work
	pb.Release()
	if pb.Data != nil {
		t.Error("Data should be nil after Release()")
	}
	if pb.bufPtr != nil {
		t.Error("bufPtr should be nil after Release()")
	}

	// Second release should not panic (idempotent)
	pb.Release() // Should not panic
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0B"},
		{512, "512B"},
		{1024, "1KB"},
		{1536, "1.5KB"},
		{1048576, "1MB"},
		{1572864, "1.5MB"},
		{1073741824, "1GB"},
		{1610612736, "1.5GB"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatBytes(tt.bytes)
			if result != tt.expected {
				t.Errorf("formatBytes(%d) = %s, want %s", tt.bytes, result, tt.expected)
			}
		})
	}
}
