package ingest

import (
	"strings"
	"testing"
)

func TestSanitizeUTF8_ValidString(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"ascii only", "Hello World"},
		{"with unicode", "Hello, 世界! Привет мир!"},
		{"with emojis", "Log message with emojis 🎉🚀💻"},
		{"mixed content", "user=admin action=login status=success café résumé naïve"},
		{"typical log", "2026-01-19T10:30:00Z INFO [server] Request processed in 42ms"},
		{"json content", `{"level":"info","msg":"test","value":123}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, modified := SanitizeUTF8(tt.input)
			if modified {
				t.Errorf("Expected no modification for valid UTF-8, got modified=true")
			}
			if result != tt.input {
				t.Errorf("Expected %q, got %q", tt.input, result)
			}
		})
	}
}

func TestSanitizeUTF8_InvalidBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "single invalid byte",
			input:    "Hello\x80World",
			expected: "Hello\ufffdWorld",
		},
		{
			name:     "multiple invalid bytes",
			input:    "\x80\x81\x82",
			expected: "\ufffd\ufffd\ufffd",
		},
		{
			name:     "invalid at start",
			input:    "\x80Hello",
			expected: "\ufffdHello",
		},
		{
			name:     "invalid at end",
			input:    "Hello\x80",
			expected: "Hello\ufffd",
		},
		{
			name:     "mixed valid and invalid",
			input:    "Hello\x80世界\x81Test",
			expected: "Hello\ufffd世界\ufffdTest",
		},
		{
			name:     "invalid in JSON-like content",
			input:    "{\"msg\":\"test\x80value\"}",
			expected: "{\"msg\":\"test\ufffdvalue\"}",
		},
		{
			name:     "latin1 high bytes",
			input:    "caf\xe9 r\xe9sum\xe9", // café résumé in Latin-1
			expected: "caf\ufffd r\ufffdsum\ufffd",
		},
		{
			name:     "truncated utf8 sequence",
			input:    "test\xc3", // incomplete 2-byte sequence
			expected: "test\ufffd",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, modified := SanitizeUTF8(tt.input)
			if !modified {
				t.Errorf("Expected modification for invalid UTF-8, got modified=false")
			}
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestSanitizeUTF8_EdgeCases(t *testing.T) {
	// Ensure we don't break valid edge cases
	tests := []struct {
		name     string
		input    string
		modified bool
	}{
		{"null byte in string", "hello\x00world", false}, // \x00 is valid UTF-8
		{"replacement char already present", "test\ufffdvalue", false},
		{"BOM", "\xef\xbb\xbfHello", false}, // UTF-8 BOM is valid
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, modified := SanitizeUTF8(tt.input)
			if modified != tt.modified {
				t.Errorf("Expected modified=%v, got modified=%v", tt.modified, modified)
			}
			if !tt.modified && result != tt.input {
				t.Errorf("Expected unchanged string, got %q instead of %q", result, tt.input)
			}
		})
	}
}

// Benchmarks to verify fast path performance
func BenchmarkSanitizeUTF8_ValidShort(b *testing.B) {
	input := "user=admin action=login"
	for i := 0; i < b.N; i++ {
		SanitizeUTF8(input)
	}
}

func BenchmarkSanitizeUTF8_ValidMedium(b *testing.B) {
	input := "Normal log message with some data: user=admin action=login status=success timestamp=2026-01-19T10:30:00Z"
	for i := 0; i < b.N; i++ {
		SanitizeUTF8(input)
	}
}

func BenchmarkSanitizeUTF8_ValidLong(b *testing.B) {
	input := strings.Repeat("This is a typical log message with various content. ", 20)
	for i := 0; i < b.N; i++ {
		SanitizeUTF8(input)
	}
}

func BenchmarkSanitizeUTF8_ValidUnicode(b *testing.B) {
	input := "日志消息 with mixed 内容 and émojis 🎉"
	for i := 0; i < b.N; i++ {
		SanitizeUTF8(input)
	}
}

func BenchmarkSanitizeUTF8_InvalidSingle(b *testing.B) {
	input := "Log with \x80 single invalid byte"
	for i := 0; i < b.N; i++ {
		SanitizeUTF8(input)
	}
}

func BenchmarkSanitizeUTF8_InvalidMultiple(b *testing.B) {
	input := "Log \x80 with \x81 multiple \x82 invalid \x83 bytes"
	for i := 0; i < b.N; i++ {
		SanitizeUTF8(input)
	}
}
