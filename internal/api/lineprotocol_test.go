package api

import "testing"

func TestIsValidMeasurementName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		// Valid names
		{"simple lowercase", "cpu", true},
		{"simple uppercase", "CPU", true},
		{"mixed case", "CpuMetrics", true},
		{"with underscore", "cpu_usage", true},
		{"with hyphen", "cpu-usage", true},
		{"with numbers", "cpu123", true},
		{"complex valid", "System-Metrics_v2", true},

		// Invalid: starts with non-letter
		{"starts with number", "123cpu", false},
		{"starts with underscore", "_cpu", false},
		{"starts with hyphen", "-cpu", false},

		// Invalid: contains illegal characters
		{"contains space", "cpu usage", false},
		{"contains dot", "cpu.usage", false},
		{"contains slash", "cpu/usage", false},
		{"contains colon", "cpu:usage", false},
		{"contains at", "cpu@host", false},

		// Invalid: control characters (the main fix for issue #122)
		{"control char 0x01", "test\x01name", false},
		{"control char 0x05", "test\x05name", false},
		{"control char 0x08", "test\x08name", false},
		{"control char 0x0B", "test\x0Bname", false},
		{"control char 0x0C", "test\x0Cname", false},
		{"control char 0x1F", "test\x1Fname", false},
		{"starts with control char", "\x05cpu", false},
		{"ends with control char", "cpu\x05", false},

		// Edge cases
		{"empty string", "", false},
		{"single letter", "a", true},
		{"max length (128)", "a" + string(make([]byte, 127)), false}, // 128 'a's but make() creates zeros
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidMeasurementName(tt.input)
			if result != tt.expected {
				t.Errorf("isValidMeasurementName(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsValidMeasurementName_MaxLength(t *testing.T) {
	// Test exactly at the 128 char boundary
	validMaxLen := make([]byte, 128)
	validMaxLen[0] = 'a'
	for i := 1; i < 128; i++ {
		validMaxLen[i] = 'b'
	}

	if !isValidMeasurementName(string(validMaxLen)) {
		t.Error("expected 128 char name to be valid")
	}

	// Test 129 chars (should be invalid)
	tooLong := make([]byte, 129)
	tooLong[0] = 'a'
	for i := 1; i < 129; i++ {
		tooLong[i] = 'b'
	}

	if isValidMeasurementName(string(tooLong)) {
		t.Error("expected 129 char name to be invalid")
	}
}
