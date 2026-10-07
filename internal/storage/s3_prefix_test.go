package storage

import "testing"

func TestS3BackendPrefixedKey(t *testing.T) {
	backend := &S3Backend{prefix: "instances/abc123/"}

	tests := []struct {
		input  string
		expect string
	}{
		{"mydb/cpu/2025/01/file.parquet", "instances/abc123/mydb/cpu/2025/01/file.parquet"},
	}

	for _, tt := range tests {
		got, err := backend.prefixedKey(tt.input)
		if err != nil {
			t.Fatalf("prefixedKey(%q) = %v", tt.input, err)
		}
		if got != tt.expect {
			t.Errorf("prefixedKey(%q) = %q, want %q", tt.input, got, tt.expect)
		}
	}

	// "" is not a key. It named the prefix directory itself, which is an
	// object nothing can address (#743), and it reaches the SDK as an empty
	// Key. It remains valid as a LIST prefix.
	if _, err := backend.prefixedKey(""); err == nil {
		t.Error(`prefixedKey("") was accepted; an empty key names no object`)
	}
	if got, err := backend.prefixedListPrefix(""); err != nil || got != "instances/abc123/" {
		t.Errorf(`prefixedListPrefix("") = %q, %v; want the configured prefix`, got, err)
	}

	// No prefix configured
	noPrefix := &S3Backend{prefix: ""}
	got, err := noPrefix.prefixedKey("mydb/cpu/file.parquet")
	if err != nil {
		t.Fatalf("prefixedKey: %v", err)
	}
	if got != "mydb/cpu/file.parquet" {
		t.Errorf("prefixedKey with no prefix = %q, want %q", got, "mydb/cpu/file.parquet")
	}
}
