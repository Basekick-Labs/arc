package storage

import "testing"

func TestSanitizeS3Prefix(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{"empty", "", ""},
		{"whitespace only", "   ", ""},
		{"simple path", "instances/abc123", "instances/abc123/"},
		{"with trailing slash", "instances/abc123/", "instances/abc123/"},
		{"with leading slash", "/instances/abc123", "instances/abc123/"},
		{"with both slashes", "/instances/abc123/", "instances/abc123/"},
		{"single segment", "prefix", "prefix/"},
		{"path traversal rejected", "instances/../etc", ""},
		{"double dot in name rejected", "instances/..hidden", ""},
		{"nested path", "org/tenant/data", "org/tenant/data/"},
		{"with hyphens and underscores", "my-org/tenant_1", "my-org/tenant_1/"},
		{"with dots", "org.name/v1.0", "org.name/v1.0/"},
		{"sql injection rejected", "tenant'; DROP TABLE --", ""},
		{"single quotes rejected", "tenant's/data", ""},
		{"semicolon rejected", "tenant;data", ""},
		{"spaces rejected", "tenant name/data", ""},
		{"special chars rejected", "tenant@#$/data", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeS3Prefix(tt.input)
			if got != tt.expect {
				t.Errorf("SanitizeS3Prefix(%q) = %q, want %q", tt.input, got, tt.expect)
			}
		})
	}
}

func TestS3BackendPrefixedKey(t *testing.T) {
	backend := &S3Backend{prefix: "instances/abc123/"}

	tests := []struct {
		input  string
		expect string
	}{
		{"mydb/cpu/2025/01/file.parquet", "instances/abc123/mydb/cpu/2025/01/file.parquet"},
		{"", "instances/abc123/"},
	}

	for _, tt := range tests {
		got := backend.prefixedKey(tt.input)
		if got != tt.expect {
			t.Errorf("prefixedKey(%q) = %q, want %q", tt.input, got, tt.expect)
		}
	}

	// No prefix configured
	noPrefix := &S3Backend{prefix: ""}
	got := noPrefix.prefixedKey("mydb/cpu/file.parquet")
	if got != "mydb/cpu/file.parquet" {
		t.Errorf("prefixedKey with no prefix = %q, want %q", got, "mydb/cpu/file.parquet")
	}
}

func TestS3BackendGetS3PathWithPrefix(t *testing.T) {
	backend := &S3Backend{bucket: "my-bucket", prefix: "tenant1/"}
	got := backend.GetS3Path("mydb/cpu/file.parquet")
	expect := "s3://my-bucket/tenant1/mydb/cpu/file.parquet"
	if got != expect {
		t.Errorf("GetS3Path = %q, want %q", got, expect)
	}

	// Without prefix
	noPrefix := &S3Backend{bucket: "my-bucket", prefix: ""}
	got = noPrefix.GetS3Path("mydb/cpu/file.parquet")
	expect = "s3://my-bucket/mydb/cpu/file.parquet"
	if got != expect {
		t.Errorf("GetS3Path (no prefix) = %q, want %q", got, expect)
	}
}

func TestS3BackendGetQueryPathWithPrefix(t *testing.T) {
	backend := &S3Backend{bucket: "my-bucket", prefix: "tenant1/"}

	// Specific hour
	got := backend.GetQueryPath("mydb", "cpu", 2025, 11, 25, 16)
	expect := "s3://my-bucket/tenant1/mydb/cpu/2025/11/25/16/*.parquet"
	if got != expect {
		t.Errorf("GetQueryPath (hour) = %q, want %q", got, expect)
	}

	// Specific day
	got = backend.GetQueryPath("mydb", "cpu", 2025, 11, 25, 0)
	expect = "s3://my-bucket/tenant1/mydb/cpu/2025/11/25/*/*.parquet"
	if got != expect {
		t.Errorf("GetQueryPath (day) = %q, want %q", got, expect)
	}

	// Without prefix — backwards compatible
	noPrefix := &S3Backend{bucket: "my-bucket", prefix: ""}
	got = noPrefix.GetQueryPath("mydb", "cpu", 2025, 11, 25, 16)
	expect = "s3://my-bucket/mydb/cpu/2025/11/25/16/*.parquet"
	if got != expect {
		t.Errorf("GetQueryPath (no prefix) = %q, want %q", got, expect)
	}
}
