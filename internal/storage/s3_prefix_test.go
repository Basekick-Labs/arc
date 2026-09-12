package storage

import "testing"

func TestValidateS3Prefix(t *testing.T) {
	accepted := []struct{ in, want string }{
		{"", ""},
		{"instances/abc123", "instances/abc123/"},
		{"instances/abc123/", "instances/abc123/"},
		{"  tenant1  ", "tenant1/"},
		{"a..b", "a..b/"}, // a legitimate name the old ".." substring check destroyed
	}
	for _, tt := range accepted {
		got, err := ValidateS3Prefix(tt.in)
		if err != nil {
			t.Errorf("ValidateS3Prefix(%q) = %v, want %q", tt.in, err, tt.want)
			continue
		}
		if got != tt.want {
			t.Errorf("ValidateS3Prefix(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}

	// Every one of these came out of the OLD function's success path and then
	// broke every write, or silently relocated the deployment to the bucket
	// root. They must fail at construction instead.
	rejected := []string{"/", "//", "a//b", ".", "a/./b", "a/../b", "///a///b///", "a b", `a\b`, "a\x00b"}
	for _, in := range rejected {
		if got, err := ValidateS3Prefix(in); err == nil {
			t.Errorf("ValidateS3Prefix(%q) = %q, want an error", in, got)
		}
	}
}

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
