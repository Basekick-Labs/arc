package storage

import (
	"strings"
	"testing"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
)

// The validator is shared by every object-store backend (S3, MinIO, Azure
// Blob), which is why it no longer carries S3 in its name. Its behaviour is
// unchanged by that rename; these are the same cases it has always pinned.
func TestValidateObjectPrefix(t *testing.T) {
	accepted := []struct{ in, want string }{
		{"", ""},
		{"instances/abc123", "instances/abc123/"},
		{"instances/abc123/", "instances/abc123/"},
		{"  tenant1  ", "tenant1/"},
		{"a..b", "a..b/"}, // a legitimate name the old ".." substring check destroyed
	}
	for _, tt := range accepted {
		got, err := ValidateObjectPrefix(tt.in)
		if err != nil {
			t.Errorf("ValidateObjectPrefix(%q) = %v, want %q", tt.in, err, tt.want)
			continue
		}
		if got != tt.want {
			t.Errorf("ValidateObjectPrefix(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}

	// Every one of these came out of the OLD function's success path and then
	// broke every write, or silently relocated the deployment to the bucket
	// root. They must fail at construction instead.
	rejected := []string{"/", "//", "a//b", ".", "a/./b", "a/../b", "///a///b///", "a b", `a\b`, "a\x00b"}
	for _, in := range rejected {
		if got, err := ValidateObjectPrefix(in); err == nil {
			t.Errorf("ValidateObjectPrefix(%q) = %q, want an error", in, got)
		}
	}
}

func TestValidateObjectPrefixDiagnosticSurvivesLogSanitization(t *testing.T) {
	_, err := ValidateObjectPrefix("tenant/bad*prefix")
	if err == nil {
		t.Fatal("ValidateObjectPrefix accepted a prefix with an unsupported character")
	}

	got := sqlutil.SanitizeErrText(err.Error())
	for _, want := range []string{
		"storage prefix",
		"unsupported character",
		"ASCII letters",
		"digits",
		"slash",
		"hyphen",
		"underscore",
		"period",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("sanitized diagnostic %q does not contain %q", got, want)
		}
	}
	if strings.Contains(got, "tenant/bad*prefix") {
		t.Errorf("sanitized diagnostic exposed the rejected prefix: %q", got)
	}
}
