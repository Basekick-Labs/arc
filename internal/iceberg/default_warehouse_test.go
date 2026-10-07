package iceberg

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// DefaultWarehouse must put the warehouse root where the DATA is, not where
// the bucket or container begins.
//
// The Azure arm returned "azure://<container>" regardless of the prefix until
// #1102, so an Azure primary with a prefix would have written its data under
// the prefix and its Iceberg metadata at the container root — leaving the
// exported table's version-hint.text unreachable from the warehouse, which is
// #534's exact failure in the same file family.
//
// The no-trailing-separator rule is part of the contract rather than cosmetic:
// exporter.warehouseRelKey trims this value off a location and then trims the
// separator itself, so a trailing one here leaves every metadata key short a
// character.
//
// Both object-store constructors probe their bucket or container at
// construction and only warn on failure, so they run offline. They are pointed
// at an httptest server answering 403 rather than at a dead port: a 403 is not
// retried, while a refused connection walks each SDK's retry ladder and costs
// seconds per backend, which matters when this is run with -count=20.
func TestDefaultWarehouse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "AuthenticationFailed", http.StatusForbidden)
	}))
	defer srv.Close()

	// Fake, syntactically valid base64. Never a real credential.
	const fakeKey = "dGhpcy1pcy1ub3QtYS1yZWFsLWtleQ=="

	azure := func(container, prefix string) storage.Backend {
		t.Helper()
		b, err := storage.NewAzureBlobBackend(&storage.AzureBlobConfig{
			ContainerName: container,
			Prefix:        prefix,
			AccountName:   "arctestaccount",
			AccountKey:    fakeKey,
			Endpoint:      srv.URL,
		}, zerolog.Nop())
		if err != nil {
			// Fatal, not Skip: construction is offline and deterministic
			// against the stub endpoint above, so a failure is a defect and a
			// Skip would quietly drop the whole table.
			t.Fatalf("NewAzureBlobBackend against the stub endpoint: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b
	}
	s3 := func(bucket, prefix string) storage.Backend {
		t.Helper()
		b, err := storage.NewS3Backend(&storage.S3Config{
			Bucket: bucket, Prefix: prefix, Region: "us-east-1",
			Endpoint: srv.URL, PathStyle: true,
			AccessKey: "a", SecretKey: "b",
		}, zerolog.Nop())
		if err != nil {
			// Fatal, not Skip, for the same reason as the Azure helper above.
			t.Fatalf("NewS3Backend against the stub endpoint: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b
	}

	for _, tc := range []struct {
		name    string
		backend storage.Backend
		want    string
	}{
		{"azure without prefix", azure("cont", ""), "azure://cont"},
		{"azure with prefix", azure("cont", "arc"), "azure://cont/arc"},
		{"azure with nested prefix", azure("cont", "a/b/"), "azure://cont/a/b"},
		// The S3 and local arms are unchanged by #1102; pinned here so a
		// future edit to this one function cannot move them unnoticed.
		{"s3 without prefix", s3("bkt", ""), "s3://bkt"},
		{"s3 with prefix", s3("bkt", "hot"), "s3://bkt/hot"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := DefaultWarehouse(tc.backend)
			if got != tc.want {
				t.Errorf("DefaultWarehouse() = %q, want %q", got, tc.want)
			}
			if strings.HasSuffix(got, "/") {
				t.Errorf("DefaultWarehouse() = %q; a warehouse root must not carry a trailing separator (warehouseRelKey trims it)", got)
			}
		})
	}

	t.Run("local arm unchanged", func(t *testing.T) {
		dir := t.TempDir()
		b, err := storage.NewLocalBackend(dir, zerolog.Nop())
		if err != nil {
			t.Fatalf("NewLocalBackend: %v", err)
		}
		if got := DefaultWarehouse(b); !strings.HasPrefix(got, "file://") {
			t.Errorf("DefaultWarehouse(local) = %q, want a file:// URI", got)
		}
	})
}
