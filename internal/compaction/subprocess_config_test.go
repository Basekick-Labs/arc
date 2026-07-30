package compaction

import (
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// The compaction subprocess rebuilds its storage backend from the parent's
// Type()+ConfigJSON(). Any field ConfigJSON emits but createStorageBackendFromConfig
// fails to parse is silently dropped, and the subprocess then operates against a
// different location than the parent — with no error.
//
// The round-trip below is the real guard: it drives the actual production parse
// function, so a field added to ConfigJSON without a matching field in the
// subprocess parse struct changes the rebuilt config and fails here.
//
// Prefix in particular defaults to empty, which is exactly why dropping it went
// unnoticed — every test and most deployments leave it unset.
func TestCreateStorageBackendFromConfig_PreservesS3Prefix(t *testing.T) {
	logger := zerolog.Nop()

	parent, err := storage.NewS3Backend(&storage.S3Config{
		Bucket:    "arc-data",
		Prefix:    "instances/abc123/",
		Region:    "us-west-2",
		Endpoint:  "http://localhost:9000",
		PathStyle: true,
	}, logger)
	if err != nil {
		t.Skipf("cannot construct S3 backend in this environment: %v", err)
	}

	cfg := &SubprocessJobConfig{
		StorageType:   parent.Type(),
		StorageConfig: parent.ConfigJSON(),
	}

	rebuilt, err := createStorageBackendFromConfig(cfg, logger)
	if err != nil {
		t.Fatalf("createStorageBackendFromConfig: %v", err)
	}
	defer rebuilt.Close()

	// The rebuilt backend must serialize back to the same configuration —
	// if the prefix were dropped, this comparison shows it.
	if got, want := rebuilt.ConfigJSON(), parent.ConfigJSON(); got != want {
		t.Errorf("rebuilt backend config differs from parent:\n  parent:  %s\n  rebuilt: %s", want, got)
	}
}

// Same round-trip contract for the local backend, which is the default and so
// the one most deployments actually exercise.
func TestCreateStorageBackendFromConfig_PreservesLocalConfig(t *testing.T) {
	logger := zerolog.Nop()

	parent, err := storage.NewLocalBackend(t.TempDir(), logger)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}

	rebuilt, err := createStorageBackendFromConfig(&SubprocessJobConfig{
		StorageType:   parent.Type(),
		StorageConfig: parent.ConfigJSON(),
	}, logger)
	if err != nil {
		t.Fatalf("createStorageBackendFromConfig: %v", err)
	}
	defer rebuilt.Close()

	if got, want := rebuilt.ConfigJSON(), parent.ConfigJSON(); got != want {
		t.Errorf("rebuilt backend config differs from parent:\n  parent:  %s\n  rebuilt: %s", want, got)
	}
}

// An unrecognized storage type must fail loudly rather than fall back to some
// default backend — a subprocess silently compacting against the wrong storage
// is worse than one that refuses to start.
func TestCreateStorageBackendFromConfig_RejectsUnknownType(t *testing.T) {
	_, err := createStorageBackendFromConfig(&SubprocessJobConfig{
		StorageType:   "resilient",
		StorageConfig: "{}",
	}, zerolog.Nop())
	if err == nil {
		t.Fatal("expected an error for an unknown storage type")
	}
}
