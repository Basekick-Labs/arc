package compaction

import (
	"net/http"
	"net/http/httptest"
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

// The same contract for Azure, which had the same omission until #1102: the
// prefix was emitted by neither side, so an Azure primary with
// storage.azure_prefix set would have compacted against the container root —
// reading nothing, or writing outputs where queries never look. A review pass
// cannot see this: the parent and the subprocess are different processes.
//
// It asserts GetPrefix() directly rather than only comparing ConfigJSON round
// trips. The S3 test above compares round trips, and that comparison passes
// when the field is dropped on BOTH sides, which is exactly the pre-fix state
// — a weakness this one does not copy.
//
// Offline on purpose: NewAzureBlobBackend needs a syntactically valid base64
// shared key and nothing more, and its container probe is a Warn rather than
// an error. The endpoint is an httptest server answering 403 so that probe
// costs one request: azcore does not retry a 403, whereas a refused
// connection walks the retry ladder and makes each construction take seconds,
// which matters because this test is meant to be run alone with -count=20.
//
// The key below is fake and is never a real credential. The subprocess reads
// its own from AZURE_STORAGE_KEY, which is why that is set too: without it the
// Azure case takes the managed-identity arm instead of the shared-key one.
func TestCreateStorageBackendFromConfig_PreservesAzurePrefix(t *testing.T) {
	logger := zerolog.Nop()

	const fakeKey = "dGhpcy1pcy1ub3QtYS1yZWFsLWtleQ==" // base64("this-is-not-a-real-key")
	t.Setenv("AZURE_STORAGE_KEY", fakeKey)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "AuthenticationFailed", http.StatusForbidden)
	}))
	defer srv.Close()

	parent, err := storage.NewAzureBlobBackend(&storage.AzureBlobConfig{
		ContainerName: "arc-data",
		Prefix:        "instances/abc123/",
		AccountName:   "arctestaccount",
		AccountKey:    fakeKey,
		Endpoint:      srv.URL,
	}, logger)
	if err != nil {
		// Fatal, not Skip. Construction here is offline and deterministic — a
		// fake base64 key and a local httptest endpoint — so a failure is a
		// real defect, and a Skip would turn the only test of the
		// cross-process hop into a silent no-op.
		t.Fatalf("NewAzureBlobBackend against the stub endpoint: %v", err)
	}
	defer parent.Close()

	if parent.GetPrefix() != "instances/abc123/" {
		t.Fatalf("parent prefix = %q; the test cannot prove anything about the hop", parent.GetPrefix())
	}

	rebuilt, err := createStorageBackendFromConfig(&SubprocessJobConfig{
		StorageType:   parent.Type(),
		StorageConfig: parent.ConfigJSON(),
	}, logger)
	if err != nil {
		t.Fatalf("createStorageBackendFromConfig: %v", err)
	}
	defer rebuilt.Close()

	azure, ok := rebuilt.(*storage.AzureBlobBackend)
	if !ok {
		t.Fatalf("rebuilt backend is %T, want *storage.AzureBlobBackend", rebuilt)
	}
	// The assertion that matters: the subprocess addresses the same key space
	// as the parent. Dropping the prefix on either side fails here.
	if got, want := azure.GetPrefix(), "instances/abc123/"; got != want {
		t.Errorf("rebuilt prefix = %q, want %q: the subprocess would compact against a different location", got, want)
	}
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
