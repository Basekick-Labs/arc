package storage

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// azuriteAccountKey is the well-known Azurite development key. It is public,
// has never guarded anything, and is used here only because
// azblob.NewSharedKeyCredential requires base64 that decodes.
const azuriteAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

// fakeObjectStore answers every request with 200 and nothing else.
//
// Both object-store constructors probe their bucket or container before
// returning (s3.go HeadBucket, azure.go GetProperties) and only warn when the
// probe fails. Pointing them at a local server keeps these tests offline AND
// fast: a bogus endpoint would instead burn the SDK retry ladder inside a
// 10-second context on every single construction.
func fakeObjectStore(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// isolateAWSEnv takes the host's AWS configuration out of play for one test.
//
// Static credentials in the spec are not enough on their own. NewS3Backend
// calls config.LoadDefaultConfig, which independently reads AWS_PROFILE /
// AWS_DEFAULT_PROFILE and the shared config, credentials and CA-bundle files
// before any credential provider is consulted, and a machine with
// AWS_PROFILE pointing at a profile these tests know nothing about fails the
// whole construction: "failed to load AWS config: failed to get shared config
// profile". Blanking the profile and pointing the file paths at names that do
// not exist puts the loader on its empty-configuration path everywhere.
//
// Skipping on a loader error would be worse than useless here: the tests would
// quietly vanish on exactly the machines that have AWS configured.
//
// This uses t.Setenv, so no test that reaches it may call t.Parallel.
func isolateAWSEnv(t *testing.T) {
	t.Helper()
	absent := t.TempDir()
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_DEFAULT_PROFILE", "")
	t.Setenv("AWS_CA_BUNDLE", "")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(absent, "no-such-aws-config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(absent, "no-such-aws-credentials"))
}

// s3SpecFor builds an S3 spec that reaches the fake store and does not consult
// the host environment: isolateAWSEnv removes what config.LoadDefaultConfig
// would read, and the static credentials below mean s3.go's own env fallback
// (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY) cannot change the result either.
func s3SpecFor(t *testing.T, backendType string) BackendSpec {
	t.Helper()
	isolateAWSEnv(t)
	return BackendSpec{
		Type: backendType,
		S3: S3Config{
			Bucket:    "arc-factory-test",
			Region:    "us-east-1",
			Endpoint:  fakeObjectStore(t),
			AccessKey: "factorytest",
			SecretKey: "factorytest",
			PathStyle: true,
		},
	}
}

func azureSpecFor(t *testing.T, backendType string) BackendSpec {
	return BackendSpec{
		Type: backendType,
		Azure: AzureBlobConfig{
			AccountName:   "devstoreaccount1",
			AccountKey:    azuriteAccountKey,
			ContainerName: "arc-factory-test",
			Endpoint:      fakeObjectStore(t),
		},
	}
}

// TestNewBackendResolvesEveryAlias pins the alias set the factory inherited
// from cmd/arc/main.go: five spellings, three backends. minio and azblob are
// the ones at risk — they are pure aliases, so dropping either breaks nothing
// that compiles and everything that is configured that way.
//
// Each case asserts the CONCRETE type behind the interface, not just a non-nil
// result: "s3" resolving to a LocalBackend would also be a non-nil Backend.
func TestNewBackendResolvesEveryAlias(t *testing.T) {
	tests := []struct {
		name     string
		spec     func(t *testing.T) BackendSpec
		wantType string
		assert   func(t *testing.T, b Backend)
	}{
		{
			name: "local",
			spec: func(t *testing.T) BackendSpec {
				return BackendSpec{Type: "local", LocalPath: filepath.Join(t.TempDir(), "root")}
			},
			wantType: "local",
			assert: func(t *testing.T, b Backend) {
				if _, ok := b.(*LocalBackend); !ok {
					t.Errorf("local resolved to %T, want *LocalBackend", b)
				}
			},
		},
		{
			name:     "s3",
			spec:     func(t *testing.T) BackendSpec { return s3SpecFor(t, "s3") },
			wantType: "s3",
			assert: func(t *testing.T, b Backend) {
				if _, ok := b.(*S3Backend); !ok {
					t.Errorf("s3 resolved to %T, want *S3Backend", b)
				}
			},
		},
		{
			name:     "minio alias",
			spec:     func(t *testing.T) BackendSpec { return s3SpecFor(t, "minio") },
			wantType: "s3",
			assert: func(t *testing.T, b Backend) {
				if _, ok := b.(*S3Backend); !ok {
					t.Errorf("minio resolved to %T, want *S3Backend", b)
				}
			},
		},
		{
			name:     "azure",
			spec:     func(t *testing.T) BackendSpec { return azureSpecFor(t, "azure") },
			wantType: "azure",
			assert: func(t *testing.T, b Backend) {
				if _, ok := b.(*AzureBlobBackend); !ok {
					t.Errorf("azure resolved to %T, want *AzureBlobBackend", b)
				}
			},
		},
		{
			name:     "azblob alias",
			spec:     func(t *testing.T) BackendSpec { return azureSpecFor(t, "azblob") },
			wantType: "azure",
			assert: func(t *testing.T, b Backend) {
				if _, ok := b.(*AzureBlobBackend); !ok {
					t.Errorf("azblob resolved to %T, want *AzureBlobBackend", b)
				}
			},
		},
		{
			// config.Load normalises storage.backend, but a hand-built spec
			// does not go through it, so the factory trims and lowercases too.
			name:     "uppercase and padded",
			spec:     func(t *testing.T) BackendSpec { return s3SpecFor(t, "  S3  ") },
			wantType: "s3",
			assert: func(t *testing.T, b Backend) {
				if _, ok := b.(*S3Backend); !ok {
					t.Errorf("padded S3 resolved to %T, want *S3Backend", b)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := NewBackend(tt.spec(t), zerolog.Nop())
			if err != nil {
				t.Fatalf("NewBackend(%s) returned an error: %v", tt.name, err)
			}
			if b == nil {
				t.Fatalf("NewBackend(%s) returned a nil backend with no error", tt.name)
			}
			defer b.Close()
			if got := b.Type(); got != tt.wantType {
				t.Errorf("NewBackend(%s).Type() = %q, want %q", tt.name, got, tt.wantType)
			}
			tt.assert(t, b)
		})
	}
}

// TestNewBackendUnknownTypeNamesEveryAlias keeps the error actionable. An
// operator who typed "gcs" needs to be told what IS accepted; the primary call
// site logs its own five-alias Fatal before it ever asks the factory, so this
// message is what every OTHER caller shows.
func TestNewBackendUnknownTypeNamesEveryAlias(t *testing.T) {
	for _, badType := range []string{"gcs", "", "s3 bucket"} {
		b, err := NewBackend(BackendSpec{Type: badType}, zerolog.Nop())
		if err == nil {
			t.Fatalf("NewBackend(type=%q) succeeded, want an error", badType)
		}
		if b != nil {
			t.Fatalf("NewBackend(type=%q) returned a non-nil backend alongside an error", badType)
		}
		msg := err.Error()
		// The message echoes the rejected value, and that echo can itself
		// contain an alias ("s3 bucket" contains "s3"), which would satisfy the
		// check below for the wrong reason. Look for the aliases in what is
		// left after the quoted echo is removed.
		aliasText := msg
		if i, j := strings.Index(aliasText, `"`), strings.LastIndex(aliasText, `"`); i >= 0 && j > i {
			aliasText = aliasText[:i] + aliasText[j+1:]
		}
		for _, alias := range []string{"local", "s3", "minio", "azure", "azblob"} {
			if !strings.Contains(aliasText, alias) {
				t.Errorf("NewBackend(type=%q) error does not name the %q alias: %s", badType, alias, msg)
			}
		}
		if badType != "" && !strings.Contains(msg, badType) {
			t.Errorf("NewBackend(type=%q) error does not echo the rejected value: %s", badType, msg)
		}
	}
}

// TestNewBackendNeverReturnsTypedNilOnError is the #713 shape.
//
// Callers keep the result in a storage.Backend variable and then branch on
// "!= nil" — cmd/arc/main.go's cold tier is the live example, where that test
// is read by the startup tier scan, the drainer existence probe and the query
// router. An interface holding a (*S3Backend)(nil) passes that test and panics
// on first use, so the factory must return the literal nil on every error path.
//
// Every case here is offline: no case reaches a request, because the
// required-field checks and the prefix validation all run before the
// connection probe each constructor ends with.
func TestNewBackendNeverReturnsTypedNilOnError(t *testing.T) {
	// The s3 specs below reach config.LoadDefaultConfig, which reads the host's
	// AWS configuration; without this, an unrelated loader failure would
	// satisfy these cases for the wrong reason.
	isolateAWSEnv(t)

	// A regular file where the local backend wants to create a directory.
	notADir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notADir, []byte("x"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	// Prefix validation happens at s3.go:186, before the HeadBucket probe that
	// closes NewS3Backend, so this spec never issues a request and its endpoint
	// is inert. It is spelled out in full anyway so the case differs from a
	// working spec in exactly one field: the prefix.
	badPrefix := S3Config{
		Bucket:    "arc-factory-test",
		Region:    "us-east-1",
		Endpoint:  fakeObjectStore(t),
		AccessKey: "factorytest",
		SecretKey: "factorytest",
		PathStyle: true,
		Prefix:    "a//b",
	}

	tests := []struct {
		name string
		spec BackendSpec
	}{
		{"unknown type", BackendSpec{Type: "gcs"}},
		{"local path under a regular file", BackendSpec{Type: "local", LocalPath: filepath.Join(notADir, "root")}},
		{"s3 with no bucket", BackendSpec{Type: "s3", S3: S3Config{Region: "us-east-1"}}},
		{"minio with no bucket", BackendSpec{Type: "minio", S3: S3Config{Region: "us-east-1"}}},
		{"s3 with an unusable prefix", BackendSpec{Type: "s3", S3: badPrefix}},
		{"azure with no container", BackendSpec{Type: "azure", Azure: AzureBlobConfig{AccountName: "acct"}}},
		{"azblob with no container", BackendSpec{Type: "azblob", Azure: AzureBlobConfig{AccountName: "acct"}}},
		{"azure with no authentication method", BackendSpec{Type: "azure", Azure: AzureBlobConfig{ContainerName: "c"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := NewBackend(tt.spec, zerolog.Nop())
			if err == nil {
				if b != nil {
					b.Close()
				}
				t.Fatalf("NewBackend(%s) succeeded, want an error", tt.name)
			}
			// This is the assertion that matters: == nil on the INTERFACE.
			if b != nil {
				t.Fatalf("NewBackend(%s) returned a typed nil inside a non-nil interface (%T); every caller that branches on != nil would use it and panic", tt.name, b)
			}
		})
	}
}

// TestNewBackendForwardsAndValidatesS3Prefix covers the one spec field whose
// loss is silent. A dropped prefix does not fail: it relocates the whole
// deployment to the bucket root, which is a different and much larger place
// (see ValidateS3Prefix's own doc comment; the compaction subprocess once had
// the same omission, which is fixed and pinned by
// internal/compaction/subprocess_config_test.go). So the factory must pass
// Prefix through, and must let ValidateS3Prefix refuse an unusable one rather
// than fall back to "".
func TestNewBackendForwardsAndValidatesS3Prefix(t *testing.T) {
	t.Run("forwarded and normalised", func(t *testing.T) {
		spec := s3SpecFor(t, "s3")
		spec.S3.Prefix = "instances/abc123"

		b, err := NewBackend(spec, zerolog.Nop())
		if err != nil {
			t.Fatalf("NewBackend: %v", err)
		}
		defer b.Close()

		s3b, ok := b.(*S3Backend)
		if !ok {
			t.Fatalf("resolved to %T, want *S3Backend", b)
		}
		if got, want := s3b.prefix, "instances/abc123/"; got != want {
			t.Errorf("backend prefix = %q, want %q; an empty prefix means the factory dropped it and every key now lands at the bucket root", got, want)
		}
	})

	// Asserting only "it failed" would be no assertion at all: every other way
	// an S3 construction can fail - a missing bucket, an AWS config the loader
	// cannot read - satisfies that too, so the subtest would still pass with
	// ValidateS3Prefix removed from the path entirely. Each case therefore
	// matches the wording ValidateS3Prefix itself produces for that input.
	t.Run("unusable prefix refuses construction", func(t *testing.T) {
		cases := []struct{ prefix, wantMsg string }{
			{"/", "is not a relative prefix"},
			{"a//b", "contains an empty segment"},
			{".", `contains a "." segment`},
			{"a b", "contains an unsupported character"},
		}
		base := s3SpecFor(t, "s3")
		for _, tc := range cases {
			spec := base
			spec.S3.Prefix = tc.prefix

			b, err := NewBackend(spec, zerolog.Nop())
			if err == nil {
				if b != nil {
					b.Close()
				}
				t.Errorf("NewBackend with prefix %q succeeded; an unusable prefix must stop construction, not silently become the bucket root", tc.prefix)
				continue
			}
			if b != nil {
				t.Errorf("NewBackend with prefix %q returned a non-nil backend alongside an error", tc.prefix)
			}
			msg := err.Error()
			if !strings.Contains(msg, "storage prefix") || !strings.Contains(msg, tc.prefix) {
				t.Errorf("NewBackend with prefix %q failed for some other reason than the prefix: %s", tc.prefix, msg)
				continue
			}
			if !strings.Contains(msg, tc.wantMsg) {
				t.Errorf("NewBackend with prefix %q error does not say %q: %s", tc.prefix, tc.wantMsg, msg)
			}
		}
	})
}
