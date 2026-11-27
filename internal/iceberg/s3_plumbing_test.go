package iceberg

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// #939 A1: schema from a file's footer without a local copy, and the S3
// FileIO's catalog properties.

func localBackendWith(t *testing.T, key string, rows int) (storage.Backend, string, int64) {
	t.Helper()
	root := t.TempDir()
	b, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	full := filepath.Join(root, key)
	writeArcStyleParquet(t, full, 1_700_000_000_000_000, rows)
	st, err := os.Stat(full)
	if err != nil {
		t.Fatal(err)
	}
	return b, full, st.Size()
}

// The footer read through the backend gives the schema the local read gives,
// whether the first tail read holds the footer or a second one is needed.
func TestSchemaFromBackend_MatchesLocalRead(t *testing.T) {
	ctx := context.Background()
	key := "mydb/cpu/2026/07/14/15/a.parquet"
	b, full, size := localBackendWith(t, key, 50)
	want, err := SchemaFromParquet(full)
	if err != nil {
		t.Fatal(err)
	}
	for _, window := range []int64{64 << 10, 16} {
		old := footerWindow
		footerWindow = window
		got, err := SchemaFromBackend(ctx, b, key, size)
		footerWindow = old
		if err != nil {
			t.Fatalf("window %d: %v", window, err)
		}
		if len(got.Fields) != len(want.Fields) {
			t.Fatalf("window %d: %d fields, want %d", window, len(got.Fields), len(want.Fields))
		}
		for i := range want.Fields {
			if got.Fields[i].Name != want.Fields[i].Name || !got.Fields[i].Type.Equals(want.Fields[i].Type) {
				t.Fatalf("window %d: field %d = %v, want %v", window, i, got.Fields[i], want.Fields[i])
			}
		}
	}
}

// A size that does not match the object (replaced or truncated since it was
// listed), or bytes that are not Parquet, is an error, never a schema.
func TestSchemaFromBackend_RefusesWhatIsNotTheListedFile(t *testing.T) {
	ctx := context.Background()
	key := "mydb/cpu/2026/07/14/15/a.parquet"
	b, _, size := localBackendWith(t, key, 50)
	if _, err := SchemaFromBackend(ctx, b, key, size+10); err == nil {
		t.Fatal("a listed size larger than the object gave a schema")
	}
	if _, err := SchemaFromBackend(ctx, b, key, size-10); err == nil {
		t.Fatal("a listed size smaller than the object gave a schema")
	}
	if err := b.Write(ctx, "mydb/cpu/x.parquet", []byte(strings.Repeat("x", 100))); err != nil {
		t.Fatal(err)
	}
	if _, err := SchemaFromBackend(ctx, b, "mydb/cpu/x.parquet", 100); err == nil {
		t.Fatal("bytes with no Parquet magic gave a schema")
	}
	if _, err := SchemaFromBackend(ctx, b, "mydb/cpu/x.parquet", 4); err == nil {
		t.Fatal("a 4-byte file gave a schema")
	}
}

// The FileIO properties: the endpoint key is always present (an absent key
// would let AWS_S3_ENDPOINT redirect the FileIO), addressing follows Arc's
// path-style setting, and no credential is ever a property.
func TestS3CatalogProps(t *testing.T) {
	cases := []struct {
		name      string
		set       storage.S3ClientSettings
		endpoint  string
		forceVirt string
	}{
		{"aws", storage.S3ClientSettings{Region: "us-east-1"}, "", "true"},
		{"compatible path style", storage.S3ClientSettings{Region: "us-east-1", Endpoint: "http://localhost:9000", PathStyle: true}, "http://localhost:9000", "false"},
		{"compatible virtual hosted", storage.S3ClientSettings{Endpoint: "https://s3.example.com"}, "https://s3.example.com", "true"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := s3CatalogProps(c.set)
			ep, ok := p[iceio.S3EndpointURL]
			if !ok || ep != c.endpoint {
				t.Fatalf("s3.endpoint = %q (present %v), want %q present", ep, ok, c.endpoint)
			}
			if got := p[iceio.S3ForceVirtualAddressing]; got != c.forceVirt {
				t.Fatalf("force-virtual-addressing = %q, want %q", got, c.forceVirt)
			}
			if c.set.Region != "" && p[iceio.S3Region] != c.set.Region {
				t.Fatalf("s3.region = %q, want %q", p[iceio.S3Region], c.set.Region)
			}
			for k := range p {
				if k == iceio.S3AccessKeyID || k == iceio.S3SecretAccessKey || k == iceio.S3SessionToken {
					t.Fatalf("credential property %q set", k)
				}
			}
		})
	}
}

// A non-S3 backend gives the exporter no aws.Config and no S3 properties.
func TestAWSConfigOf_LocalBackend(t *testing.T) {
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if cfg, props := awsConfigOf(b); cfg != nil || props != nil {
		t.Fatalf("local backend: cfg=%v props=%v, want none", cfg, props)
	}
}
