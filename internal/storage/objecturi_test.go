package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// readPathBackends returns one of each backend shape the URI builders switch
// on, including an S3 backend WITH a configured prefix. The prefixed case is
// the one that matters: every read-path bug this file guards against was a
// builder that forgot the prefix.
func readPathBackends(t *testing.T) map[string]Backend {
	t.Helper()
	local, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("local backend: %v", err)
	}
	return map[string]Backend{
		"local":           local,
		"s3":              &S3Backend{bucket: "b"},
		"s3-prefixed":     &S3Backend{bucket: "b", prefix: "tenant/"},
		"s3-deep-prefix":  &S3Backend{bucket: "b", prefix: "a/b/"},
		"azure":           &AzureBlobBackend{containerName: "c"},
		"unknown-backend": unknownBackend{},
	}
}

// unknownBackend exercises backendRoot's default branch: a Backend that is none
// of the three concrete types must still resolve to a single consistent root,
// not to a different one per call site.
type unknownBackend struct{ Backend }

// TestGetStoragePathNeverCollides.
//
// Injectivity is mostly guaranteed by construction here, because a segment
// containing "/" is rejected before the mapping runs, so this cannot fail
// unless segment validation is removed outright. That is what it is for: the
// separator check is the only thing standing between ("a", "b/c") and
// ("a/b", "c") naming one glob, and #743's lesson was that the interesting
// storage bugs are mapping bugs rather than verdict bugs.
//
// The root-and-prefix half of the mapping, which is where #746's live bugs
// actually were, is covered by TestObjectURIAndGetStoragePathShareOneRoot and
// by the MinIO tests. This one cannot see those.
func TestGetStoragePathNeverCollides(t *testing.T) {
	names := []string{
		"db", "cpu", "a", "b", "c", "a/b", "b/c", "a.b", "a..b", ".hidden",
		"db/cpu", "x", "x/y/z", "", ".", "..", "a\\b", "a*", "a?", "a[0]",
		strings.Repeat("n", 255), strings.Repeat("n", 256),
	}

	for backendName, backend := range readPathBackends(t) {
		t.Run(backendName, func(t *testing.T) {
			type pair struct{ db, m string }
			seen := make(map[string]pair)
			accepted := 0

			for _, db := range names {
				for _, m := range names {
					got, err := GetStoragePath(backend, db, m)
					if err != nil {
						continue
					}
					accepted++
					if prev, dup := seen[got]; dup {
						t.Errorf("two inputs produce one glob %q: (%q,%q) and (%q,%q)",
							got, prev.db, prev.m, db, m)
					}
					seen[got] = pair{db, m}
				}
			}

			// A property test whose generator mostly rejects asserts almost
			// nothing (#741's fifth review lesson), so pin a floor. Kept well
			// below the current count so trimming one name from the corpus
			// does not fail the test for an unrelated reason.
			if accepted < 64 {
				t.Fatalf("only %d of %d input pairs were accepted; the corpus no longer exercises the mapping",
					accepted, len(names)*len(names))
			}
		})
	}
}

// TestGetStoragePathRejectsSeparatorsInSegments is the case a key-level check
// cannot catch. ValidateKey(database + "/" + measurement) accepts a measurement
// of "a/b", because the joined string is a perfectly good key. It just names a
// different directory than the caller asked for.
func TestGetStoragePathRejectsSeparatorsInSegments(t *testing.T) {
	b := &S3Backend{bucket: "b"}
	for _, tc := range []struct{ name, db, m string }{
		{"separator in measurement", "db", "a/b"},
		{"separator in database", "a/b", "cpu"},
		{"backslash in measurement", "db", "a\\b"},
		{"empty measurement", "db", ""},
		{"empty database", "", "cpu"},
		{"dot segment", "db", "."},
		{"parent segment", "db", ".."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := GetStoragePath(b, tc.db, tc.m)
			if err == nil {
				t.Fatalf("expected a rejection, got %q", got)
			}
			if !errors.Is(err, ErrInvalidPath) {
				t.Errorf("error %v does not wrap ErrInvalidPath", err)
			}
		})
	}
}

// TestGetStoragePathRejectsGlobMetacharacters pins the rule the key contract
// deliberately does NOT have.
//
// ValidateKey accepts "*" because a write of that key names exactly one object.
// A read cannot: the same string is a DuckDB glob operator, so "cpu*" reads
// every measurement starting with "cpu". Key injectivity is necessary and not
// sufficient for the read path, which is the distinction #746 is about.
func TestGetStoragePathRejectsGlobMetacharacters(t *testing.T) {
	b := &S3Backend{bucket: "b"}
	for _, name := range []string{"a*", "a?", "a[0-9]", "a{1,2}", "*", "[", "]", "{", "}"} {
		t.Run(name, func(t *testing.T) {
			// A write of this key is fine: it names one object.
			if err := ValidateKey("db/" + name + "/f.parquet"); err != nil {
				t.Fatalf("the key contract should still accept %q for writes: %v", name, err)
			}
			// A read of it is not.
			if _, err := GetStoragePath(b, "db", name); err == nil {
				t.Errorf("measurement %q must not reach a read_parquet glob", name)
			}
			if _, err := GetStoragePath(b, name, "cpu"); err == nil {
				t.Errorf("database %q must not reach a read_parquet glob", name)
			}
		})
	}
}

// TestObjectURIAndGetStoragePathShareOneRoot.
//
// Both builders hang their result off the same backend root, and partition
// pruning relies on exactly that: it parses the root out of a glob built by
// GetStoragePath and trims it off URLs of the shape ObjectURI produces. If the
// two ever disagreed about where a backend's keys start, pruning would list the
// wrong place, which is the bug this change fixes.
func TestObjectURIAndGetStoragePathShareOneRoot(t *testing.T) {
	const (
		db  = "db"
		msr = "cpu"
		key = db + "/" + msr + "/2026/09/12/13/f.parquet"
	)
	for backendName, backend := range readPathBackends(t) {
		t.Run(backendName, func(t *testing.T) {
			glob, err := GetStoragePath(backend, db, msr)
			if err != nil {
				t.Fatalf("GetStoragePath: %v", err)
			}
			uri, err := ObjectURI(backend, key)
			if err != nil {
				t.Fatalf("ObjectURI: %v", err)
			}

			// The root as partition pruning derives it: strip the part of the
			// glob that GetStoragePath appends.
			root := strings.TrimSuffix(glob, db+"/"+msr+"/**/*.parquet")
			if !strings.HasPrefix(uri, root) {
				t.Fatalf("ObjectURI %q does not hang off the glob's root %q", uri, root)
			}
			if got := strings.TrimPrefix(uri, root); got != key {
				t.Errorf("trimming the root off %q gives %q, want the original key %q", uri, got, key)
			}
		})
	}
}

// TestObjectURICarriesTheConfiguredPrefix is the regression for the retention
// bug: the URI must name the object the backend actually wrote.
func TestObjectURICarriesTheConfiguredPrefix(t *testing.T) {
	const key = "db/cpu/2026/09/12/13/f.parquet"
	for _, tc := range []struct {
		name    string
		backend Backend
		want    string
	}{
		{"s3 without prefix", &S3Backend{bucket: "b"}, "s3://b/" + key},
		{"s3 with prefix", &S3Backend{bucket: "b", prefix: "tenant/"}, "s3://b/tenant/" + key},
		{"s3 with nested prefix", &S3Backend{bucket: "b", prefix: "a/b/"}, "s3://b/a/b/" + key},
		{"azure has no prefix concept", &AzureBlobBackend{containerName: "c"}, "azure://c/" + key},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ObjectURI(tc.backend, key)
			if err != nil {
				t.Fatalf("ObjectURI: %v", err)
			}
			if got != tc.want {
				t.Errorf("ObjectURI() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestObjectURINamesTheFileTheBackendWrites is the read/write agreement
// property for local storage, where it can be checked against the filesystem
// rather than against a string this package also computes.
func TestObjectURINamesTheFileTheBackendWrites(t *testing.T) {
	b, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("local backend: %v", err)
	}
	ctx := t.Context()

	for _, key := range []string{
		"db/cpu/2026/09/12/13/f.parquet",
		"db/a..b/f.parquet",
		"single",
	} {
		t.Run(key, func(t *testing.T) {
			want := []byte(key)
			if err := b.Write(ctx, key, want); err != nil {
				t.Fatalf("write: %v", err)
			}
			uri, err := ObjectURI(b, key)
			if err != nil {
				t.Fatalf("ObjectURI: %v", err)
			}
			if !filepath.IsAbs(uri) {
				t.Errorf("local ObjectURI must be absolute, got %q", uri)
			}
			got, err := os.ReadFile(uri)
			if err != nil {
				t.Fatalf("ObjectURI %q does not name the written file: %v", uri, err)
			}
			if string(got) != string(want) {
				t.Errorf("ObjectURI names the wrong file: read %q, wrote %q", got, want)
			}
		})
	}
}

// TestObjectURIRejectsWhatTheBackendWouldRefuse: the second chokepoint must not
// be looser than the first, or it is not a chokepoint.
func TestObjectURIRejectsWhatTheBackendWouldRefuse(t *testing.T) {
	b, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("local backend: %v", err)
	}
	ctx := t.Context()

	for _, key := range []string{
		"", "/db/x", "db/x/", "db//x", "db/./x", "db/../x", ".", "..",
		"db\\x", "db/x\x00", strings.Repeat("a", 256), strings.Repeat("ab/", 400) + "f",
	} {
		writeErr := b.Write(ctx, key, []byte("x"))
		_, uriErr := ObjectURI(b, key)
		if writeErr != nil && uriErr == nil {
			t.Errorf("key %q: Backend.Write refuses it but ObjectURI accepts it", key)
		}
	}
}
