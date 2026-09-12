package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// manifestReachableKeys is the closed set of spellings that raft
// ValidateManifestPath accepts and storage ValidateKey refuses. It is the exact
// input set the quarantine branches added for #747 must handle, because these
// are the only keys that can reach a cleanup or replication loop from the
// cluster manifest.
//
// It is a closed set rather than a sample: ValidateManifestPath rejects empty,
// absolute and ".." outright, and those three are therefore unreachable by
// construction. A test that uses one of them as its representative case proves
// nothing about these loops.
//
// Kept here, next to the contract it is derived from, so tightening either
// validator shows up as a failure in one place.
func manifestReachableKeys() []struct {
	name string
	key  string
} {
	return []struct {
		name string
		key  string
	}{
		{"trailing separator", "testdb/cpu/2026/04/11/14/"},
		{"dot segment", "testdb/./cpu/2026/04/11/14/f.parquet"},
		{"empty interior segment", "testdb//cpu/2026/04/11/14/f.parquet"},
		{"backslash", `testdb\cpu/2026/04/11/14/f.parquet`},
		{"oversize segment", "testdb/" + strings.Repeat("m", MaxKeySegmentLen+1) + "/f.parquet"},
		// Every segment here is inside MaxKeySegmentLen, so this case exercises
		// the whole-key length rule rather than re-testing the segment rule.
		{"oversize key", "testdb/" + strings.Repeat(strings.Repeat("a", 200)+"/", 6) + "f.parquet"},
	}
}

// TestBackendMethodsReportInvalidPath pins the property every #747 quarantine
// branch depends on: for a key ValidateKey refuses, EVERY key-taking method
// returns an error that matches ErrInvalidPath, through whatever wrapping the
// method applies.
//
// The table is over methods, not over backends, so a tenth method added without
// validation fails here rather than in whichever cleanup loop reaches it first.
// S3 and Azure enforce the same contract through prefixedKey and blobKey, and
// are covered against live servers in objectstore_contract_test.go.
func TestBackendMethodsReportInvalidPath(t *testing.T) {
	b, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	ctx := context.Background()

	methods := []struct {
		name string
		call func(key string) error
	}{
		{"Write", func(k string) error { return b.Write(ctx, k, []byte("x")) }},
		{"WriteReader", func(k string) error { return b.WriteReader(ctx, k, strings.NewReader("x"), 1) }},
		{"Read", func(k string) error { _, err := b.Read(ctx, k); return err }},
		{"ReadTo", func(k string) error { return b.ReadTo(ctx, k, io.Discard) }},
		{"ReadToAt", func(k string) error { return b.ReadToAt(ctx, k, io.Discard, 0) }},
		{"StatFile", func(k string) error { _, err := b.StatFile(ctx, k); return err }},
		{"Delete", func(k string) error { return b.Delete(ctx, k) }},
		{"Exists", func(k string) error { _, err := b.Exists(ctx, k); return err }},
		{"AppendReader", func(k string) error { return b.AppendReader(ctx, k, strings.NewReader("x"), 1) }},
		{"RemoveDirectory", func(k string) error { return b.RemoveDirectory(ctx, k) }},
	}

	for _, tc := range manifestReachableKeys() {
		for _, m := range methods {
			t.Run(m.name+"/"+tc.name, func(t *testing.T) {
				err := m.call(tc.key)
				if err == nil {
					t.Fatalf("%s(%q) succeeded; the key is not addressable", m.name, tc.key)
				}
				if !errors.Is(err, ErrInvalidPath) {
					t.Errorf("%s(%q) = %v, which does not match ErrInvalidPath, so a caller cannot tell it apart from a transient failure", m.name, tc.key, err)
				}
			})
		}
	}
}

// TestInvalidPathSurvivesDoubleWrapping pins that errors.Is still matches after
// a caller re-wraps, which is what compaction's downloadSingleFile does. Any
// quarantine check that string-matched the message instead would pass the test
// above and fail here.
func TestInvalidPathSurvivesDoubleWrapping(t *testing.T) {
	b, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	inner := b.ReadTo(context.Background(), `db\m/f.parquet`, io.Discard)
	wrapped := errWrap(errWrap(inner))
	if !errors.Is(wrapped, ErrInvalidPath) {
		t.Fatalf("errors.Is failed after two wraps: %v", wrapped)
	}
	if strings.Count(wrapped.Error(), "invalid path") == 0 {
		t.Fatalf("wrapped message lost the cause: %v", wrapped)
	}
}

func errWrap(err error) error {
	return fmt.Errorf("layer: %w", err)
}

// TestDeleteBatchAggregatesRatherThanShortCircuits is a warning pinned as a
// test. Both LocalBackend and S3Backend join per-key failures into one error,
// so a BATCH error matches ErrInvalidPath when as little as one key in a
// thousand is bad. Quarantine logic must therefore classify per file; a caller
// that wrote errors.Is(batchErr, ErrInvalidPath) and dropped the batch would
// silently discard every valid delete in it.
//
// The assertion is deliberately in both directions: the match happens, AND the
// valid key was still deleted, which is what makes acting on the batch error
// wrong rather than merely imprecise.
func TestDeleteBatchAggregatesRatherThanShortCircuits(t *testing.T) {
	root := t.TempDir()
	b, err := NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	ctx := context.Background()
	const good = "testdb/cpu/2026/04/11/14/good.parquet"
	if err := b.Write(ctx, good, []byte("payload")); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	batchErr := b.DeleteBatch(ctx, []string{good, `testdb\cpu/bad.parquet`})
	if batchErr == nil {
		t.Fatal("DeleteBatch accepted an unusable key without reporting it")
	}
	if !errors.Is(batchErr, ErrInvalidPath) {
		t.Fatalf("batch error = %v, expected it to match ErrInvalidPath", batchErr)
	}
	if _, statErr := os.Stat(filepath.Join(root, good)); !os.IsNotExist(statErr) {
		t.Fatal("the valid key was NOT deleted, so treating the batch error as permanent would lose it")
	}
}

// TestWriteReaderRejectsBeforeConsumingBody pins why the puller quarantines at
// StatFile rather than at the write: a rejected key does not consume the body,
// so a puller that reached the write would already have paid for the transfer.
func TestWriteReaderRejectsBeforeConsumingBody(t *testing.T) {
	b, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	body := bytes.NewReader(make([]byte, 4096))
	if err := b.WriteReader(context.Background(), `db\m/f.parquet`, body, 4096); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("WriteReader = %v, want ErrInvalidPath", err)
	}
	if body.Len() != 4096 {
		t.Fatalf("body was consumed (%d bytes left of 4096); the rejection must precede the transfer", body.Len())
	}
}
