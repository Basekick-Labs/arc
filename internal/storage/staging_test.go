package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// Tests for #744. LocalBackend stages every write at key+PartSuffix, so the
// staging file of key "x" WAS the committed object "x.part". The two destroyed
// each other in four different orderings, all of which are closed by reserving
// the suffix and addressing the partial through StagingInspector.

func TestReservedStagingSuffixIsNotAKey(t *testing.T) {
	for _, k := range []string{"x.parquet.part", "db/cpu/a.part", ".part"} {
		err := ValidateKey(k)
		if err == nil {
			t.Errorf("ValidateKey(%q) was accepted; it names the staging file of another key", k)
			continue
		}
		if !errors.Is(err, ErrInvalidPath) {
			t.Errorf("ValidateKey(%q) = %v, want ErrInvalidPath", k, err)
		}
	}
	// The suffix is only reserved at the end.
	for _, k := range []string{"x.part.parquet", "db/.partial/a.parquet", "a.parts"} {
		if err := ValidateKey(k); err != nil {
			t.Errorf("ValidateKey(%q) = %v, want accepted", k, err)
		}
	}
}

// TestCommittedWriteSurvivesAConcurrentStagedWrite is the regression proper.
// Before the fix, Write("x.parquet.part") returned success and was then wiped
// by the next WriteReader("x.parquet") opening its staging file with O_TRUNC.
func TestCommittedWriteSurvivesAConcurrentStagedWrite(t *testing.T) {
	b, _ := NewLocalBackend(t.TempDir(), zerolog.Nop())
	ctx := context.Background()

	if err := b.Write(ctx, "x.parquet.part", []byte("PAYLOAD-TWO")); err == nil {
		t.Fatal(`Write("x.parquet.part") was accepted; it is the staging file of "x.parquet"`)
	}
	if err := b.Write(ctx, "x.parquet", []byte("PAYLOAD-ONE")); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := b.Read(ctx, "x.parquet")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "PAYLOAD-ONE" {
		t.Errorf("Read = %q, want PAYLOAD-ONE", got)
	}
}

// TestStagedPartialIsInvisibleToListButReclaimable pins both halves. A partial
// must not appear in List, because every List-then-Read caller would then get a
// key the backend refuses; and it must still be findable, or an abandoned one
// could never be reclaimed and would fill the disk.
func TestStagedPartialIsInvisibleToListButReclaimable(t *testing.T) {
	b, _ := NewLocalBackend(t.TempDir(), zerolog.Nop())
	ctx := context.Background()

	// A failed WriteReader leaves a staged partial behind by design.
	err := b.WriteReader(ctx, "db/cpu/x.parquet", io.MultiReader(
		bytes.NewReader([]byte("PART")),
		errReader{errors.New("transport failed")},
	), 64)
	if err == nil {
		t.Fatal("expected the staged write to fail")
	}

	keys, err := b.List(ctx, "db/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, k := range keys {
		if ValidateKey(k) != nil {
			t.Errorf("List returned %q, which this backend refuses", k)
		}
	}

	n, err := b.StagedSize(ctx, "db/cpu/x.parquet")
	if err != nil {
		t.Fatalf("StagedSize: %v", err)
	}
	if n != int64(len("PART")) {
		t.Fatalf("StagedSize = %d, want %d; an abandoned partial must stay findable", n, len("PART"))
	}

	staged, err := b.ListStaged(ctx, "db/")
	if err != nil {
		t.Fatalf("ListStaged: %v", err)
	}
	if len(staged) != 1 || staged[0].Path != "db/cpu/x.parquet" {
		t.Fatalf("ListStaged = %+v, want one entry keyed by the owning key", staged)
	}

	if err := b.DeleteStaged(ctx, "db/cpu/x.parquet"); err != nil {
		t.Fatalf("DeleteStaged: %v", err)
	}
	if n, _ := b.StagedSize(ctx, "db/cpu/x.parquet"); n != -1 {
		t.Errorf("StagedSize after delete = %d, want -1", n)
	}
	// Deleting one that is not there is not an error.
	if err := b.DeleteStaged(ctx, "db/cpu/x.parquet"); err != nil {
		t.Errorf("DeleteStaged on a missing partial = %v, want nil", err)
	}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// TestDeleteReclaimsTheKeysPartial pins that removing an object removes its
// half-written staging file too. Without this the partial is invisible to List
// and nothing else would ever find it.
func TestDeleteReclaimsTheKeysPartial(t *testing.T) {
	b, _ := NewLocalBackend(t.TempDir(), zerolog.Nop())
	ctx := context.Background()

	if err := b.Write(ctx, "db/cpu/x.parquet", []byte("committed")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.WriteReader(ctx, "db/cpu/x.parquet", io.MultiReader(
		bytes.NewReader([]byte("PART")), errReader{errors.New("boom")}), 64)
	if n, _ := b.StagedSize(ctx, "db/cpu/x.parquet"); n < 0 {
		t.Fatal("precondition: expected a staged partial")
	}

	if err := b.Delete(ctx, "db/cpu/x.parquet"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if n, _ := b.StagedSize(ctx, "db/cpu/x.parquet"); n >= 0 {
		t.Errorf("staged partial survived Delete (%d bytes); nothing else can see it", n)
	}
}

// TestStagedPartialOfANowReservedKeyIsReclaimable covers the pre-upgrade
// orphan. A partial can belong to a key that was legal before the suffix was
// reserved, and refusing to address it would leave it hidden from List and
// unreclaimable forever.
func TestStagedPartialOfANowReservedKeyIsReclaimable(t *testing.T) {
	dir := t.TempDir()
	b, _ := NewLocalBackend(dir, zerolog.Nop())
	ctx := context.Background()

	// Written behind the backend's back, as an older version would have left it.
	if err := os.MkdirAll(filepath.Join(dir, "db"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "db", "x.part"+PartSuffix), []byte("LEGACY"), 0o600); err != nil {
		t.Fatal(err)
	}

	staged, err := b.ListStaged(ctx, "db/")
	if err != nil {
		t.Fatalf("ListStaged: %v", err)
	}
	if len(staged) != 1 || staged[0].Path != "db/x.part" {
		t.Fatalf("ListStaged = %+v, want one entry for db/x.part", staged)
	}
	// The owning key is itself reserved now, so this is exactly the case that
	// must not be refused.
	if err := b.DeleteStaged(ctx, staged[0].Path); err != nil {
		t.Fatalf("DeleteStaged(%q) = %v; the orphan would be permanently unreclaimable", staged[0].Path, err)
	}
}

// TestKeyLengthLeavesRoomForTheStagingSuffix pins that a key the contract
// accepts can actually be written. At exactly the segment limit the staging
// file overflowed the filesystem's own limit and the write failed.
func TestKeyLengthLeavesRoomForTheStagingSuffix(t *testing.T) {
	b, _ := NewLocalBackend(t.TempDir(), zerolog.Nop())
	ctx := context.Background()

	key := "db/" + strings.Repeat("k", MaxKeySegmentLen-len(PartSuffix))
	if err := ValidateKey(key); err != nil {
		t.Fatalf("ValidateKey rejected a key at the documented limit: %v", err)
	}
	if err := b.WriteReader(ctx, key, bytes.NewReader([]byte("x")), 1); err != nil {
		t.Errorf("the contract accepted a key the write path cannot store: %v", err)
	}
}
