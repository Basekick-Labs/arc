package api

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

type readFailStore struct {
	storage.Backend
	fail error
}

func (s *readFailStore) Read(ctx context.Context, p string) ([]byte, error) {
	if s.fail != nil {
		return nil, s.fail
	}
	return s.Backend.Read(ctx, p)
}

// The strict variant (the Iceberg export, #939 A0): while the DELETE journal
// has not been read in full, the files it names are unknown, so the strict
// reader fails and reads it again on the next call; reads go on as before.
func TestRetiredReadsStrictNeedsTheWholeJournal(t *testing.T) {
	ctx := context.Background()
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	old, by := "db/m/2026/10/20/10/a.parquet", "db/m/2026/10/20/10/a_rewrite_1.parquet"
	rec := []byte(`{"v":1,"path":"` + old + `","replaced_by":"` + by + `"}`)
	if err := b.Write(ctx, storage.RetiredKey(old), rec); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(ctx, old, []byte("rows")); err != nil {
		t.Fatal(err)
	}
	store := &readFailStore{Backend: b, fail: errors.New("connection reset")}
	r := NewRetiredReads(store)
	if err := r.Seed(ctx); err == nil {
		t.Fatal("Seed with an unreadable record reported no error")
	}
	if got := r.ExcludedFor(ctx, "db/m/"); len(got) != 0 {
		t.Fatalf("lenient after a failed seed: excluded %v, want none", got)
	}
	if _, err := r.ExcludedForStrict(ctx, "db/m/"); err == nil {
		t.Fatal("strict with the journal unread: no error")
	}
	store.fail = nil
	got, err := r.ExcludedForStrict(ctx, "db/m/")
	if err != nil || len(got) != 1 || got[0] != old {
		t.Fatalf("strict once the journal reads: excluded %v (%v), want %s", got, err, old)
	}
}
