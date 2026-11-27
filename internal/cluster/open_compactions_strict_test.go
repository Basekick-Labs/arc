package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
)

// The strict variant (the Iceberg export, #939 A0) answers where a read
// guesses: a failed stat or the stale-local breaker is an error, not a file
// counted next to its replacement.

type statFailStore struct {
	storage.Backend
	err error
}

func (s statFailStore) StatFile(context.Context, string) (int64, error) { return 0, s.err }

var errThrottled = errors.New("throttled")

// An object job whose output cannot be stat'ed: reads count the inputs, the
// strict reader fails.
func TestOpenStrictFailsOnAnUnstatableObjectOutput(t *testing.T) {
	r := objectRig(t)
	ctx := context.Background()
	job := r.objectJob("j1")
	r.copyToFinal(job.StagedKey, job.Output.Path)
	inputs := []string{job.Inputs[0].Path, job.Inputs[1].Path}
	if got, err := r.c.OpenExcludedForStrict(ctx, openPrefix); err != nil || !sameSet(got, inputs) {
		t.Fatalf("output at its key: strict excluded %v (%v), want %v", got, err, inputs)
	}

	r2 := objectRig(t)
	job2 := r2.objectJob("j2")
	r2.copyToFinal(job2.StagedKey, job2.Output.Path)
	r2.c.storage = statFailStore{Backend: r2.c.storage, err: errThrottled}
	if got := r2.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("lenient read with a failing stat excluded %v, want none (counts the inputs)", got)
	}
	if _, err := r2.c.OpenExcludedForStrict(ctx, openPrefix); !errors.Is(err, errThrottled) {
		t.Fatalf("strict read with a failing stat: err = %v, want the stat error", err)
	}
}

// The stale-local breaker: reads count every deleted copy, the strict reader
// fails until the breaker clears.
func TestOpenStrictFailsWhileTheBreakerHolds(t *testing.T) {
	r := staleRig(t)
	ctx := context.Background()
	key := staleHour + "b.parquet"
	r.write(key, "rows")
	r.c.noteLocalDeleted(key, "retention:1")
	r.c.stale.exportBreaker.Store(true)
	r.c.stale.exportBreakerAt.Store(time.Now().UnixNano())
	if _, err := r.c.OpenExcludedForStrict(ctx, openPrefix); !errors.Is(err, errDeletedCopiesImplausible) {
		t.Fatalf("breaker tripped: strict err = %v, want errDeletedCopiesImplausible", err)
	}
	r.c.stale.exportBreaker.Store(false)
	if got, err := r.c.OpenExcludedForStrict(ctx, openPrefix); err != nil || !sameSet(got, []string{key}) {
		t.Fatalf("breaker clear: strict excluded %v (%v)", got, err)
	}
}

// A DELETE rewrite whose new file cannot be stat'ed: reads count the old
// file, the strict reader fails.
func TestOpenStrictFailsOnAnUnstatableRewrite(t *testing.T) {
	r := staleRig(t)
	ctx := context.Background()
	old := staleHour + "cpu_1.parquet"
	rw := staleHour + "cpu_1_rewrite_5.parquet"
	r.write(old, "all the rows")
	r.write(rw, "the kept rows")
	r.c.noteLocalDeleted(old, "delete-rewrite")
	e := raft.FileEntry{Path: rw, SHA256: strings.Repeat("b", 64), SizeBytes: int64(len("the kept rows")), Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	healthy := r.c.storage
	r.c.storage = statFailStore{Backend: healthy, err: errThrottled}
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("lenient read with a failing stat excluded %v, want none", got)
	}
	if _, err := r.c.OpenExcludedForStrict(ctx, openPrefix); !errors.Is(err, errThrottled) {
		t.Fatalf("strict read with a failing stat: err = %v, want the stat error", err)
	}
	r.c.storage = healthy
	if got, err := r.c.OpenExcludedForStrict(ctx, openPrefix); err != nil || !sameSet(got, []string{old}) {
		t.Fatalf("stat healthy: strict excluded %v (%v), want the old file", got, err)
	}
}
