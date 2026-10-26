package storage

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestRewriteFamilies(t *testing.T) {
	h := "db/cpu/2026/10/07/20/"
	paths := []string{
		h + "cpu_1.parquet",
		h + "cpu_1_rewrite_100.parquet",
		h + "cpu_1_rewrite_200.parquet",
		h + "cpu_2.parquet",
		h + "cpu_2_rewrite_x.parquet",                  // not a rewrite name: its own family
		"db/cpu/2026/10/07/21/cpu_1_rewrite_5.parquet", // same stem, another hour
		h + "cpu_3_b1_compacted.parquet",
		h + "cpu_3_b1_compacted_rewrite_9.parquet",
	}
	got := RewriteFamilies(paths)
	if len(got) != 2 {
		t.Fatalf("families %v, want 2", got)
	}
	f1 := got[h+"cpu_1.parquet"]
	sort.Strings(f1)
	if len(f1) != 3 {
		t.Fatalf("family of cpu_1: %v", f1)
	}
	if len(got[h+"cpu_3_b1_compacted.parquet"]) != 2 {
		t.Fatalf("family of a compacted output: %v", got)
	}
	if RewriteFamily(h+"cpu_1_rewrite_100.parquet") != h+"cpu_1.parquet" {
		t.Fatal("RewriteFamily")
	}
}

type flakyDelete struct {
	*LocalBackend
	fails int
	calls int
	err   error
}

func (f *flakyDelete) Delete(ctx context.Context, path string) error {
	f.calls++
	if f.calls <= f.fails {
		return f.err
	}
	return f.LocalBackend.Delete(ctx, path)
}

func TestDeleteWithRetry(t *testing.T) {
	prev := DeleteRetryBackoff
	DeleteRetryBackoff = []time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() { DeleteRetryBackoff = prev })
	lb, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = lb.Write(ctx, "db/x.parquet", []byte("x"))

	b := &flakyDelete{LocalBackend: lb, fails: 2, err: errors.New("transient")}
	if err := DeleteWithRetry(ctx, b, "db/x.parquet"); err != nil || b.calls != 3 {
		t.Fatalf("two transient failures: %v after %d calls", err, b.calls)
	}
	b = &flakyDelete{LocalBackend: lb, fails: 9, err: errors.New("down")}
	if err := DeleteWithRetry(ctx, b, "db/y.parquet"); err == nil || b.calls != 3 {
		t.Fatalf("persistent failure: %v after %d calls", err, b.calls)
	}
	b = &flakyDelete{LocalBackend: lb, fails: 9, err: ErrObjectNotFound}
	if err := DeleteWithRetry(ctx, b, "db/z.parquet"); err != nil || b.calls != 1 {
		t.Fatalf("not found: %v after %d calls", err, b.calls)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	b = &flakyDelete{LocalBackend: lb, fails: 9, err: errors.New("down")}
	if err := DeleteWithRetry(cctx, b, "db/w.parquet"); err == nil || b.calls != 1 {
		t.Fatalf("cancelled: %v after %d calls", err, b.calls)
	}
}
