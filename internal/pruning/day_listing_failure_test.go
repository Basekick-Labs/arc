package pruning

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

// scriptedListBackend answers List from a script: each call takes the next
// entry, and the last entry repeats.
type scriptedListBackend struct {
	*mockS3Backend
	mu     sync.Mutex
	script []listAnswer
	calls  int
}

type listAnswer struct {
	files []string
	err   error
}

func (b *scriptedListBackend) List(_ context.Context, _ string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	i := b.calls
	if i >= len(b.script) {
		i = len(b.script) - 1
	}
	b.calls++
	return b.script[i].files, b.script[i].err
}

const (
	dayListGlob = "other://bucket/db/cpu/**/*.parquet"
	dayListDay  = "other://bucket/db/cpu/2025/01/15/*.parquet"
	dayListSQL  = "SELECT * FROM cpu WHERE time >= '2025-01-15 10:00:00' AND time < '2025-01-15 11:00:00'"
)

// These run on a scheme Arc does not build: there a day whose listing fails
// is still dropped (on s3://, azure:// and gs:// it is read through one
// recursive glob, day_widening_test.go), which is the state these tests need
// to show an unverified result is not kept.
var (
	errListing   = errors.New("injected listing failure")
	dayListDaily = []string{"db/cpu/2025/01/15/cpu_daily.parquet"}
)

// dayListPruner: a day whose listing follows the script. hours names the hour
// directories that exist under it.
func dayListPruner(hours []string, script ...listAnswer) *PartitionPruner {
	backend := &scriptedListBackend{
		mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{
			"db/cpu/2025/01/":    {"15"},
			"db/cpu/2025/01/15/": hours,
		}},
		script: script,
	}
	p := NewPartitionPruner(zerolog.Nop())
	p.SetStorageBackend(backend)
	return p
}

func resultHasPath(r interface{}, want string) bool {
	switch v := r.(type) {
	case string:
		return v == want
	case []string:
		for _, s := range v {
			if s == want {
				return true
			}
		}
	}
	return false
}

// A result that rests on a failed listing is not remembered: once storage
// answers again, the very next query sees the day's daily files. Before, the
// answer without them was served from cache for up to a minute.
func TestUnverifiedPruningResultIsNotCached(t *testing.T) {
	p := dayListPruner([]string{"10"}, listAnswer{err: errListing}, listAnswer{files: dayListDaily})

	first, _ := p.OptimizeTablePath(context.Background(), dayListGlob, dayListSQL)
	if resultHasPath(first, dayListDay) {
		t.Fatalf("precondition: the first result %v should have dropped the day path (its listing failed)", first)
	}
	second, _ := p.OptimizeTablePath(context.Background(), dayListGlob, dayListSQL)
	if !resultHasPath(second, dayListDay) {
		t.Errorf("second result = %v: the day path is still missing, so the unverified first result was served from cache", second)
	}
}

// The same when nothing at all survives the failed listing: the fallback to
// the unpruned glob is not remembered either.
func TestUnverifiedEmptyPruningResultIsNotCached(t *testing.T) {
	p := dayListPruner(nil, listAnswer{err: errListing}, listAnswer{files: dayListDaily})

	first, optimized := p.OptimizeTablePath(context.Background(), dayListGlob, dayListSQL)
	if optimized {
		t.Fatalf("precondition: the first result %v should be the unpruned fallback", first)
	}
	second, optimized := p.OptimizeTablePath(context.Background(), dayListGlob, dayListSQL)
	if !optimized || !resultHasPath(second, dayListDay) {
		t.Errorf("second result = (%v, %v): want the day path; the unverified fallback was served from cache", second, optimized)
	}
}

// The SQL transform cache keys off the volatility flag: an unverified result
// must raise it, a verified one must not.
func TestUnverifiedPruningResultIsVolatile(t *testing.T) {
	for _, c := range []struct {
		name   string
		answer listAnswer
		want   bool
	}{
		{"listing failed", listAnswer{err: errListing}, true},
		{"listing answered", listAnswer{files: dayListDaily}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := dayListPruner([]string{"10"}, c.answer)
			ctx, vol := WithVolatileResult(context.Background())
			p.OptimizeTablePath(ctx, dayListGlob, dayListSQL)
			if vol.Volatile != c.want {
				t.Errorf("Volatile = %v, want %v", vol.Volatile, c.want)
			}
		})
	}
}
