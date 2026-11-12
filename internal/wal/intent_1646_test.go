package wal

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// intentWAL writes columnar tracked entries of rows rows each, then the
// given intents, and closes the writer as a crash would leave it: no
// checkpoint for any entry. It returns the WAL directory and the tokens.
func intentWAL(t *testing.T, entries, rows int, intents func(tokens []string) []FlushIntent) (string, []string) {
	t.Helper()
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	var tokens []string
	for i := 0; i < entries; i++ {
		ids, err := w.AppendRawWithMetaTracked("db", coveredTestPayload(t, rows))
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, ids...)
	}
	for _, in := range intents(tokens) {
		if err := w.RecordFlushIntent(in); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, tokens
}

type adoptRun struct {
	adoptCalls   []FlushIntent
	checkpointed []string
	replayed     []string // identities handed to the replay callback
	adopt        bool
	cpErr        error
}

func (a *adoptRun) opts() *RecoveryOptions {
	return &RecoveryOptions{
		ColumnarCallback: func(_ context.Context, _, _ string, _ map[string][]interface{}, id string) error {
			a.replayed = append(a.replayed, id)
			return nil
		},
		BeforeDelete: func(context.Context) error { return nil },
		CheckpointRecovered: func(_ context.Context, ids []string) error {
			if a.cpErr != nil {
				return a.cpErr
			}
			a.checkpointed = append(a.checkpointed, ids...)
			return nil
		},
		AdoptIntent: func(_ context.Context, in FlushIntent) bool {
			a.adoptCalls = append(a.adoptCalls, in)
			return a.adopt
		},
	}
}

func (a *adoptRun) recover(t *testing.T, dir string) *RecoveryStats {
	t.Helper()
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, a.opts())
	if err != nil {
		t.Fatal(err)
	}
	return stats
}

func TestFlushIntentRoundTripAndInertForOldReaders(t *testing.T) {
	in := FlushIntent{Path: "db/m/2026/11/12/10/m_x.parquet", Size: 1234, IDs: []string{"0000000000000001000000000000002a", RecoveryRowIdentity("0000000000000001000000000000002b", 0, 7)}}
	s, err := EncodeFlushIntent(in)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := ParseFlushIntent(s)
	if !ok || got.Path != in.Path || got.Size != in.Size || len(got.IDs) != 2 || got.IDs[1] != in.IDs[1] {
		t.Fatalf("round trip %+v %v", got, ok)
	}
	// What an older binary sees: one checkpoint identity, which is neither a
	// tracked token nor a row range, so it matches nothing and covers nothing.
	if trackedRowIdentity(s) {
		t.Fatal("intent reads as a tracked token")
	}
	if _, _, _, isRange := ParseRecoveryRowIdentity(s); isRange {
		t.Fatal("intent reads as a row range")
	}
	if len(recoveryRowCoverage(map[string]struct{}{s: {}})) != 0 {
		t.Fatal("intent contributes row coverage")
	}
	// And it survives the checkpoint scan as written.
	dir, _ := intentWAL(t, 1, 2, func([]string) []FlushIntent { return []FlushIntent{in} })
	files, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	var found bool
	for _, f := range files {
		hashes, err := NewReader(f, zerolog.Nop()).ReadCheckpointHashes()
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range hashes {
			if h == s {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("intent not read back by the checkpoint scan")
	}
}

func TestFlushIntentTooLargeIsRefused(t *testing.T) {
	ids := make([]string, MaxFlushIntentBytes/30)
	for i := range ids {
		ids[i] = strings.Repeat("a", 32)
	}
	if _, err := EncodeFlushIntent(FlushIntent{Path: "p", Size: 1, IDs: ids}); !errors.Is(err, ErrFlushIntentTooLarge) {
		t.Fatalf("err %v", err)
	}
}

// #1646: entries whose file a flush stored before the crash took its
// checkpoint are checkpointed, not replayed.
func TestRecoveryAdoptsAStoredFlush(t *testing.T) {
	dir, tokens := intentWAL(t, 3, 2, func(tok []string) []FlushIntent {
		return []FlushIntent{{Path: "db/cpu/f1.parquet", Size: 100, IDs: tok[:2]}}
	})
	a := &adoptRun{adopt: true}
	stats := a.recover(t, dir)
	if len(a.replayed) != 1 || a.replayed[0] != tokens[2] {
		t.Fatalf("replayed %v, want only %s", a.replayed, tokens[2])
	}
	if len(a.adoptCalls) != 1 || stats.AdoptedFiles != 1 {
		t.Fatalf("adopt calls %d stats %+v", len(a.adoptCalls), stats)
	}
	if !contains(a.checkpointed, tokens[0]) || !contains(a.checkpointed, tokens[1]) {
		t.Fatalf("adopted entries not checkpointed: %v", a.checkpointed)
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "*.wal")); len(left) != 0 {
		t.Fatalf("WAL files left: %v", left)
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func TestRecoveryReplaysWhenTheFileIsNotThere(t *testing.T) {
	dir, _ := intentWAL(t, 2, 2, func(tok []string) []FlushIntent {
		return []FlushIntent{{Path: "db/cpu/f1.parquet", Size: 100, IDs: tok}}
	})
	a := &adoptRun{adopt: false}
	stats := a.recover(t, dir)
	if len(a.replayed) != 2 || stats.AdoptedFiles != 0 || len(a.adoptCalls) != 1 {
		t.Fatalf("replayed %v calls %d stats %+v", a.replayed, len(a.adoptCalls), stats)
	}
}

// Without a durable checkpoint for the adopted entries nothing is skipped.
func TestRecoveryReplaysWhenTheAdoptionCheckpointFails(t *testing.T) {
	dir, _ := intentWAL(t, 2, 2, func(tok []string) []FlushIntent {
		return []FlushIntent{{Path: "db/cpu/f1.parquet", Size: 100, IDs: tok}}
	})
	a := &adoptRun{adopt: true, cpErr: errors.New("WAL full")}
	stats := a.recover(t, dir)
	if len(a.replayed) != 2 || stats.AdoptedFiles != 0 {
		t.Fatalf("replayed %v stats %+v", a.replayed, stats)
	}
}

// All or nothing: an intent one of whose entries is already checkpointed is
// not adopted, and AdoptIntent is not even asked.
func TestRecoveryDoesNotAdoptAPartlyCheckpointedIntent(t *testing.T) {
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	var tokens []string
	for i := 0; i < 2; i++ {
		ids, err := w.AppendRawWithMetaTracked("db", coveredTestPayload(t, 2))
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, ids...)
	}
	if err := w.RecordFlushIntent(FlushIntent{Path: "db/cpu/f1.parquet", Size: 100, IDs: tokens}); err != nil {
		t.Fatal(err)
	}
	if err := w.MarkFlushed(tokens[:1]); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	a := &adoptRun{adopt: true}
	stats := a.recover(t, dir)
	if len(a.adoptCalls) != 0 || len(a.replayed) != 1 || stats.AdoptedFiles != 0 {
		t.Fatalf("calls %d replayed %v stats %+v", len(a.adoptCalls), a.replayed, stats)
	}
}

// A per-hour file's intent names row ranges: those rows are skipped, the rest
// of the entry is replayed, and the entry is checkpointed whole after.
func TestRecoveryAdoptsARowRangeIntent(t *testing.T) {
	dir, tokens := intentWAL(t, 1, 4, func(tok []string) []FlushIntent {
		return []FlushIntent{{Path: "db/cpu/h10.parquet", Size: 100, IDs: []string{RecoveryRowIdentity(tok[0], 0, 2)}}}
	})
	a := &adoptRun{adopt: true}
	stats := a.recover(t, dir)
	want := RecoveryRowIdentity(tokens[0], 2, 4)
	if len(a.replayed) != 1 || a.replayed[0] != want || stats.AdoptedFiles != 1 {
		t.Fatalf("replayed %v, want [%s]; stats %+v", a.replayed, want, stats)
	}
	if !contains(a.checkpointed, tokens[0]) {
		t.Fatalf("parent not checkpointed after the rest replayed: %v", a.checkpointed)
	}
}

// Without CheckpointRecovered there is no durable proof to write: no adoption.
func TestRecoveryNeedsCheckpointRecoveredToAdopt(t *testing.T) {
	dir, _ := intentWAL(t, 1, 2, func(tok []string) []FlushIntent {
		return []FlushIntent{{Path: "db/cpu/f1.parquet", Size: 100, IDs: tok}}
	})
	a := &adoptRun{adopt: true}
	opts := a.opts()
	opts.CheckpointRecovered = nil
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, opts)
	if err != nil || len(a.adoptCalls) != 0 || len(a.replayed) != 1 || stats.AdoptedFiles != 0 {
		t.Fatalf("err %v calls %d replayed %v", err, len(a.adoptCalls), a.replayed)
	}
}
