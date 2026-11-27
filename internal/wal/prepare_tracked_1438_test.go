package wal

// #1438: a write refused after its WAL append was replayed at the next start.
// A queued uncovered write now prepares its entries and commits them under the
// shard lock (PrepareTracked); entries enqueued for a write that then fails
// are retracted by checkpoint (retractEnqueued).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// The prepared entry is the entry AppendRawWithMetaTracked writes: recovery,
// checkpoints and replication read both the same way.
func TestPrepareTrackedWritesTheAppendedBytes(t *testing.T) {
	newW := func() *Writer {
		return &Writer{entryChan: make(chan walEntry, 4), pendingSeqs: map[uint64]struct{}{}, trackedInstance: 7}
	}
	payload := []byte("\x81\xa1m\xa3cpu")

	appended := newW()
	tokens, err := appended.AppendRawWithMetaTracked("db1", payload)
	if err != nil {
		t.Fatal(err)
	}
	want := <-appended.entryChan

	prepared := newW()
	p, err := prepared.PrepareTracked("db1", payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.entryChan) != 0 {
		t.Fatal("PrepareTracked enqueued an entry: only Commit may")
	}
	got, err := p.Commit(0)
	if err != nil {
		t.Fatal(err)
	}
	have := <-prepared.entryChan

	if fmt.Sprint(got) != fmt.Sprint(tokens) {
		t.Fatalf("tokens = %v, want %v", got, tokens)
	}
	// Bytes 4-12 are the entry's timestamp; everything else, CRC included, matches.
	if !bytes.Equal(have.data[:4], want.data[:4]) || !bytes.Equal(have.data[12:], want.data[12:]) || have.seq != want.seq {
		t.Fatalf("prepared entry differs from the appended one:\n%x\n%x", have.data, want.data)
	}
	if have.covKey != "" {
		t.Fatalf("uncovered entry carries coverage key %q", have.covKey)
	}
}

// A durable entry is acknowledged by its sync, which cannot be waited for under
// the shard lock: preparing one is refused, so the caller appends instead.
func TestPrepareTrackedRefusesDurable(t *testing.T) {
	w := &Writer{config: WriterConfig{AckMode: AckModeDurable}, pendingSeqs: map[uint64]struct{}{}}
	if _, err := w.PrepareTracked("db1", []byte("x")); err == nil {
		t.Fatal("PrepareTracked accepted a durable writer")
	}
	if len(w.pendingSeqs) != 0 {
		t.Fatal("a refused prepare left a sequence pending")
	}
}

// The uncovered twin of TestCoveredCommitDropKeepsEnqueuedTokens: on a full
// queue the entries already enqueued keep their tokens (the flush checkpoints
// them), the rest are released.
func TestUncoveredCommitDropKeepsEnqueuedTokens(t *testing.T) {
	w := &Writer{entryChan: make(chan walEntry, 1), pendingSeqs: map[uint64]struct{}{}, trackedInstance: 5}
	p := &PreparedAppend{w: w}
	for i := 0; i < 3; i++ {
		seq := uint64(i + 1)
		w.pendingSeqs[seq] = struct{}{}
		p.entries = append(p.entries, preparedEntry{data: make([]byte, WALEntryHeaderSize+8), seq: seq, token: fmt.Sprintf("%016x%016x", 5, seq)})
	}
	tokens, err := p.Commit(0)
	if !errors.Is(err, ErrWALDropped) {
		t.Fatalf("Commit err = %v, want ErrWALDropped", err)
	}
	if len(tokens) != 1 || tokens[0] != p.entries[0].token {
		t.Fatalf("tokens = %v, want the first chunk's only", tokens)
	}
	if _, ok := w.pendingSeqs[1]; !ok || len(w.pendingSeqs) != 1 {
		t.Fatalf("pending = %v, want only seq 1", w.pendingSeqs)
	}
}

func newModeWriter(t *testing.T, mode AckMode) (*Writer, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := NewWriter(&WriterConfig{WALDir: dir, SyncMode: SyncModeFsync, AckMode: mode, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	return w, dir
}

func replayedEntries(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	_, err := NewRecovery(dir, zerolog.Nop()).Recover(context.Background(), func(context.Context, []map[string]interface{}) error {
		n++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// Entries retracted for a failed write are checkpointed: recovery replays
// nothing, in both acknowledgement modes.
func TestRetractEnqueuedIsNotReplayed(t *testing.T) {
	payload := []byte("\x91\x81\xa1v\x01")
	for _, mode := range []AckMode{AckModeQueued, AckModeDurable} {
		t.Run(string(mode), func(t *testing.T) {
			w, dir := newModeWriter(t, mode)
			tokens, err := w.AppendRawWithMetaTracked("db1", payload)
			if err != nil {
				t.Fatal(err)
			}
			kept, err := w.AppendRawWithMetaTracked("db1", payload)
			if err != nil || len(kept) == 0 {
				t.Fatalf("second append: %v", err)
			}
			w.retractEnqueued(tokens)
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if n := replayedEntries(t, dir); n != 1 {
				t.Fatalf("replayed %d entries, want only the one not retracted", n)
			}
			if w.RetractionsFailed != 0 {
				t.Fatalf("RetractionsFailed = %d", w.RetractionsFailed)
			}
		})
	}
}

// An abandoned prepare writes nothing; a committed one is replayed.
func TestPrepareTrackedAbandonWritesNothing(t *testing.T) {
	w, dir := newModeWriter(t, AckModeQueued)
	payload := []byte("\x91\x81\xa1v\x01")
	abandoned, err := w.PrepareTracked("db1", payload)
	if err != nil {
		t.Fatal(err)
	}
	abandoned.Abandon()
	committed, err := w.PrepareTracked("db1", payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := committed.Commit(0); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if n := replayedEntries(t, dir); n != 1 {
		t.Fatalf("replayed %d entries, want the committed one only", n)
	}
	_ = os.RemoveAll(dir)
}

// A retraction that cannot be written (here: the writer is closed) releases
// the entries, is counted every time, and is logged at most once per
// retractLogInterval: a failing WAL disk fails every retraction.
func TestRetractTrackedFailureIsCountedAndSampled(t *testing.T) {
	var logs bytes.Buffer
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := NewWriter(&WriterConfig{WALDir: dir, SyncMode: SyncModeFsync, AckMode: AckModeDurable, Logger: zerolog.New(&logs)})
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := w.AppendRawWithMetaTracked("db1", []byte("\x91\x81\xa1v\x01"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if w.RetractTracked(tokens) {
			t.Fatal("a retraction on a closed writer reported success")
		}
	}
	if w.RetractionsFailed != 2 {
		t.Fatalf("RetractionsFailed = %d, want 2", w.RetractionsFailed)
	}
	if n := bytes.Count(logs.Bytes(), []byte("could not be retracted")); n != 1 {
		t.Fatalf("logged %d failed-retraction lines, want 1 (sampled)", n)
	}
	if len(w.pendingSeqs) != 0 {
		t.Fatalf("failed retraction left %d sequences pending", len(w.pendingSeqs))
	}
}
