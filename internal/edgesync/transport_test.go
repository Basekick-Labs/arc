package edgesync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// entryFor builds a ledger entry describing the given content.
func entryFor(path string, content []byte) *LedgerEntry {
	return &LedgerEntry{
		HubID:       DefaultHubID,
		Path:        path,
		SHA256:      sha256Hex(content),
		SizeBytes:   int64(len(content)),
		Database:    "default",
		Measurement: "cpu",
	}
}

func TestPutOutcome_RetryableAndDone(t *testing.T) {
	// The two classification helpers drive the agent's control flow, so their
	// disagreements matter more than their agreements. Conflict is the case
	// worth pinning: it is neither done nor retryable, because resending
	// cannot resolve a content disagreement and would risk overwriting good
	// data.
	tests := []struct {
		outcome       PutOutcome
		wantRetryable bool
		wantDone      bool
	}{
		{OutcomeCommitted, false, true},
		{OutcomeAlreadyPresent, false, true},
		{OutcomePartial, true, false},
		{OutcomeChecksumMismatch, true, false},
		{OutcomeBackpressure, true, false},
		{OutcomeConflict, false, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.outcome), func(t *testing.T) {
			if got := tt.outcome.Retryable(); got != tt.wantRetryable {
				t.Errorf("Retryable() = %v, want %v", got, tt.wantRetryable)
			}
			if got := tt.outcome.Done(); got != tt.wantDone {
				t.Errorf("Done() = %v, want %v", got, tt.wantDone)
			}
		})
	}
}

func TestPutResult_Validate(t *testing.T) {
	entry := entryFor("db/cpu/a.parquet", []byte("hello world"))

	tests := []struct {
		name    string
		result  *PutResult
		wantErr string // substring; empty means the result must validate
	}{
		{
			name:   "committed full size",
			result: &PutResult{Outcome: OutcomeCommitted, BytesAccepted: entry.SizeBytes},
		},
		{
			name:   "partial short of the file",
			result: &PutResult{Outcome: OutcomePartial, BytesAccepted: 4},
		},
		{
			name:   "conflict with the hub digest",
			result: &PutResult{Outcome: OutcomeConflict, TheirSHA256: sha256Hex([]byte("other"))},
		},
		{
			name:   "backpressure with a delay",
			result: &PutResult{Outcome: OutcomeBackpressure, RetryAfter: 2 * time.Second},
		},
		{
			name:    "unknown outcome",
			result:  &PutResult{Outcome: PutOutcome("teapot")},
			wantErr: "unknown put outcome",
		},
		{
			name:    "negative bytes",
			result:  &PutResult{Outcome: OutcomeCommitted, BytesAccepted: -1},
			wantErr: "negative bytes accepted",
		},
		{
			name:    "accepted more than the file holds",
			result:  &PutResult{Outcome: OutcomeCommitted, BytesAccepted: entry.SizeBytes + 1},
			wantErr: "more than the file's",
		},
		{
			name: "partial that accepted everything",
			// A contradiction: the caller would leave the entry pending
			// forever with nothing left to send.
			result:  &PutResult{Outcome: OutcomePartial, BytesAccepted: entry.SizeBytes},
			wantErr: "accepted the whole file",
		},
		{
			name:    "conflict without a digest",
			result:  &PutResult{Outcome: OutcomeConflict},
			wantErr: "without the hub's sha256",
		},
		{
			name: "conflict whose digest matches",
			// Not a conflict at all — the hub holds identical content, so
			// this should have been AlreadyPresent.
			result:  &PutResult{Outcome: OutcomeConflict, TheirSHA256: entry.SHA256},
			wantErr: "not a conflict",
		},
		{
			name: "backpressure without a delay",
			// Zero would busy-loop against an already-overloaded hub.
			result:  &PutResult{Outcome: OutcomeBackpressure},
			wantErr: "without a retry delay",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.result.Validate(entry)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want an error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Validate() = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}

	t.Run("nil result", func(t *testing.T) {
		var r *PutResult
		if err := r.Validate(entry); err == nil {
			t.Error("Validate() on nil = nil, want an error")
		}
	})
}

func TestReconcileResult_Validate(t *testing.T) {
	tests := []struct {
		name    string
		result  *ReconcileResult
		wantErr string
	}{
		{
			name: "disjoint lists",
			result: &ReconcileResult{
				Missing:   []string{"a.parquet", "b.parquet"},
				Present:   []string{"c.parquet"},
				Conflicts: []Conflict{{Path: "d.parquet", TheirSHA256: "deadbeef"}},
			},
		},
		{
			name:   "empty result",
			result: &ReconcileResult{},
		},
		{
			name: "same path missing and present",
			// The agent would try to both send and skip this file.
			result: &ReconcileResult{
				Missing: []string{"a.parquet"},
				Present: []string{"a.parquet"},
			},
			wantErr: "both missing and present",
		},
		{
			name: "same path present and conflicted",
			result: &ReconcileResult{
				Present:   []string{"a.parquet"},
				Conflicts: []Conflict{{Path: "a.parquet", TheirSHA256: "deadbeef"}},
			},
			wantErr: "both present and conflicts",
		},
		{
			name:    "empty path in missing",
			result:  &ReconcileResult{Missing: []string{""}},
			wantErr: "empty path in missing",
		},
		{
			name:    "conflict without a digest",
			result:  &ReconcileResult{Conflicts: []Conflict{{Path: "a.parquet"}}},
			wantErr: "without the hub's sha256",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.result.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want an error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Validate() = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestMemoryTransport_ReconcilePartitionsPendingSet(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()

	missing := entryFor("db/cpu/missing.parquet", []byte("not on the hub"))
	present := entryFor("db/cpu/present.parquet", []byte("already delivered"))
	conflict := entryFor("db/cpu/conflict.parquet", []byte("spoke version"))

	tr.Seed(DefaultHubID, present.Path, present.SHA256, present.SizeBytes)
	// Same path, different content — a spoke_id collision or corruption.
	tr.Seed(DefaultHubID, conflict.Path, sha256Hex([]byte("hub version")), 11)

	res, err := tr.Reconcile(ctx, DefaultHubID, []*LedgerEntry{missing, present, conflict})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := res.Validate(); err != nil {
		t.Fatalf("reconcile result failed validation: %v", err)
	}

	if len(res.Missing) != 1 || res.Missing[0] != missing.Path {
		t.Errorf("missing = %v, want [%s]", res.Missing, missing.Path)
	}
	if len(res.Present) != 1 || res.Present[0] != present.Path {
		t.Errorf("present = %v, want [%s]", res.Present, present.Path)
	}
	if len(res.Conflicts) != 1 || res.Conflicts[0].Path != conflict.Path {
		t.Fatalf("conflicts = %v, want one for %s", res.Conflicts, conflict.Path)
	}
	// The hub's digest is the evidence an operator needs to tell a collision
	// from corruption; a conflict without it is unactionable.
	if res.Conflicts[0].TheirSHA256 == conflict.SHA256 {
		t.Error("conflict reports the spoke's own digest; it must report the hub's")
	}
}

func TestMemoryTransport_PutCommitsAndVerifies(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()

	content := []byte("parquet bytes")
	e := entryFor("db/cpu/a.parquet", content)

	res, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content), 0)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := res.Validate(e); err != nil {
		t.Fatalf("result failed validation: %v", err)
	}
	if res.Outcome != OutcomeCommitted {
		t.Errorf("outcome = %q, want %q", res.Outcome, OutcomeCommitted)
	}
	if res.BytesAccepted != e.SizeBytes {
		t.Errorf("bytes accepted = %d, want %d", res.BytesAccepted, e.SizeBytes)
	}

	if got, ok := tr.Has(DefaultHubID, e.Path); !ok || got != e.SHA256 {
		t.Errorf("hub holds (%q, %v), want (%q, true)", got, ok, e.SHA256)
	}
}

func TestMemoryTransport_RedeliveryIsIdempotent(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()

	content := []byte("parquet bytes")
	e := entryFor("db/cpu/a.parquet", content)

	if _, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content), 0); err != nil {
		t.Fatalf("first put: %v", err)
	}

	// The lost-ack case: the spoke never saw the acknowledgment and resends.
	// This must be a no-op, not a duplicate — it is what turns at-least-once
	// delivery into exactly-once effect.
	res, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content), 0)
	if err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if res.Outcome != OutcomeAlreadyPresent {
		t.Errorf("outcome = %q, want %q", res.Outcome, OutcomeAlreadyPresent)
	}
	if !res.Outcome.Done() {
		t.Error("already-present must count as done, or the agent would resend forever")
	}
}

func TestMemoryTransport_ConflictRefusesOverwrite(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()

	hubContent := []byte("the hub's version")
	spokeContent := []byte("the spoke's version")
	e := entryFor("db/cpu/contested.parquet", spokeContent)
	tr.Seed(DefaultHubID, e.Path, sha256Hex(hubContent), int64(len(hubContent)))

	res, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(spokeContent), 0)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if res.Outcome != OutcomeConflict {
		t.Fatalf("outcome = %q, want %q", res.Outcome, OutcomeConflict)
	}
	if res.Outcome.Retryable() {
		t.Error("conflict must not be retryable — resending cannot resolve it")
	}

	// The hub's bytes must be untouched: overwriting would destroy whichever
	// copy is the correct one.
	if got, _ := tr.Has(DefaultHubID, e.Path); got != sha256Hex(hubContent) {
		t.Error("conflict overwrote the hub's content")
	}
}

func TestMemoryTransport_ChecksumMismatchDiscards(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()

	// The entry claims one digest; the body carries different bytes — bit-rot
	// in flight, or in the edge's own storage.
	e := entryFor("db/cpu/corrupt.parquet", []byte("original content"))
	corrupted := []byte("corrupted conten") // same length, different bytes

	res, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(corrupted), 0)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if res.Outcome != OutcomeChecksumMismatch {
		t.Fatalf("outcome = %q, want %q", res.Outcome, OutcomeChecksumMismatch)
	}
	if !res.Outcome.Retryable() {
		t.Error("checksum mismatch must be retryable — the spoke can resend from its own copy")
	}

	// Verify-before-commit: corrupt bytes must never become stored content.
	if _, ok := tr.Has(DefaultHubID, e.Path); ok {
		t.Error("hub stored content that failed checksum verification")
	}
}

func TestMemoryTransport_PartialThenResume(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()

	content := []byte("the full parquet payload")
	e := entryFor("db/cpu/big.parquet", content)

	// A contact window closes mid-file: only a prefix arrives.
	const prefixLen = 10
	res, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content[:prefixLen]), 0)
	if err != nil {
		t.Fatalf("partial put: %v", err)
	}
	if err := res.Validate(e); err != nil {
		t.Fatalf("partial result failed validation: %v", err)
	}
	if res.Outcome != OutcomePartial {
		t.Fatalf("outcome = %q, want %q", res.Outcome, OutcomePartial)
	}
	if res.BytesAccepted != prefixLen {
		t.Fatalf("bytes accepted = %d, want %d", res.BytesAccepted, prefixLen)
	}
	if _, ok := tr.Has(DefaultHubID, e.Path); ok {
		t.Error("a partially-received file must not be visible as committed content")
	}

	// The next window resumes from the checkpoint, sending only the tail —
	// the property that lets a large file cross a link whose windows are
	// shorter than the transfer.
	res2, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content[res.BytesAccepted:]), res.BytesAccepted)
	if err != nil {
		t.Fatalf("resume put: %v", err)
	}
	if res2.Outcome != OutcomeCommitted {
		t.Fatalf("resumed outcome = %q, want %q", res2.Outcome, OutcomeCommitted)
	}

	// The reassembled file must hash to the original — a resume that spliced
	// the tail at the wrong offset would corrupt silently.
	if got, ok := tr.Has(DefaultHubID, e.Path); !ok || got != e.SHA256 {
		t.Errorf("reassembled digest = %q, want %q", got, e.SHA256)
	}
}

func TestMemoryTransport_ResumeWithoutStagedPrefixFails(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()

	content := []byte("the full parquet payload")
	e := entryFor("db/cpu/big.parquet", content)

	// Resuming against a hub that has no prefix (it restarted, or the spoke's
	// checkpoint is stale) must fail loudly rather than commit a file built
	// from a tail alone.
	_, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content[10:]), 10)
	if err == nil {
		t.Fatal("resume without a staged prefix succeeded; it must fail")
	}
	if !strings.Contains(err.Error(), "no staged prefix") {
		t.Errorf("err = %q, want it to mention the missing prefix", err)
	}
}

func TestMemoryTransport_ScriptedBackpressure(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()

	content := []byte("payload")
	e := entryFor("db/cpu/a.parquet", content)
	tr.ScriptPut(DefaultHubID, e.Path, BackpressureResult(3*time.Second))

	res, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content), 0)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := res.Validate(e); err != nil {
		t.Fatalf("backpressure result failed validation: %v", err)
	}
	if res.Outcome != OutcomeBackpressure {
		t.Fatalf("outcome = %q, want %q", res.Outcome, OutcomeBackpressure)
	}
	if res.RetryAfter != 3*time.Second {
		t.Errorf("retry after = %v, want 3s", res.RetryAfter)
	}
	// The hub deliberately did not read the body, so nothing is stored.
	if _, ok := tr.Has(DefaultHubID, e.Path); ok {
		t.Error("hub stored a file it answered with backpressure")
	}

	// Once the script is exhausted the transport behaves normally, so a test
	// can drive "throttled, then accepted".
	res2, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content), 0)
	if err != nil {
		t.Fatalf("retry after backpressure: %v", err)
	}
	if res2.Outcome != OutcomeCommitted {
		t.Errorf("outcome after backpressure = %q, want %q", res2.Outcome, OutcomeCommitted)
	}
}

func TestBackpressureResult_NeverZeroDelay(t *testing.T) {
	// A zero delay would busy-loop against an already-overloaded hub, so the
	// constructor floors it rather than trusting the caller.
	for _, d := range []time.Duration{0, -time.Second} {
		got := BackpressureResult(d)
		if got.RetryAfter <= 0 {
			t.Errorf("BackpressureResult(%v).RetryAfter = %v, want > 0", d, got.RetryAfter)
		}
		if err := got.Validate(nil); err != nil {
			t.Errorf("BackpressureResult(%v) failed validation: %v", d, err)
		}
	}
}

func TestMemoryTransport_HubIsolation(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()

	content := []byte("payload")
	e := entryFor("db/cpu/a.parquet", content)

	if _, err := tr.PutFile(ctx, "cloud", e, bytes.NewReader(content), 0); err != nil {
		t.Fatalf("put to cloud: %v", err)
	}

	// Delivering to one hub must not mark it delivered to another — the same
	// isolation the ledger's (hub_id, path) key provides on the spoke side.
	res, err := tr.Reconcile(ctx, "factory", []*LedgerEntry{e})
	if err != nil {
		t.Fatalf("reconcile factory: %v", err)
	}
	if len(res.Missing) != 1 {
		t.Errorf("factory missing = %v, want the file to still be missing there", res.Missing)
	}
	if len(res.Present) != 0 {
		t.Errorf("factory present = %v, want empty — hubs are not isolated", res.Present)
	}
}

func TestMemoryTransport_ClosedRejectsCalls(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()
	content := []byte("payload")
	e := entryFor("db/cpu/a.parquet", content)

	if err := tr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := tr.Reconcile(ctx, DefaultHubID, []*LedgerEntry{e}); !errors.Is(err, ErrTransportClosed) {
		t.Errorf("Reconcile after close = %v, want ErrTransportClosed", err)
	}
	if _, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content), 0); !errors.Is(err, ErrTransportClosed) {
		t.Errorf("PutFile after close = %v, want ErrTransportClosed", err)
	}
}

func TestMemoryTransport_HonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tr := NewMemoryTransport()
	content := []byte("payload")
	e := entryFor("db/cpu/a.parquet", content)

	// A contact window that closes must abort in-progress work rather than
	// finishing against a link that is already gone.
	if _, err := tr.Reconcile(ctx, DefaultHubID, []*LedgerEntry{e}); !errors.Is(err, context.Canceled) {
		t.Errorf("Reconcile with cancelled ctx = %v, want context.Canceled", err)
	}
	if _, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content), 0); !errors.Is(err, context.Canceled) {
		t.Errorf("PutFile with cancelled ctx = %v, want context.Canceled", err)
	}
}

func TestMemoryTransport_RejectsBadOffset(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()
	content := []byte("payload")
	e := entryFor("db/cpu/a.parquet", content)

	for _, offset := range []int64{-1, e.SizeBytes + 1} {
		if _, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content), offset); err == nil {
			t.Errorf("PutFile with offset %d succeeded, want an error", offset)
		}
	}
}

func TestMemoryTransport_ConcurrentPutsAreSafe(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()

	// §8.2 runs several transfers at once, so the transport must tolerate it.
	// Run under -race for this to mean anything.
	const files = 16
	errs := make(chan error, files)
	for i := 0; i < files; i++ {
		go func(i int) {
			content := []byte(strings.Repeat("x", i+1))
			e := entryFor("db/cpu/f"+string(rune('a'+i))+".parquet", content)
			_, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content), 0)
			errs <- err
		}(i)
	}
	for i := 0; i < files; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent put: %v", err)
		}
	}
}

func TestMemoryTransport_SamePathConcurrencyNeverOverwrites(t *testing.T) {
	ctx := context.Background()

	// §6.1's never-overwrite rule under concurrency. Two transfers race to
	// write DIFFERENT content to the same path: exactly one may commit, and
	// the other must see a conflict or already-present — never a second
	// commit that silently replaces the first.
	//
	// This is a LOGIC race, not a data race: every map access is mutex-guarded,
	// so -race cannot see it. It only appears as a wrong outcome, which is why
	// it needs its own test rather than relying on the detector. Repeated
	// because the losing interleaving is probabilistic.
	for trial := 0; trial < 200; trial++ {
		tr := NewMemoryTransport()
		contentA := []byte("aaaaaaaaaaaa")
		contentB := []byte("bbbbbbbbbbbb")
		const path = "db/cpu/contested.parquet"
		entryA := entryFor(path, contentA)
		entryB := entryFor(path, contentB)

		var wg sync.WaitGroup
		results := make([]*PutResult, 2)
		errs := make([]error, 2)

		wg.Add(2)
		go func() {
			defer wg.Done()
			results[0], errs[0] = tr.PutFile(ctx, DefaultHubID, entryA, bytes.NewReader(contentA), 0)
		}()
		go func() {
			defer wg.Done()
			results[1], errs[1] = tr.PutFile(ctx, DefaultHubID, entryB, bytes.NewReader(contentB), 0)
		}()
		wg.Wait()

		committed := 0
		for i, r := range results {
			if errs[i] != nil {
				t.Fatalf("trial %d: put %d: %v", trial, i, errs[i])
			}
			if r.Outcome == OutcomeCommitted {
				committed++
			}
		}
		if committed != 1 {
			t.Fatalf("trial %d: %d of 2 concurrent puts committed different content to %q; exactly 1 may win",
				trial, committed, path)
		}

		// Whichever won, the stored digest must match one of the two inputs
		// exactly — not a splice of both.
		got, ok := tr.Has(DefaultHubID, path)
		if !ok {
			t.Fatalf("trial %d: nothing stored after a committed put", trial)
		}
		if got != entryA.SHA256 && got != entryB.SHA256 {
			t.Fatalf("trial %d: stored digest %q matches neither input — content was spliced", trial, got)
		}
	}
}

func TestMemoryTransport_PerPathHubIsolation(t *testing.T) {
	ctx := context.Background()

	// The previous isolation test wrote to one hub and reconciled another, so
	// it only failed if EVERY path lost hubID at once. Real bugs live in one
	// path, so each is checked independently against a hub that holds the file
	// and one that does not.
	content := []byte("payload")
	e := entryFor("db/cpu/a.parquet", content)

	tr := NewMemoryTransport()
	if _, err := tr.PutFile(ctx, "cloud", e, bytes.NewReader(content), 0); err != nil {
		t.Fatalf("put to cloud: %v", err)
	}

	t.Run("Has", func(t *testing.T) {
		if _, ok := tr.Has("cloud", e.Path); !ok {
			t.Error("cloud should hold the file")
		}
		if _, ok := tr.Has("factory", e.Path); ok {
			t.Error("factory reports a file only cloud received — Has ignores hubID")
		}
	})

	t.Run("Reconcile", func(t *testing.T) {
		cloudRes, err := tr.Reconcile(ctx, "cloud", []*LedgerEntry{e})
		if err != nil {
			t.Fatalf("reconcile cloud: %v", err)
		}
		if len(cloudRes.Present) != 1 {
			t.Errorf("cloud present = %v, want the file", cloudRes.Present)
		}

		factoryRes, err := tr.Reconcile(ctx, "factory", []*LedgerEntry{e})
		if err != nil {
			t.Fatalf("reconcile factory: %v", err)
		}
		if len(factoryRes.Present) != 0 {
			t.Errorf("factory present = %v, want empty — Reconcile ignores hubID", factoryRes.Present)
		}
	})

	t.Run("PutFile writes to the named hub", func(t *testing.T) {
		// A second hub must accept the same file independently rather than
		// short-circuiting on the first hub's copy.
		res, err := tr.PutFile(ctx, "factory", e, bytes.NewReader(content), 0)
		if err != nil {
			t.Fatalf("put to factory: %v", err)
		}
		if res.Outcome != OutcomeCommitted {
			t.Errorf("outcome = %q, want %q — PutFile ignores hubID", res.Outcome, OutcomeCommitted)
		}
	})
}

func TestMemoryTransport_ScriptIsHubScoped(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()

	content := []byte("payload")
	e := entryFor("db/cpu/a.parquet", content)

	// Only "cloud" is throttled. A path-keyed script would be consumed by the
	// factory transfer, inverting the test's meaning.
	tr.ScriptPut("cloud", e.Path, BackpressureResult(time.Second))

	factoryRes, err := tr.PutFile(ctx, "factory", e, bytes.NewReader(content), 0)
	if err != nil {
		t.Fatalf("put to factory: %v", err)
	}
	if factoryRes.Outcome != OutcomeCommitted {
		t.Errorf("factory outcome = %q, want %q — it consumed cloud's script",
			factoryRes.Outcome, OutcomeCommitted)
	}

	cloudRes, err := tr.PutFile(ctx, "cloud", e, bytes.NewReader(content), 0)
	if err != nil {
		t.Fatalf("put to cloud: %v", err)
	}
	if cloudRes.Outcome != OutcomeBackpressure {
		t.Errorf("cloud outcome = %q, want %q — its script was consumed elsewhere",
			cloudRes.Outcome, OutcomeBackpressure)
	}
}

func TestMemoryTransport_ConflictReportsHubDigestNotSpokes(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()

	hubContent := []byte("the hub's version")
	spokeContent := []byte("the spoke's version")
	e := entryFor("db/cpu/contested.parquet", spokeContent)
	hubSHA := sha256Hex(hubContent)
	tr.Seed(DefaultHubID, e.Path, hubSHA, int64(len(hubContent)))

	res, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(spokeContent), 0)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	// §6.1 calls the hub's digest the operator's only evidence for telling a
	// spoke_id collision from corruption. Echoing the spoke's own digest back
	// would make the conflict unactionable.
	if res.TheirSHA256 != hubSHA {
		t.Errorf("TheirSHA256 = %q, want the hub's %q", res.TheirSHA256, hubSHA)
	}
	if res.TheirSHA256 == e.SHA256 {
		t.Error("conflict echoed the spoke's own digest")
	}
}

func TestMemoryTransport_StagedCheckpointNeverMovesBackward(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()

	content := []byte("the full parquet payload here")
	e := entryFor("db/cpu/big.parquet", content)

	// A long partial, then a shorter one (a retry that died earlier). If the
	// hub replaced its staged prefix with the shorter attempt, a spoke holding
	// the longer checkpoint would resume past what the hub has and get a hard
	// error with no defined recovery — the file would strand permanently.
	first, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content[:20]), 0)
	if err != nil {
		t.Fatalf("first partial: %v", err)
	}
	if first.BytesAccepted != 20 {
		t.Fatalf("first accepted = %d, want 20", first.BytesAccepted)
	}

	second, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content[:5]), 0)
	if err != nil {
		t.Fatalf("second partial: %v", err)
	}
	if second.BytesAccepted < first.BytesAccepted {
		t.Fatalf("checkpoint moved backward: %d then %d", first.BytesAccepted, second.BytesAccepted)
	}

	// Resuming from the original, longer checkpoint must still work.
	final, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content[first.BytesAccepted:]), first.BytesAccepted)
	if err != nil {
		t.Fatalf("resume from %d: %v", first.BytesAccepted, err)
	}
	if final.Outcome != OutcomeCommitted {
		t.Fatalf("outcome = %q, want %q", final.Outcome, OutcomeCommitted)
	}
	if got, _ := tr.Has(DefaultHubID, e.Path); got != e.SHA256 {
		t.Errorf("reassembled digest = %q, want %q", got, e.SHA256)
	}
}

func TestMemoryTransport_RejectsIncompleteEntry(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()
	content := []byte("payload")

	t.Run("empty sha256", func(t *testing.T) {
		// Without a digest there is nothing to verify against; committing
		// anyway would defeat verify-before-commit entirely.
		e := entryFor("db/cpu/a.parquet", content)
		e.SHA256 = ""
		if _, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content), 0); err == nil {
			t.Error("PutFile with an empty sha256 succeeded; it must be rejected")
		}
	})

	t.Run("empty path", func(t *testing.T) {
		e := entryFor("", content)
		if _, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content), 0); err == nil {
			t.Error("PutFile with an empty path succeeded; it must be rejected")
		}
	})
}

func TestMemoryTransport_RejectsInvalidScriptedResult(t *testing.T) {
	ctx := context.Background()
	content := []byte("payload")
	e := entryFor("db/cpu/a.parquet", content)

	t.Run("nil", func(t *testing.T) {
		tr := NewMemoryTransport()
		tr.ScriptPut(DefaultHubID, e.Path, nil)
		// (nil, nil) would violate the interface contract and panic every
		// caller that reads res.Outcome.
		res, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content), 0)
		if err == nil {
			t.Errorf("scripted nil returned (%v, nil); want an error", res)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		tr := NewMemoryTransport()
		// A conflict with no digest is exactly what Validate exists to reject;
		// scripting must not be a way to bypass it.
		tr.ScriptPut(DefaultHubID, e.Path, &PutResult{Outcome: OutcomeConflict})
		if _, err := tr.PutFile(ctx, DefaultHubID, e, bytes.NewReader(content), 0); err == nil {
			t.Error("an invalid scripted result was returned without error")
		}
	})
}

func TestMemoryTransport_ReconcileRejectsMalformedEntries(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()
	good := entryFor("db/cpu/a.parquet", []byte("payload"))

	t.Run("nil entry", func(t *testing.T) {
		if _, err := tr.Reconcile(ctx, DefaultHubID, []*LedgerEntry{good, nil}); err == nil {
			t.Error("Reconcile with a nil entry succeeded; it must be rejected")
		}
	})

	t.Run("empty path", func(t *testing.T) {
		// An empty path is a valid map key, so without an explicit guard it
		// would classify against hub[""] and land in a result list — which
		// ReconcileResult.Validate then rejects, blaming the result instead
		// of the input that caused it.
		bad := entryFor("", []byte("payload"))
		if _, err := tr.Reconcile(ctx, DefaultHubID, []*LedgerEntry{good, bad}); err == nil {
			t.Error("Reconcile with an empty path succeeded; it must be rejected")
		}
	})
}
