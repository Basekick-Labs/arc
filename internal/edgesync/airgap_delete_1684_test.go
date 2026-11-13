package edgesync

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// The importer signs its verify answer into the ack; ReadAck accepts it and
// refuses any tampering with the attestation, which decides what a spoke
// deletes.
func TestAck_HeldAttestationSignedAndTamperProof(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)
	rig.importer.verify = func(_ context.Context, _ string, entries []ReconcileEntry) (*VerifyResult, error) {
		return &VerifyResult{Held: []string{entries[0].Path}, HeldCompacted: []string{entries[1].Path}, Missing: []string{}}, nil
	}
	dir := exportBundle(t, rig.secret, 3, testHubID)
	if _, err := rig.importer.Import(ctx, dir); err != nil {
		t.Fatal(err)
	}
	ack, err := ReadAck(dir, rig.secret, testSpokeID, testHubID)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if !ack.HeldAttested() || len(ack.Held) != 1 || len(ack.HeldCompacted) != 1 || ack.VerifiedAt == 0 {
		t.Fatalf("attestation = held %v compacted %v at %d", ack.Held, ack.HeldCompacted, ack.VerifiedAt)
	}

	raw, err := os.ReadFile(filepath.Join(dir, ackName))
	if err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(a *Ack){
		"held swapped":        func(a *Ack) { a.Held, a.HeldCompacted = a.HeldCompacted, a.Held },
		"held grows":          func(a *Ack) { a.Held = append(a.Held, a.Paths[2]) },
		"held not acked":      func(a *Ack) { a.Held = []string{"default/cpu/2026/08/07/14/other.parquet"} },
		"mac stripped":        func(a *Ack) { a.HeldMAC = "" },
		"verified_at changed": func(a *Ack) { a.VerifiedAt++ },
	} {
		t.Run(name, func(t *testing.T) {
			var a Ack
			mustUnmarshal(t, raw, &a)
			edit(&a)
			if err := writeJSONFile(filepath.Join(dir, ackName), &a); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadAck(dir, rig.secret, testSpokeID, testHubID); !errors.Is(err, ErrAckInvalid) {
				t.Fatalf("tampered ack accepted: %v", err)
			}
		})
	}
	// A verify failure: the ack still goes out, without an attestation.
	rig2 := newImportRig(t, false)
	rig2.importer.verify = func(context.Context, string, []ReconcileEntry) (*VerifyResult, error) {
		return nil, errors.New("hub unavailable")
	}
	dir2 := exportBundle(t, rig2.secret, 2, testHubID)
	if _, err := rig2.importer.Import(ctx, dir2); err != nil {
		t.Fatal(err)
	}
	if ack2, err := ReadAck(dir2, rig2.secret, testSpokeID, testHubID); err != nil || ack2.HeldAttested() {
		t.Fatalf("ack after a failed verify: attested=%v err=%v", ack2 != nil && ack2.HeldAttested(), err)
	}
}

type airgapRig struct {
	l       *Ledger
	files   map[string]bool
	deleted []string
	d       *AttestedDeleter
}

func newAirgapRig(t *testing.T, paths ...string) *airgapRig {
	t.Helper()
	r := &airgapRig{l: setupTestLedger(t), files: map[string]bool{}}
	for _, p := range paths {
		e := testEntry(p)
		e.HubID = testHubID
		if err := r.l.Track(context.Background(), e); err != nil {
			t.Fatal(err)
		}
		r.files[p] = true
	}
	r.d = &AttestedDeleter{Ledger: r.l, HubID: testHubID, MaxAge: 7 * 24 * time.Hour, Logger: zerolog.Nop(),
		Delete: func(_ context.Context, p string, _ int64) error {
			r.deleted = append(r.deleted, p)
			delete(r.files, p)
			return nil
		}}
	return r
}

func (r *airgapRig) export(t *testing.T, bundle string, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if err := r.l.MarkExported(context.Background(), testHubID, p, bundle); err != nil {
			t.Fatal(err)
		}
	}
}

func attestedAck(bundle string, paths, held, compacted []string) *Ack {
	return &Ack{BundleID: bundle, HubID: testHubID, SpokeID: testSpokeID, Paths: paths,
		Held: held, HeldCompacted: compacted, VerifiedAt: 1, HeldMAC: "verified-by-ReadAck"}
}

// An applied attestation deletes the held files (and held_compacted only when
// accepted); an ack without one deletes nothing.
func TestAttestedDeleter_DeletesWhatTheAckAttests(t *testing.T) {
	ctx := context.Background()
	const a, b, c = "default/cpu/2026/08/06/14/a.parquet", "default/cpu/2026/08/06/14/b.parquet", "default/cpu/2026/08/06/14/c.parquet"
	r := newAirgapRig(t, a, b, c)
	r.export(t, "B1", a, b, c)
	res, err := ApplyAck(ctx, r.l, attestedAck("B1", []string{a, b, c}, []string{a}, []string{b}), zerolog.Nop())
	if err != nil || res.Attested != 2 {
		t.Fatalf("apply: attested=%d err=%v", res.Attested, err)
	}
	if n, err := r.d.Run(ctx); err != nil || n != 1 || r.files[a] {
		t.Fatalf("run: deleted %d (%v) err=%v, want only the held file", n, r.deleted, err)
	}
	r.d.AcceptCompacted = true
	if n, _ := r.d.Run(ctx); n != 1 || r.files[b] || !r.files[c] {
		t.Fatalf("accept compacted: deleted %v, want b too and never c", r.deleted)
	}
}

// Replay after a requeue, a stale ack over a fresher verification, an ack
// for another bundle, an attestation past its age: nothing deleted.
func TestAttestedDeleter_Guards(t *testing.T) {
	ctx := context.Background()
	const p = "default/cpu/2026/08/06/14/a.parquet"
	ack := attestedAck("B1", []string{p}, []string{p}, nil)

	t.Run("requeued after export", func(t *testing.T) {
		r := newAirgapRig(t, p)
		r.export(t, "B1", p)
		if err := r.l.MarkSynced(ctx, testHubID, p); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
		if err := r.l.RequeueSynced(ctx, testHubID, p); err != nil { // the hub forgot it
			t.Fatal(err)
		}
		if res, err := ApplyAck(ctx, r.l, ack, zerolog.Nop()); err != nil || res.Attested != 0 {
			t.Fatalf("an old ack replayed after a requeue was attested (%d, %v)", res.Attested, err)
		}
		if n, _ := r.d.Run(ctx); n != 0 {
			t.Fatal("deleted after a requeue")
		}
	})
	t.Run("fresher verification wins", func(t *testing.T) {
		r := newAirgapRig(t, p)
		r.export(t, "B1", p)
		if err := r.l.MarkSynced(ctx, testHubID, p); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
		if err := r.l.MarkVerified(ctx, testHubID, p, HubStateKept); err != nil { // network said kept, later
			t.Fatal(err)
		}
		if res, _ := ApplyAck(ctx, r.l, ack, zerolog.Nop()); res.Attested != 0 {
			t.Fatal("a stale ack overwrote a fresher verification")
		}
	})
	t.Run("another bundle", func(t *testing.T) {
		r := newAirgapRig(t, p)
		r.export(t, "B2", p)
		if res, _ := ApplyAck(ctx, r.l, ack, zerolog.Nop()); res.Attested != 0 {
			t.Fatal("an ack for a bundle that did not carry the file was attested")
		}
	})
	t.Run("too old", func(t *testing.T) {
		r := newAirgapRig(t, p)
		r.export(t, "B1", p)
		if res, _ := ApplyAck(ctx, r.l, ack, zerolog.Nop()); res.Attested != 1 {
			t.Fatal("premise: attested")
		}
		r.d.MaxAge = time.Nanosecond
		time.Sleep(2 * time.Millisecond)
		if n, _ := r.d.Run(ctx); n != 0 {
			t.Fatal("an attestation past its max age deleted a file")
		}
	})
	t.Run("delay", func(t *testing.T) {
		r := newAirgapRig(t, p)
		r.export(t, "B1", p)
		ApplyAck(ctx, r.l, ack, zerolog.Nop())
		r.d.Delay = time.Hour
		if n, _ := r.d.Run(ctx); n != 0 {
			t.Fatal("deleted before the delay")
		}
	})
}

func mustUnmarshal(t *testing.T, raw []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatal(err)
	}
}
