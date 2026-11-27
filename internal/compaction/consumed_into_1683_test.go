package compaction

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Recovery re-fires the output-aware observer with the job's output after
// the inputs-only one, and keeps the manifest when it fails (#1683).
func TestRecoveryRefiresConsumedIntoWithTheOutput(t *testing.T) {
	ctx := context.Background()
	backend := nsTestBackend(t)
	mm := NewManifestManager(backend, zerolog.Nop())
	const out = "rocket-01/db/meas/2026/08/19/10/out_compacted.parquet"
	if err := backend.Write(ctx, out, []byte("data")); err != nil {
		t.Fatal(err)
	}
	mpath, err := mm.WriteManifest(ctx, &Manifest{
		OutputPath: out, OutputSize: 4,
		InputFiles: []string{"rocket-01/db/meas/2026/08/19/10/raw1.parquet"},
		Database:   "rocket-01/db", Measurement: "meas", PartitionPath: "rocket-01/db/meas/2026/08/19/10",
		Tier: "hourly", Status: ManifestStatusPending, CreatedAt: time.Now().UTC(), JobID: "j1",
	})
	if err != nil {
		t.Fatal(err)
	}
	ok := func([]string) error { return nil }

	failing := recoveryHooks{consumedInto: func([]string, string) error { return errors.New("sqlite busy") }}
	if _, err := mm.recoverOrphanedManifestsWithHooks(ctx, recoveryScope{}, nil, ok, failing); err == nil {
		t.Fatal("recovery must report a failed holder mark")
	}
	if present, _ := backend.Exists(ctx, mpath); !present {
		t.Fatal("manifest deleted despite a failed holder mark")
	}

	var gotInputs []string
	var gotOutput string
	hooks := recoveryHooks{consumedInto: func(in []string, o string) error { gotInputs, gotOutput = in, o; return nil }}
	if _, err := mm.recoverOrphanedManifestsWithHooks(ctx, recoveryScope{}, nil, ok, hooks); err != nil {
		t.Fatal(err)
	}
	if gotOutput != out || len(gotInputs) != 1 {
		t.Fatalf("consumedInto(%v, %q), want the input and %q", gotInputs, gotOutput, out)
	}
	if present, _ := backend.Exists(ctx, mpath); present {
		t.Fatal("manifest not finalized after both marks succeeded")
	}
}

// The manager's FinishStagedInto and the legacy path's notify reach the
// output-aware observer; nothing is called without an output.
func TestManagerNotifyConsumedInto(t *testing.T) {
	m := &Manager{}
	var got string
	m.SetOnConsumedInto(func(_ []string, o string) error { got = o; return nil })
	if err := m.FinishStagedInto([]string{"a"}, "out"); err != nil || got != "out" {
		t.Fatalf("got %q err %v", got, err)
	}
	got = ""
	if err := m.notifyConsumedInto([]string{"a"}, ""); err != nil || got != "" {
		t.Fatalf("called without an output: %q", got)
	}
}
