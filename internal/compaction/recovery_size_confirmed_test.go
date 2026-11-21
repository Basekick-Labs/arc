package compaction

import (
	"context"
	"errors"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// faultyBackend is a LocalBackend whose StatFile, Delete and Exists can be
// made to fail.
type faultyBackend struct {
	storage.Backend
	statErr    error
	statSize   *int64
	failDelete string
	failExists string
}

func (b *faultyBackend) Exists(ctx context.Context, path string) (bool, error) {
	if path != "" && path == b.failExists {
		return false, errors.New("injected exists failure")
	}
	return b.Backend.Exists(ctx, path)
}

func (b *faultyBackend) StatFile(ctx context.Context, path string) (int64, error) {
	if b.statErr != nil {
		return 0, b.statErr
	}
	if b.statSize != nil {
		return *b.statSize, nil
	}
	return b.Backend.StatFile(ctx, path)
}

func (b *faultyBackend) Delete(ctx context.Context, path string) error {
	if path == b.failDelete {
		return errors.New("injected delete failure")
	}
	return b.Backend.Delete(ctx, path)
}

const (
	sizeTestOutput = "db/cpu/2026/01/02/03/compacted.parquet"
	sizeTestInputA = "db/cpu/2026/01/02/03/a.parquet"
	sizeTestInputB = "db/cpu/2026/01/02/03/b.parquet"
)

// newSizeCheckFixture stores an output of outputBytes, the named inputs, and
// a manifest recording recordedSize for the output and both inputs.
func newSizeCheckFixture(t *testing.T, outputBytes string, recordedSize int64, inputs ...string) (*faultyBackend, *ManifestManager, string) {
	t.Helper()
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close() })
	backend := &faultyBackend{Backend: local}
	ctx := context.Background()
	if err := backend.Write(ctx, sizeTestOutput, []byte(outputBytes)); err != nil {
		t.Fatal(err)
	}
	for _, in := range inputs {
		if err := backend.Write(ctx, in, []byte("source")); err != nil {
			t.Fatal(err)
		}
	}
	mm := NewManifestManager(backend, zerolog.Nop())
	manifestPath, err := mm.WriteManifest(ctx, &Manifest{
		OutputPath:  sizeTestOutput,
		OutputSize:  recordedSize,
		InputFiles:  []string{sizeTestInputA, sizeTestInputB},
		Database:    "db",
		Measurement: "cpu",
		Tier:        "hourly",
		Status:      ManifestStatusPending,
		JobID:       "size-check",
	})
	if err != nil {
		t.Fatal(err)
	}
	return backend, mm, manifestPath
}

func mustExist(t *testing.T, b storage.Backend, want bool, paths ...string) {
	t.Helper()
	for _, p := range paths {
		got, err := b.Exists(context.Background(), p)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s: exists = %v, want %v", p, got, want)
		}
	}
}

// The size of the output cannot be read: recovery must not delete the inputs
// on the strength of a check it did not make.
func TestRecoveryKeepsInputsWhenOutputSizeCannotBeRead(t *testing.T) {
	backend, mm, manifestPath := newSizeCheckFixture(t, "full output", 11, sizeTestInputA, sizeTestInputB)
	backend.statErr = errors.New("injected stat failure")

	recovered, err := mm.RecoverOrphanedManifests(context.Background(), nil, nil)
	if err == nil || recovered != 0 {
		t.Fatalf("recovery = (%d, %v), want (0, error)", recovered, err)
	}
	mustExist(t, backend, true, sizeTestOutput, sizeTestInputA, sizeTestInputB, manifestPath)

	// The failure clears: the same manifest completes.
	backend.statErr = nil
	recovered, err = mm.RecoverOrphanedManifests(context.Background(), nil, nil)
	if err != nil || recovered != 1 {
		t.Fatalf("recovery after the failure cleared = (%d, %v), want (1, nil)", recovered, err)
	}
	mustExist(t, backend, true, sizeTestOutput)
	mustExist(t, backend, false, sizeTestInputA, sizeTestInputB, manifestPath)
}

// StatFile answers "no such object" (-1) for an output Exists just reported:
// not a size mismatch, so the output must not be deleted as a partial upload.
func TestRecoveryDoesNotDiscardAnOutputWhoseSizeIsUnknown(t *testing.T) {
	backend, mm, manifestPath := newSizeCheckFixture(t, "full output", 11, sizeTestInputA, sizeTestInputB)
	gone := int64(-1)
	backend.statSize = &gone

	recovered, err := mm.RecoverOrphanedManifests(context.Background(), nil, nil)
	if err == nil || recovered != 0 {
		t.Fatalf("recovery = (%d, %v), want (0, error)", recovered, err)
	}
	mustExist(t, backend, true, sizeTestOutput, sizeTestInputA, sizeTestInputB, manifestPath)
}

// The output is short AND an input is already gone: the output may be the
// only copy of that input's rows, so nothing is deleted, and the manifest
// stays so the files it names are still held back from compaction.
func TestRecoveryKeepsAShortOutputWhenAnInputIsAlreadyGone(t *testing.T) {
	backend, mm, manifestPath := newSizeCheckFixture(t, "short", 99999, sizeTestInputA) // b was never stored

	for pass := 1; pass <= 2; pass++ {
		recovered, err := mm.RecoverOrphanedManifests(context.Background(), nil, nil)
		if err == nil || recovered != 0 {
			t.Fatalf("pass %d: recovery = (%d, %v), want (0, error)", pass, recovered, err)
		}
		mustExist(t, backend, true, sizeTestOutput, sizeTestInputA, manifestPath)
	}
	files, err := mm.GetFilesInManifests(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{sizeTestOutput, sizeTestInputA} {
		if _, ok := files[p]; !ok {
			t.Errorf("%s is no longer held back from compaction", p)
		}
	}
}

// Whether an input is still stored cannot be established: same refusal.
func TestRecoveryKeepsAShortOutputWhenAnInputCannotBeChecked(t *testing.T) {
	backend, mm, manifestPath := newSizeCheckFixture(t, "short", 99999, sizeTestInputA, sizeTestInputB)
	backend.failExists = sizeTestInputB

	recovered, err := mm.RecoverOrphanedManifests(context.Background(), nil, nil)
	if err == nil || recovered != 0 {
		t.Fatalf("recovery = (%d, %v), want (0, error)", recovered, err)
	}
	backend.failExists = ""
	mustExist(t, backend, true, sizeTestOutput, sizeTestInputA, sizeTestInputB, manifestPath)
}

// A manifest that names no inputs gives no assurance that the rows are
// anywhere but the output.
func TestRecoveryKeepsAShortOutputWhenTheManifestNamesNoInputs(t *testing.T) {
	backend, mm, _ := newSizeCheckFixture(t, "short", 99999)
	ctx := context.Background()
	manifestPath, err := mm.WriteManifest(ctx, &Manifest{
		OutputPath: sizeTestOutput, OutputSize: 99999, Database: "db", Measurement: "cpu",
		Tier: "hourly", Status: ManifestStatusPending, JobID: "size-check",
	})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := mm.RecoverOrphanedManifests(ctx, nil, nil)
	if err == nil || recovered != 0 {
		t.Fatalf("recovery = (%d, %v), want (0, error)", recovered, err)
	}
	mustExist(t, backend, true, sizeTestOutput, manifestPath)
}

// A manifest with no recorded size cannot confirm anything, so it must not
// lead to the inputs being deleted (0 == 0 is not a confirmation).
func TestRecoveryKeepsInputsWhenTheManifestRecordsNoSize(t *testing.T) {
	backend, mm, manifestPath := newSizeCheckFixture(t, "", 0, sizeTestInputA, sizeTestInputB)

	recovered, err := mm.RecoverOrphanedManifests(context.Background(), nil, nil)
	if err == nil || recovered != 0 {
		t.Fatalf("recovery = (%d, %v), want (0, error)", recovered, err)
	}
	mustExist(t, backend, true, sizeTestOutput, sizeTestInputA, sizeTestInputB, manifestPath)
}

// The short output cannot be deleted: the manifest must stay, or the output
// and its inputs sit side by side with nothing keeping them out of the next
// compaction.
func TestRecoveryKeepsTheManifestWhenAShortOutputCannotBeDeleted(t *testing.T) {
	backend, mm, manifestPath := newSizeCheckFixture(t, "short", 99999, sizeTestInputA, sizeTestInputB)
	backend.failDelete = sizeTestOutput

	recovered, err := mm.RecoverOrphanedManifests(context.Background(), nil, nil)
	if err == nil || recovered != 0 {
		t.Fatalf("recovery = (%d, %v), want (0, error)", recovered, err)
	}
	mustExist(t, backend, true, sizeTestOutput, sizeTestInputA, sizeTestInputB, manifestPath)

	files, err := mm.GetFilesInManifests(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{sizeTestOutput, sizeTestInputA, sizeTestInputB} {
		if _, ok := files[p]; !ok {
			t.Errorf("%s is no longer held back from compaction", p)
		}
	}

	// The delete starts working: the job is undone and can be redone.
	backend.failDelete = ""
	recovered, err = mm.RecoverOrphanedManifests(context.Background(), nil, nil)
	if err != nil || recovered != 1 {
		t.Fatalf("recovery after the failure cleared = (%d, %v), want (1, nil)", recovered, err)
	}
	mustExist(t, backend, false, sizeTestOutput, manifestPath)
	mustExist(t, backend, true, sizeTestInputA, sizeTestInputB)
}
