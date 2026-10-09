package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// A checkpoint covering a row-range identity, or a whole-entry token from an
// earlier writer instance, describes entries in a file recovery RETAINED. That
// file is absent from w.fileOrder, so the purge walk cannot see it and cannot
// stop at it — and the file holding the checkpoint is the only record that
// those entries were flushed. Deleting it makes recovery replay rows that are
// already in storage: permanent duplicates for a measurement without tags,
// which is the hazard PurgeFlushed's own doc comment describes.
func TestPurgeFlushedPinsFilesHoldingForeignProof(t *testing.T) {
	for _, tc := range []struct {
		name     string
		identity func(w *Writer) string
		pinned   bool
	}{
		{
			name: "row range identity from a retained file",
			// The parent is another instance's token; the range form is what
			// bounded tracked replay checkpoints.
			identity: func(*Writer) string {
				return recoveryRowIdentity("ffffffffffffffff0000000000000001", 0, 2)
			},
			pinned: true,
		},
		{
			name: "whole-entry token from an earlier writer instance",
			identity: func(*Writer) string {
				return "ffffffffffffffff0000000000000007"
			},
			pinned: true,
		},
		{
			name: "this process's own token is not foreign",
			identity: func(w *Writer) string {
				// Mint a real identity so the instance prefix matches.
				hashes, err := w.AppendTracked([]map[string]interface{}{{"k": 1}})
				if err != nil || len(hashes) != 1 {
					t.Fatalf("AppendTracked: %v hashes=%v", err, hashes)
				}
				return hashes[0]
			},
			pinned: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writer, err := NewWriter(&WriterConfig{
				WALDir: dir, SyncMode: SyncModeFsync, MaxSizeBytes: 100 * 1024 * 1024, Logger: zerolog.Nop(),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()

			identity := tc.identity(writer)

			// The checkpoint lands in the file that is current now.
			proofFile := writer.CurrentFile()
			if err := writer.MarkFlushed([]string{identity}); err != nil {
				t.Fatalf("MarkFlushed: %v", err)
			}
			// Rotate so the proof file is no longer active and becomes an
			// ordinary purge candidate.
			if err := writer.Rotate(); err != nil {
				t.Fatal(err)
			}

			deleted, err := writer.PurgeFlushed(writer.MinUnflushedSequence())
			if err != nil {
				t.Fatalf("PurgeFlushed: %v", err)
			}
			_, statErr := os.Stat(proofFile)
			present := statErr == nil

			if tc.pinned {
				if !present {
					t.Fatalf("the file holding the only proof for a retained file was purged (deleted=%d); recovery will replay rows already in storage", deleted)
				}
			} else if present && deleted == 0 {
				t.Fatalf("a file holding only this process's own checkpoint was pinned; nothing would ever reclaim it")
			}
		})
	}
}

// A sidecar that cannot be written is the WAL-volume-full case quarantine
// exists for. Aborting the pass left the poison file in place AND every later
// file unreached, so the feature failed in its own target scenario.
//
// The failure is injected through writeReplayAttemptsFn, because planting an
// obstruction at the sidecar path fails the READ instead — a different path,
// and the reason the existing metadata-failure cases do not cover this one.
// The WAL directory stays writable, so the quarantine rename still succeeds,
// which is exactly the out-of-space shape.
func TestReplayFailureFallsBackWhenSidecarCannotBeWritten(t *testing.T) {
	dir, files := writeRecoveryFiles(t, 1)
	path := files[0]

	orig := writeReplayAttemptsFn
	writeReplayAttemptsFn = func(string, int) error { return errors.New("no space left on device") }
	t.Cleanup(func() { writeReplayAttemptsFn = orig })

	rec := NewRecovery(dir, zerolog.Nop())

	// maxAttempts 1, so one failed pass reaches the quarantine decision. That
	// decision must still happen: the rename needs no free space.
	quarantined, err := rec.noteReplayFailure(path, 1)
	if err != nil {
		t.Fatalf("a sidecar that cannot be written must not abort the pass: %v", err)
	}
	if !quarantined {
		t.Fatal("the file was neither quarantined nor counted; the poison file stays and every later file is never reached")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Errorf("original path still present after quarantine: %v", statErr)
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "*.wal.failed"))
	if len(matches) != 1 {
		t.Errorf("quarantined files = %v, want exactly one", matches)
	}
}

// Below maxAttempts, an unwritable sidecar must still count the pass in memory
// so repeated failures eventually quarantine rather than retrying for ever.
func TestUnwritableSidecarStillAccumulatesInMemory(t *testing.T) {
	dir, files := writeRecoveryFiles(t, 1)
	path := files[0]
	orig := writeReplayAttemptsFn
	writeReplayAttemptsFn = func(string, int) error { return errors.New("no space left on device") }
	t.Cleanup(func() { writeReplayAttemptsFn = orig })

	rec := NewRecovery(dir, zerolog.Nop())
	for pass := 1; pass <= 2; pass++ {
		quarantined, err := rec.noteReplayFailure(path, 3)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if quarantined {
			t.Fatalf("pass %d quarantined before reaching maxAttempts 3", pass)
		}
	}
	quarantined, err := rec.noteReplayFailure(path, 3)
	if err != nil {
		t.Fatalf("third pass: %v", err)
	}
	if !quarantined {
		t.Fatal("three failed passes did not quarantine; the in-memory fallback is not accumulating, so a full volume retries for ever")
	}
}
