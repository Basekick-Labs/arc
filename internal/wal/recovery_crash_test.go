package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

func crashRecoveryWriter(t *testing.T, dir string) *Writer {
	t.Helper()
	w, err := NewWriter(&WriterConfig{WALDir: dir, SyncMode: SyncModeFsync, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		w.mu.Lock()
		closed := w.closed
		w.mu.Unlock()
		if !closed {
			_ = w.Close()
		}
	})
	return w
}

func TestRecoveryTornTail(t *testing.T) {
	testRecoveryTornTail(t, []map[string]interface{}{{"_measurement": "events", "v": 1}})
}

func testRecoveryTornTail(t *testing.T, rows []map[string]interface{}) {
	t.Helper()
	for _, mode := range []string{"payload", "header", "only_payload", "failed_barrier", "middle_checksum", "last_checksum", "middle_checksum_and_tail"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			w := crashRecoveryWriter(t, dir)
			for i := 0; i < 3; i++ {
				if _, err := w.AppendTracked(rows); err != nil {
					t.Fatal(err)
				}
			}
			path := w.CurrentFile()
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			offsets := []int{WALFileHeaderSize}
			for i := 0; i < 2; i++ {
				o := offsets[i]
				offsets = append(offsets, o+WALEntryHeaderSize+int(binary.BigEndian.Uint32(data[o:o+4])))
			}
			wantRows := 2 * len(rows)
			wantKept := false
			switch mode {
			case "payload", "failed_barrier":
				data = data[:len(data)-4]
				wantKept = mode == "failed_barrier"
			case "header":
				data = data[:offsets[2]+5]
			case "only_payload":
				data = data[:offsets[1]-4]
				wantRows = 0
			case "middle_checksum", "middle_checksum_and_tail":
				data[offsets[1]+WALEntryHeaderSize] ^= 0xff
				wantKept = true
				if mode == "middle_checksum_and_tail" {
					data = data[:len(data)-4]
					wantRows = len(rows)
				}
			case "last_checksum":
				data[offsets[2]+WALEntryHeaderSize] ^= 0xff
				wantKept = true
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			replayed, barriers := 0, 0
			stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(),
				func(_ context.Context, records []map[string]interface{}) error {
					replayed += len(records)
					return nil
				}, &RecoveryOptions{BeforeDelete: func(context.Context) error {
					barriers++
					if mode == "failed_barrier" {
						return errors.New("storage unavailable")
					}
					return nil
				}})
			if (err != nil) != (mode == "failed_barrier") {
				t.Fatalf("recovery error = %v, stats = %+v", err, stats)
			}
			if replayed != wantRows {
				t.Fatalf("replayed %d rows, want %d", replayed, wantRows)
			}
			_, statErr := os.Stat(path)
			if wantKept {
				if statErr != nil || stats.KeptFiles != 1 {
					t.Fatalf("unsafe deletion: stat=%v stats=%+v", statErr, stats)
				}
			} else if !os.IsNotExist(statErr) || stats.KeptFiles != 0 || stats.QuarantinedFiles != 0 {
				t.Fatalf("ordinary crash tail retained: stat=%v stats=%+v", statErr, stats)
			}
			if !wantKept && wantRows > 0 && barriers != 1 {
				t.Fatalf("good rows were not fenced before deletion: barriers=%d", barriers)
			}
		})
	}
}

func TestRecoveryQuarantinePreservesCrossFileCheckpoint(t *testing.T) {
	testRecoveryQuarantinePreservesCrossFileCheckpoint(t, []map[string]interface{}{{"_measurement": "events", "v": 1}})
}

func testRecoveryQuarantinePreservesCrossFileCheckpoint(t *testing.T, rows []map[string]interface{}) {
	t.Helper()
	for _, name := range []string{"plain", "collision_suffix", "scan_error_after_checkpoint"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			w := crashRecoveryWriter(t, dir)
			appendRows := func(records []map[string]interface{}) string {
				t.Helper()
				ids, err := w.AppendTracked(records)
				if err != nil || len(ids) != 1 || len(ids[0]) != 32 {
					t.Fatalf("AppendTracked: ids=%v err=%v", ids, err)
				}
				return ids[0]
			}
			first := w.CurrentFile()
			durableID := appendRows(rows)
			appendRows(rows)
			if err := w.Rotate(); err != nil {
				t.Fatal(err)
			}
			poisonFile := w.CurrentFile()
			if err := w.MarkFlushed([]string{durableID}); err != nil {
				t.Fatal(err)
			}
			poisonID := appendRows(rows)
			if err := w.Rotate(); err != nil {
				t.Fatal(err)
			}
			last := w.CurrentFile()
			appendRows(rows)
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if name == "scan_error_after_checkpoint" {
				// The scanner returns valid checkpoint hashes plus an error for
				// a later oversized header. Recovery must retain the valid prefix.
				f, err := os.OpenFile(poisonFile, os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				var header [WALEntryHeaderSize]byte
				binary.BigEndian.PutUint32(header[:4], MaxWALPayloadSize+1)
				_, writeErr := f.Write(header[:])
				closeErr := f.Close()
				if writeErr != nil || closeErr != nil {
					t.Fatalf("damage header: write=%v close=%v", writeErr, closeErr)
				}
			}
			if name == "collision_suffix" {
				if err := os.WriteFile(poisonFile+".failed", nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			durableReplays, poisonCalls := 0, 0
			callback := func(_ context.Context, _ []map[string]interface{}, id string) error {
				if id == durableID {
					durableReplays++
				}
				if id == poisonID {
					poisonCalls++
					return errors.New("permanent invalid entry")
				}
				return nil
			}
			stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil,
				&RecoveryOptions{TrackedRowCallback: callback, MaxReplayFailures: 1,
					BeforeDelete: func(context.Context) error { return errors.New("storage unavailable") }})
			if err == nil || stats.QuarantinedFiles != 1 || stats.KeptFiles != 2 || durableReplays != 0 {
				t.Fatalf("first pass: stats=%+v durableReplays=%d err=%v", stats, durableReplays, err)
			}
			for _, path := range []string{first, last} {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("pending file lost: %v", err)
				}
			}
			// A fresh Recovery models a process restart: the only proof that the
			// first entry is durable is now inside the quarantined checkpoint file.
			stats, err = NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil,
				&RecoveryOptions{TrackedRowCallback: callback, BeforeDelete: func(context.Context) error { return nil }})
			if err != nil || stats.KeptFiles != 0 || stats.RecoveredFiles != 2 {
				t.Fatalf("second pass: stats=%+v err=%v", stats, err)
			}
			if durableReplays != 0 || poisonCalls != 1 {
				t.Fatalf("checkpoint lost or quarantine replayed: durableReplays=%d poisonCalls=%d", durableReplays, poisonCalls)
			}
			quarantined, err := filepath.Glob(filepath.Join(dir, "*.wal*.failed"))
			if err != nil || len(quarantined) == 0 {
				t.Fatalf("quarantine was not preserved: files=%v err=%v", quarantined, err)
			}
		})
	}
}
