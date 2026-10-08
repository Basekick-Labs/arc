package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/rs/zerolog"
)

func newReceivedTestWriter(t *testing.T) *Writer {
	t.Helper()
	w, err := NewWriter(&WriterConfig{WALDir: t.TempDir(), SyncMode: SyncModeFdatasync, BufferSize: 1024, MaxSizeBytes: 1 << 40, MaxAge: time.Hour, Logger: zerolog.Nop()})
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

func receivedTestPayload(t *testing.T, seq uint64) []byte {
	t.Helper()
	body, err := msgpack.Marshal(map[string]interface{}{"m": "cpu", "columns": map[string]interface{}{"time": []int64{1700000000000000}, "v": []int64{42}}})
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, walTrackedHeaderSize+len(body))
	payload[0] = WALTrackedMarker
	binary.BigEndian.PutUint64(payload[1:9], 1234)
	binary.BigEndian.PutUint64(payload[9:17], seq)
	copy(payload[17:], body)
	return payload
}

func TestReceivedWALPreservesIdentityAndProvenance(t *testing.T) {
	w := newReceivedTestWriter(t)
	payload := receivedTestPayload(t, 1)
	original := bytes.Clone(payload)
	w.SetReplicationHook(func(*ReplicationEntry) { t.Error("received WAL was replicated again") })
	inputs := [][]byte{receivedTestPayload(t, 1), receivedTestPayload(t, 2), payload}
	for _, input := range inputs[:2] {
		if err := w.AppendReplicated(input); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.AppendReplicated(payload); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, original) {
		t.Fatal("authenticated caller payload mutated")
	}
	path := w.CurrentFile()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reader := NewReader(path, zerolog.Nop())
	entries, err := reader.ReadAll()
	if err != nil || reader.CorruptedEntries != 0 || len(entries) != 3 {
		t.Fatalf("entries=%d corrupt=%d err=%v", len(entries), reader.CorruptedEntries, err)
	}
	for i, entry := range entries {
		want := inputs[i]
		identity, _, _ := TrackedPayload(want)
		if entry.PayloadHash != identity || !bytes.Equal(entry.ReplicationPayload, want) {
			t.Fatalf("lost origin at entry %d", i)
		}
		if entry.ColumnarData != nil || entry.Records != nil {
			t.Fatal("received entry exposed to originating recovery")
		}
	}
}

func TestReceivedRecoveryRequiresDurableReplicaFlush(t *testing.T) {
	for _, mode := range []string{"no_callbacks", "no_flush", "apply_failure", "flush_failure", "success"} {
		t.Run(mode, func(t *testing.T) {
			w := newReceivedTestWriter(t)
			if err := w.AppendReplicated(receivedTestPayload(t, 1)); err != nil {
				t.Fatal(err)
			}
			path := w.CurrentFile()
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			applied, flushed := false, false
			opts := &RecoveryOptions{}
			if mode != "no_callbacks" {
				opts.ReplicationCallback = func(context.Context, []byte) error {
					applied = true
					if mode == "apply_failure" {
						return errors.New("apply failed")
					}
					return nil
				}
			}
			if mode != "no_callbacks" && mode != "no_flush" {
				opts.ReplicationFlush = func(context.Context) error {
					if !applied {
						t.Fatal("flush before apply")
					}
					if _, err := os.Stat(path); err != nil {
						t.Fatal("WAL removed before durable flush")
					}
					flushed = true
					if mode == "flush_failure" {
						return errors.New("flush failed")
					}
					return nil
				}
			}
			_, err := NewRecovery(w.config.WALDir, zerolog.Nop()).RecoverWithOptions(context.Background(), func(context.Context, []map[string]interface{}) error {
				t.Fatal("received rows routed to origin")
				return nil
			}, opts)
			if mode == "success" {
				if err != nil || !flushed {
					t.Fatalf("success err=%v flushed=%v", err, flushed)
				}
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("successful recovery retained WAL: %v", err)
				}
			} else {
				if err == nil {
					t.Fatal("missing recovery error")
				}
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("failed recovery discarded WAL: %v", err)
				}
			}
		})
	}
}

func TestReceivedWALPurgesOnlyAfterCheckpoint(t *testing.T) {
	w := newReceivedTestWriter(t)
	payload := receivedTestPayload(t, 1)
	identity, _, _ := TrackedPayload(payload)
	if err := w.AppendReplicated(payload); err != nil {
		t.Fatal(err)
	}
	path := w.CurrentFile()
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	// Neither the foreign origin identity nor an abandoned local write can
	// accidentally release the receiver's independent durability obligation.
	w.ForgetTracked([]string{identity})
	for _, purge := range []func() (int, error){w.PurgeInactive, w.PurgeAll, func() (int, error) { return w.PurgeUnaccountedOlderThan(-time.Hour) }, func() (int, error) { return w.PurgeFlushed(w.MinUnflushedSequence()) }} {
		if _, err := purge(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("unflushed received WAL discarded: %v", err)
		}
	}
	if err := w.MarkFlushed([]string{identity}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.PurgeFlushed(w.MinUnflushedSequence()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("durable received WAL not reclaimed: %v", err)
	}
}

func TestPreviousProcessReceivedWALSurvivesAgePurge(t *testing.T) {
	w := newReceivedTestWriter(t)
	if err := w.AppendReplicated(receivedTestPayload(t, 1)); err != nil {
		t.Fatal(err)
	}
	path := w.CurrentFile()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := NewWriter(&w.config)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if _, err := next.PurgeUnaccountedOlderThan(-time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("previous-process replica data discarded: %v", err)
	}
}

// Legacy purge tests use a valid header-only file rather than arbitrary bytes:
// unknown framing must now be retained because it may hide received entries.
func emptyWALFixture() []byte {
	data := make([]byte, WALFileHeaderSize)
	copy(data, WALMagic)
	binary.BigEndian.PutUint16(data[4:6], WALVersion)
	data[6] = WALChecksumCRC32
	return data
}

func TestForeignReceivedWALKeepsLaterCheckpoint(t *testing.T) {
	w := newReceivedTestWriter(t)
	payload := receivedTestPayload(t, 1)
	identity, _, _ := TrackedPayload(payload)
	if err := w.AppendReplicated(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := NewWriter(&w.config)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if err := next.MarkFlushed([]string{identity}); err != nil {
		t.Fatal(err)
	}
	checkpointPath := next.CurrentFile()
	if err := next.Rotate(); err != nil {
		t.Fatal(err)
	}
	for _, purge := range []func() (int, error){func() (int, error) { return next.PurgeFlushed(next.MinUnflushedSequence()) }, func() (int, error) { return next.PurgeUnaccountedOlderThan(-time.Hour) }} {
		if _, err := purge(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(checkpointPath); err != nil {
			t.Fatalf("purge orphaned received data from its durable checkpoint: %v", err)
		}
	}
}
