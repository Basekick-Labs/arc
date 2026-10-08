package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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
	for _, purge := range []func() (int, error){w.PurgeInactive, w.PurgeAll, func() (int, error) { return w.PurgeFlushed(w.MinUnflushedSequence()) }} {
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
	for _, purge := range []func() (int, error){func() (int, error) { return next.PurgeFlushed(next.MinUnflushedSequence()) }} {
		if _, err := purge(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(checkpointPath); err != nil {
			t.Fatalf("purge orphaned received data from its durable checkpoint: %v", err)
		}
	}
}

func TestReplicationRecoveryRefusesAmbiguousLegacyWAL(t *testing.T) {
	w := newReceivedTestWriter(t)
	body, err := msgpack.Marshal(map[string]interface{}{"m": "cpu", "columns": map[string]interface{}{"time": []int64{1700000000000000}, "v": []int64{42}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendRawWithMeta("db", body); err != nil {
		t.Fatal(err)
	}
	path := w.CurrentFile()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	invoked := false
	recovery := NewRecovery(w.config.WALDir, zerolog.Nop())
	_, err = recovery.RecoverWithOptions(context.Background(), func(context.Context, []map[string]interface{}) error { invoked = true; return nil }, &RecoveryOptions{
		RequireOriginIdentity: true,
		ColumnarCallback: func(context.Context, string, string, map[string][]interface{}, string) error {
			invoked = true
			return nil
		},
	})
	if err == nil || invoked {
		t.Fatalf("ambiguous WAL was replayed: invoked=%v err=%v", invoked, err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("migration refusal changed the only durable WAL copy")
	}
}

func TestReceivedRecoveryUsesSharedBeforeDeleteBarrier(t *testing.T) {
	w := newReceivedTestWriter(t)
	payload := receivedTestPayload(t, 1)
	if err := w.AppendReplicated(payload); err != nil {
		t.Fatal(err)
	}
	path := w.CurrentFile()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("asynchronous flush failed before the barrier")
	applied, barriers := 0, 0
	options := &RecoveryOptions{
		ReplicationCallback: func(context.Context, []byte) error { applied++; return nil },
		BeforeDelete: func(context.Context) error {
			barriers++
			if applied == 0 {
				t.Fatal("barrier preceded replay")
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatal("WAL removed before the barrier")
			}
			return sentinel
		},
	}
	recovery := NewRecovery(w.config.WALDir, zerolog.Nop())
	if _, err := recovery.RecoverWithOptions(context.Background(), nil, options); !errors.Is(err, sentinel) {
		t.Fatalf("missing barrier failure: %v", err)
	}
	if barriers != 1 {
		t.Fatalf("barriers=%d", barriers)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("failed shared barrier discarded received WAL")
	}
	options.BeforeDelete = func(context.Context) error { return nil }
	if _, err := recovery.RecoverWithOptions(context.Background(), nil, options); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("successful shared barrier did not reclaim WAL")
	}
}

func TestHandoffRecoveryPreservesWholeRowIdentity(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint(partial), func(t *testing.T) {
			w := newReceivedTestWriter(t)
			rows := []map[string]interface{}{{"m": "cpu", "time": int64(1700000000000000), "v": 1}, {"m": "cpu", "time": int64(1700000000000001), "v": 2}}
			identities, err := w.AppendTracked(rows)
			if err != nil {
				t.Fatal(err)
			}
			if partial {
				if err := w.MarkFlushed([]string{recoveryRowIdentity(identities[0], 0, 1)}); err != nil {
					t.Fatal(err)
				}
			}
			path := w.CurrentFile()
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			called := false
			_, err = NewRecovery(w.config.WALDir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
				BatchSize: 1, PreserveWholeTrackedRows: true, RequireOriginIdentity: true,
				TrackedRowCallback: func(_ context.Context, batch []map[string]interface{}, identity string) error {
					called = true
					if identity != identities[0] || len(batch) != len(rows) {
						t.Fatal("handoff replay split originating identity")
					}
					return nil
				},
				BeforeDelete: func(context.Context) error { return nil },
			})
			if partial {
				if err == nil || called {
					t.Fatal("partial old checkpoints must not be published as complete coverage")
				}
				if _, err := os.Stat(path); err != nil {
					t.Fatal("migration refusal removed WAL")
				}
			} else if err != nil || !called {
				t.Fatalf("whole entry recovery: called=%v err=%v", called, err)
			}
		})
	}
}
