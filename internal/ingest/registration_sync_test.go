package ingest

// #1209: a flush makes its file registrations durable before it checkpoints
// the WAL. The other order lets the WAL drop rows whose file the cluster has no
// durable record of, and a crash then leaves that file unannounced for good.

import (
	"context"
	"sync"
	"testing"
	"time"
)

type orderLog struct {
	mu     sync.Mutex
	events []string
}

func (l *orderLog) add(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *orderLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

type syncingRegistrar struct{ log *orderLog }

func (r *syncingRegistrar) RegisterFile(_, _, _ string, _ time.Time, _ int64, _ string) {
	r.log.add("register")
}

func (r *syncingRegistrar) SyncRegistrations() error {
	r.log.add("sync")
	return nil
}

type orderedWAL struct {
	checkpointRecordingWAL
	log *orderLog
}

func (w *orderedWAL) MarkFlushed(hashes []string) error {
	w.log.add("checkpoint")
	return w.checkpointRecordingWAL.MarkFlushed(hashes)
}

func checkRegistrationsSyncedBeforeCheckpoint(t *testing.T, events []string) {
	t.Helper()
	sawRegister, sawSync := false, false
	checkpoints := 0
	for _, e := range events {
		switch e {
		case "register":
			sawRegister, sawSync = true, false
		case "sync":
			sawSync = true
		case "checkpoint":
			checkpoints++
			if sawRegister && !sawSync {
				t.Fatalf("WAL checkpoint after a registration with no sync in between: %v", events)
			}
		}
	}
	if !sawRegister || checkpoints == 0 {
		t.Fatalf("flush did not register and checkpoint: %v", events)
	}
}

func TestAsyncFlushSyncsRegistrationsBeforeTheWALCheckpoint(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1) // size 1: every write flushes asynchronously
	log := &orderLog{}
	buffer.SetWAL(&orderedWAL{log: log})
	buffer.SetFileRegistrar(&syncingRegistrar{log: log})
	defer buffer.Close()

	if err := buffer.writeColumnarInternal(context.Background(), "default", checkpointTestRecord(), false, ""); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(log.snapshot()) < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	checkRegistrationsSyncedBeforeCheckpoint(t, log.snapshot())
}

func TestSyncFlushSyncsRegistrationsBeforeTheWALCheckpoint(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000) // nothing flushes until Close
	log := &orderLog{}
	buffer.SetWAL(&orderedWAL{log: log})
	buffer.SetFileRegistrar(&syncingRegistrar{log: log})

	if err := buffer.writeColumnarInternal(context.Background(), "default", checkpointTestRecord(), false, ""); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Close(); err != nil {
		t.Fatal(err)
	}
	checkRegistrationsSyncedBeforeCheckpoint(t, log.snapshot())
}

// Without a WAL nothing is ordered by the sync, so a flush pays no fsync.
func TestFlushWithoutAWALDoesNotSyncRegistrations(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000)
	buffer.wal = nil
	log := &orderLog{}
	buffer.SetFileRegistrar(&syncingRegistrar{log: log})
	if err := buffer.writeColumnarInternal(context.Background(), "default", checkpointTestRecord(), false, ""); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, e := range log.snapshot() {
		if e == "sync" {
			t.Fatalf("synced registrations with no WAL: %v", log.snapshot())
		}
	}
}
