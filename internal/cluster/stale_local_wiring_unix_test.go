//go:build unix

package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// The coordinator's real puller, started as a node starts it (#1182): the
// startup walk waits for the journal to open, then hashes a copy this node
// has no record of and records what it holds. The journal is a FIFO so its
// open blocks until the test lets it finish.
func TestStartupWalkWaitsForTheJournalAndRecordsTheHash(t *testing.T) {
	r := staleRig(t)
	r.c.stale.seen.Load().close()
	r.c.stale.seen.Store(nil)
	r.c.cfg.RaftDataDir = t.TempDir()
	fifo := filepath.Join(r.c.cfg.RaftDataDir, seenLocalJournal)
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}

	key := staleHour + "a.parquet"
	body := "rows a"
	r.write(key, body)
	sum := sha256.Sum256([]byte(body))
	e := raft.FileEntry{Path: key, SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(body)), Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	r.c.mu.Lock()
	err := r.c.startFilePullerLocked()
	puller, deleteStop, deleteWg := r.c.puller, r.c.deleteStop, r.c.deleteWg
	r.c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		w, err := os.OpenFile(fifo, os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.WriteString(seenLocalHeader + r.c.cfg.ClusterName + "\n")
		_ = w.Close()
	}
	t.Cleanup(func() {
		release()
		r.c.cancel()
		puller.Stop()
		close(deleteStop)
		deleteWg.Wait()
		r.c.stopStaleLocal()
	})

	// The walk has had time to reach the copy; it must still be waiting.
	time.Sleep(300 * time.Millisecond)
	if s := puller.Stats(); s["catchup_entries_walked"] != 0 || s["refresh_verified"] != 0 {
		t.Fatalf("the walk ran before the journal opened: %v", s)
	}

	release()
	waitFSM(t, func() bool {
		seen := r.c.stale.seen.Load()
		if seen == nil {
			return false
		}
		rec, ok := seen.get(key)
		return ok && rec.sha == shaPrefix(e.SHA256)
	})
	// The hash is recorded inside the verify, before the worker counts it
	// and settles the request: wait for both rather than read them once.
	deadline := time.Now().Add(10 * time.Second)
	for {
		s := puller.Stats()
		if s["refresh_verified"] == 1 && s["listed_unvouched"] == 1 && puller.FullyCaughtUp() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stats %v, gate %v: want one unvouched copy hashed and verified, and the gate green", s, puller.CatchUpStatus())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if s := puller.Stats(); s["refresh_verified"] != 1 || s["listed_unvouched"] != 1 {
		t.Fatalf("stats moved past one verified copy: %v", s)
	}
}
