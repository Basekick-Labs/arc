package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// damageWALs writes files WAL files of three one-row tracked entries each
// (oldest first), closes the writer, and flips one payload byte of the second
// entry of the oldest file. Each row's "v" is file*10+entry.
func damageWALs(t *testing.T, dir string, files int) []string {
	t.Helper()
	w := crashRecoveryWriter(t, dir)
	var paths []string
	for f := 0; f < files; f++ {
		if f > 0 {
			if err := w.Rotate(); err != nil {
				t.Fatal(err)
			}
		}
		paths = append(paths, w.CurrentFile())
		for e := 0; e < 3; e++ {
			if _, err := w.AppendTracked([]map[string]interface{}{{"_measurement": "events", "v": f*10 + e}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	flipSecondEntry(t, paths[0])
	return paths
}

func flipSecondEntry(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	second := WALFileHeaderSize + WALEntryHeaderSize + int(binary.BigEndian.Uint32(data[WALFileHeaderSize:WALFileHeaderSize+4]))
	data[second+WALEntryHeaderSize] ^= 0xff
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

type replayLog struct {
	rows     []int
	barriers int
	// rowsAtBarrier is len(rows) at each barrier.
	rowsAtBarrier []int
}

func (l *replayLog) callback(_ context.Context, records []map[string]interface{}) error {
	for _, r := range records {
		v, _ := r["v"].(int64)
		if v == 0 {
			if i, ok := r["v"].(int8); ok {
				v = int64(i)
			}
		}
		l.rows = append(l.rows, int(v))
	}
	return nil
}

func (l *replayLog) barrier(context.Context) error {
	l.barriers++
	l.rowsAtBarrier = append(l.rowsAtBarrier, len(l.rows))
	return nil
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// #1634: a damaged entry in the oldest file no longer keeps every newer file
// unreplayed for three passes. One pass replays the intact entries of every
// file, quarantines the damaged one after the barrier, and deletes the rest.
func TestDamagedFileIsQuarantinedAndNewerFilesReplayInOnePass(t *testing.T) {
	dir := t.TempDir()
	paths := damageWALs(t, dir, 3)
	before := metrics.Get().GetWALDamagedEntries()
	var log replayLog
	var quarantined []string
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), log.callback, &RecoveryOptions{
		BeforeDelete: log.barrier,
		OnQuarantine: func(path, as string) { quarantined = append(quarantined, filepath.Base(path)) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(log.rows) != 8 { // 3 files x 3 entries, minus the damaged one
		t.Fatalf("replayed %d rows (%v), want 8", len(log.rows), log.rows)
	}
	if stats.QuarantinedFiles != 1 || stats.KeptFiles != 0 || stats.RecoveredFiles != 2 || stats.CorruptedEntries != 1 {
		t.Fatalf("stats %+v", stats)
	}
	if exists(paths[0]) || !exists(paths[0]+".failed") {
		t.Fatal("damaged file not quarantined")
	}
	if exists(paths[1]) || exists(paths[2]) {
		t.Fatal("newer files not deleted after replay")
	}
	if len(quarantined) != 1 || quarantined[0] != filepath.Base(paths[0]) {
		t.Fatalf("OnQuarantine %v", quarantined)
	}
	// The intact entries of the damaged file were fenced before its rename.
	if log.barriers == 0 || log.rowsAtBarrier[0] < 2 {
		t.Fatalf("barriers %d at rows %v", log.barriers, log.rowsAtBarrier)
	}
	if got := metrics.Get().GetWALDamagedEntries() - before; got != 1 {
		t.Fatalf("arc_wal_damaged_entries_total moved by %d, want 1", got)
	}

	// The next pass has nothing left to replay.
	var again replayLog
	stats, err = NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), again.callback, &RecoveryOptions{BeforeDelete: again.barrier})
	if err != nil || len(again.rows) != 0 || stats.KeptFiles != 0 {
		t.Fatalf("second pass: rows %v stats %+v err %v", again.rows, stats, err)
	}
}

// The quarantine waits for the barrier: when it fails, the damaged file stays
// a .wal, unrenamed, and no newer file is deleted.
func TestDamagedFileIsNotQuarantinedWhenTheBarrierFails(t *testing.T) {
	dir := t.TempDir()
	paths := damageWALs(t, dir, 2)
	var log replayLog
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), log.callback, &RecoveryOptions{
		BeforeDelete: func(context.Context) error { return errors.New("storage unavailable") },
	})
	if err == nil {
		t.Fatal("barrier failure not reported")
	}
	if !exists(paths[0]) || exists(paths[0]+".failed") || !exists(paths[1]) {
		t.Fatalf("files changed after a failed barrier: stats %+v", stats)
	}
	if stats.QuarantinedFiles != 0 || stats.KeptFiles != 2 {
		t.Fatalf("stats %+v", stats)
	}
}

// A replay that fails (a transient cause, not bytes on disk) keeps today's
// behaviour: the file is struck and kept, and newer files are left unread so
// the same cause does not strike them too.
func TestCallbackFailureStillStopsThePass(t *testing.T) {
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	first := w.CurrentFile()
	if _, err := w.AppendTracked([]map[string]interface{}{{"_measurement": "events", "v": 1}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	second := w.CurrentFile()
	if _, err := w.AppendTracked([]map[string]interface{}{{"_measurement": "events", "v": 2}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	calls := 0
	stats, _ := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(context.Context, []map[string]interface{}) error { calls++; return errors.New("buffer refused") },
		&RecoveryOptions{})
	if calls != 1 || stats.QuarantinedFiles != 0 || stats.KeptFiles != 2 || !exists(first) || !exists(second) {
		t.Fatalf("calls %d stats %+v", calls, stats)
	}
	if n, _ := readReplayAttempts(second); n != 0 {
		t.Fatalf("newer file struck %d times", n)
	}
}

// A file with damaged frames AND a failed replay is not quarantined at once:
// the replay failure may be transient.
func TestDamageWithCallbackFailureKeepsStrikes(t *testing.T) {
	dir := t.TempDir()
	paths := damageWALs(t, dir, 1)
	stats, _ := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(context.Context, []map[string]interface{}) error { return errors.New("buffer refused") },
		&RecoveryOptions{})
	if stats.QuarantinedFiles != 0 || !exists(paths[0]) {
		t.Fatalf("stats %+v", stats)
	}
}

// An intact entry this build cannot decode keeps the three strikes: a later
// build may decode it (#1225), so it is not quarantined on the first pass.
func TestUndecodableEntryStillTakesThreePasses(t *testing.T) {
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	bad, err := msgpack.Marshal(map[string]interface{}{"m": 1.5, "columns": map[string]interface{}{"v": []interface{}{int64(1)}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendRawWithMetaTracked("db", bad); err != nil {
		t.Fatal(err)
	}
	file := w.CurrentFile()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for pass := 1; pass <= 3; pass++ {
		stats, _ := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(),
			func(context.Context, []map[string]interface{}) error { return nil },
			&RecoveryOptions{ColumnarCallback: func(context.Context, string, string, map[string][]interface{}, string) error { return nil }})
		if want := pass == 3; (stats.QuarantinedFiles == 1) != want {
			t.Fatalf("pass %d: stats %+v", pass, stats)
		}
	}
	if exists(file) || !exists(file+".failed") {
		t.Fatal("not quarantined after three passes")
	}
}

// Damage that is not a checksum mismatch (here a length past the limit, the
// shape a read error or a damaged length field takes) keeps the strikes: it
// may be transient, and the reader cannot resync after it.
func TestNonChecksumDamageKeepsStrikes(t *testing.T) {
	dir := t.TempDir()
	paths := damageWALs(t, dir, 2)
	data, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	// Undo the checksum flip, then break the second frame's length instead.
	flipSecondEntry(t, paths[0])
	data, err = os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	second := WALFileHeaderSize + WALEntryHeaderSize + int(binary.BigEndian.Uint32(data[WALFileHeaderSize:WALFileHeaderSize+4]))
	binary.BigEndian.PutUint32(data[second:second+4], MaxWALPayloadSize+1)
	if err := os.WriteFile(paths[0], data, 0600); err != nil {
		t.Fatal(err)
	}
	var log replayLog
	stats, _ := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), log.callback, &RecoveryOptions{BeforeDelete: log.barrier})
	if stats.QuarantinedFiles != 0 || !exists(paths[0]) || !exists(paths[1]) {
		t.Fatalf("non-checksum damage quarantined at once: stats %+v", stats)
	}
	if n, _ := readReplayAttempts(paths[0]); n != 1 {
		t.Fatalf("strikes %d, want 1", n)
	}
}

// Under the registration hold the damaged file is kept and struck, and no
// later file of the pass is deleted: a later file holds the only checkpoint
// for a batch of the damaged file, which must not be replayed again.
func TestDamagedFileUnderTheHoldKeepsLaterProof(t *testing.T) {
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	first := w.CurrentFile()
	tokens := commitCovered(t, w, "db1/cpu", 1) // flushed before the crash
	commitCovered(t, w, "db1/cpu", 2)           // its frame is damaged below
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	second := w.CurrentFile()
	if err := w.MarkFlushed(tokens); err != nil { // the checkpoint lands in the second file
		t.Fatal(err)
	}
	inst := w.TrackedInstance()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	flipSecondEntry(t, first)

	hold := newFakeHold()
	hold.set(inst, "db1/cpu", 1, true)
	replays := 0
	run := func() *RecoveryStats {
		stats, _ := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
			ColumnarCallback: func(context.Context, string, string, map[string][]interface{}, string) error { replays++; return nil },
			BeforeDelete:     func(context.Context) error { return nil },
			Held:             hold.source(),
		})
		return stats
	}
	stats := run()
	if !exists(first) || !exists(second) {
		t.Fatalf("pass 1 deleted proof: stats %+v", stats)
	}
	replays = 0
	run()
	if replays != 0 {
		t.Fatalf("a flushed batch was replayed again (%d)", replays)
	}
}
