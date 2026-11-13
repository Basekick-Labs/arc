package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// lengthFixture writes n one-row tracked entries, then a checkpoint for the
// first one, into one closed file and returns its path, bytes and the frame
// offsets (data frames first, the checkpoint last).
func lengthFixture(t *testing.T, n int) (string, []byte, []int) {
	t.Helper()
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	var first []string
	for i := 0; i < n; i++ {
		ids, err := w.AppendTracked([]map[string]interface{}{{"_measurement": "events", "v": i}})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = ids
		}
	}
	if err := w.MarkFlushed(first); err != nil {
		t.Fatal(err)
	}
	path := w.CurrentFile()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var offs []int
	for o := WALFileHeaderSize; o+WALEntryHeaderSize <= len(data); {
		offs = append(offs, o)
		o += WALEntryHeaderSize + int(binary.BigEndian.Uint32(data[o:o+4]))
	}
	if len(offs) != n+1 {
		t.Fatalf("%d frames, want %d", len(offs), n+1)
	}
	return path, data, offs
}

func setLength(data []byte, off int, n uint32) []byte {
	out := append([]byte(nil), data...)
	binary.BigEndian.PutUint32(out[off:off+4], n)
	return out
}

func write(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

// readAllValues returns the "v" of every row ReadAll returns, and the reader.
func readAllValues(t *testing.T, path string) ([]int, *Reader) {
	t.Helper()
	r := NewReader(path, zerolog.Nop())
	entries, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var vs []int
	for _, e := range entries {
		for _, rec := range e.Records {
			vs = append(vs, rowValue(rec))
		}
	}
	return vs, r
}

// #1641: a length pointing past the end of the file is repaired by the
// payload checksum, and every frame after it is read (and recovered), where
// it used to read as a torn tail and the file was deleted without them.
func TestLengthPastEOFIsRepairedAndLaterFramesRecovered(t *testing.T) {
	path, data, offs := lengthFixture(t, 3)
	write(t, path, setLength(data, offs[1], uint32(len(data))))

	vs, r := readAllValues(t, path)
	if len(vs) != 3 || r.RepairedLengths != 1 || r.TruncatedEntries != 0 || r.CorruptedEntries != 0 {
		t.Fatalf("values %v repaired %d truncated %d corrupted %d", vs, r.RepairedLengths, r.TruncatedEntries, r.CorruptedEntries)
	}
	// The checkpoint after the damaged frame is seen by the checkpoint scan,
	// and the identities after it by the identity scan.
	cr := NewReader(path, zerolog.Nop())
	hashes, err := cr.ReadCheckpointHashes()
	if err != nil || len(hashes) != 1 || cr.RepairedLengths != 1 {
		t.Fatalf("checkpoints %v err %v repaired %d", hashes, err, cr.RepairedLengths)
	}
	ids, err := NewReader(path, zerolog.Nop()).ReadTrackedIdentities()
	if err != nil || len(ids) != 3 {
		t.Fatalf("identities %v err %v", ids, err)
	}

	before := metrics.Get().GetWALRepairedLengths()
	replayed := 0
	stats, err := NewRecovery(filepath.Dir(path), zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(_ context.Context, rows []map[string]interface{}) error { replayed += len(rows); return nil },
		&RecoveryOptions{BeforeDelete: func(context.Context) error { return nil }})
	if err != nil || replayed != 2 { // the first entry is checkpointed
		t.Fatalf("replayed %d err %v stats %+v", replayed, err, stats)
	}
	if got := metrics.Get().GetWALRepairedLengths() - before; got != 1 {
		t.Fatalf("metric moved by %d", got)
	}
}

// A length damaged to be longer but still inside the file is NOT repaired:
// the checkpoint scan skips data frames by their stated length without
// reading them, so a repair in ReadAll alone would make the two disagree and
// replay an entry whose checkpoint the scan missed. Depending on how the
// misaligned bytes are classified, recovery may keep the damaged file for
// another pass or quarantine it immediately. Either way, it must preserve the
// file and must not replay the flushed entry (review of #1641).
func TestLengthTooLongInsideTheFileIsDamageNotRepair(t *testing.T) {
	path, data, offs := lengthFixture(t, 3)
	trueLen := offs[1] - offs[0] - WALEntryHeaderSize
	bad := setLength(data, offs[0], uint32(trueLen+21))
	write(t, path, bad)
	_, r := readAllValues(t, path)
	if r.RepairedLengths != 0 || r.CorruptedEntries == 0 {
		t.Fatalf("repaired %d corrupted %d", r.RepairedLengths, r.CorruptedEntries)
	}
	var replayed []int
	stats, _ := NewRecovery(filepath.Dir(path), zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(_ context.Context, rows []map[string]interface{}) error {
			for _, row := range rows {
				replayed = append(replayed, rowValue(row))
			}
			return nil
		},
		&RecoveryOptions{BeforeDelete: func(context.Context) error { return nil }})
	for _, v := range replayed {
		if v == 0 {
			t.Fatalf("the checkpointed entry was replayed: %v", replayed)
		}
	}
	kept, pathErr := os.ReadFile(path)
	quarantined, quarantineErr := os.ReadFile(path + ".failed")
	pathExists := pathErr == nil
	quarantineExists := quarantineErr == nil
	if pathErr != nil && !os.IsNotExist(pathErr) {
		t.Fatalf("read damaged WAL file: %v", pathErr)
	}
	if quarantineErr != nil && !os.IsNotExist(quarantineErr) {
		t.Fatalf("read quarantined WAL file: %v", quarantineErr)
	}
	if pathExists == quarantineExists {
		t.Fatalf("damaged file must be kept or quarantined, not deleted or duplicated: path err=%v quarantine err=%v stats=%+v", pathErr, quarantineErr, stats)
	}
	preserved := kept
	if quarantineExists {
		preserved = quarantined
	}
	if !bytes.Equal(preserved, bad) {
		t.Fatal("recovery changed the damaged WAL bytes")
	}
	if (stats.QuarantinedFiles == 1) != quarantineExists || stats.QuarantinedFiles > 1 {
		t.Fatalf("quarantined files=%d, quarantine path exists=%t", stats.QuarantinedFiles, quarantineExists)
	}
}

// A candidate end followed only by a torn tail does not confirm a repair:
// random bytes read as a plausible torn header too often.
func TestRepairFollowedByTornTailIsNotConfirmed(t *testing.T) {
	path, data, offs := lengthFixture(t, 3)
	// Damage frame 1's length past the end, then tear the file inside frame 2.
	bad := setLength(data, offs[1], uint32(len(data)))
	bad = bad[:offs[2]+WALEntryHeaderSize+2]
	write(t, path, bad)
	_, r := readAllValues(t, path)
	if r.RepairedLengths != 0 || r.TruncatedEntries != 1 {
		t.Fatalf("repaired %d truncated %d", r.RepairedLengths, r.TruncatedEntries)
	}
}

// The last frame of the file, its length past the end: confirmed by the end
// of the file.
func TestLengthOfTheLastFrameIsRepaired(t *testing.T) {
	path, data, offs := lengthFixture(t, 2)
	last := offs[len(offs)-1]
	write(t, path, setLength(data, last, uint32(len(data))))
	hashes, err := NewReader(path, zerolog.Nop()).ReadCheckpointHashes()
	if err != nil || len(hashes) != 1 {
		t.Fatalf("checkpoint frame not repaired: %v %v", hashes, err)
	}
}

// A real torn tail is not mistaken for damage.
func TestTornTailIsNotRepaired(t *testing.T) {
	path, data, _ := lengthFixture(t, 3)
	write(t, path, data[:len(data)-4])
	vs, r := readAllValues(t, path)
	if len(vs) != 3 || r.RepairedLengths != 0 || r.TruncatedEntries != 1 {
		t.Fatalf("values %v repaired %d truncated %d", vs, r.RepairedLengths, r.TruncatedEntries)
	}
}

// A damaged length AND a damaged payload: nothing matches the checksum, so
// it reads as today (a torn tail). Documented as not covered.
func TestLengthAndPayloadDamageIsNotRepaired(t *testing.T) {
	path, data, offs := lengthFixture(t, 3)
	bad := setLength(data, offs[1], uint32(len(data)))
	bad[offs[1]+WALEntryHeaderSize+2] ^= 0xff
	write(t, path, bad)
	_, r := readAllValues(t, path)
	if r.RepairedLengths != 0 {
		t.Fatalf("repaired %d", r.RepairedLengths)
	}
}

// A coincidental checksum match that nothing confirms is not a repair: the
// bytes after the candidate end are neither the end of the file nor a frame.
func TestUnconfirmedChecksumMatchIsNotARepair(t *testing.T) {
	path, data, offs := lengthFixture(t, 2)
	// Make frame 1's stored checksum the CRC of its first 5 payload bytes,
	// and its length past the end: a prefix matches, but what follows (the
	// rest of its own payload) is not a frame.
	p := offs[1] + WALEntryHeaderSize
	bad := setLength(data, offs[1], uint32(len(data)))
	binary.BigEndian.PutUint32(bad[offs[1]+12:offs[1]+16], crc32.ChecksumIEEE(bad[p:p+5]))
	write(t, path, bad)
	_, r := readAllValues(t, path)
	if r.RepairedLengths != 0 || r.TruncatedEntries != 1 {
		t.Fatalf("repaired %d truncated %d", r.RepairedLengths, r.TruncatedEntries)
	}
}

// Sixteen zero bytes are not a frame (CRC32 of nothing is 0).
func TestZeroHeaderDoesNotConfirm(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "z")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(make([]byte, 64)); err != nil {
		t.Fatal(err)
	}
	if frameEndConfirmed(f, 64, 8) {
		t.Fatal("a zero header confirmed a frame end")
	}
}

// rowValue is the "v" of a replayed row, whatever integer type msgpack chose.
func rowValue(row map[string]interface{}) int {
	switch v := row["v"].(type) {
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	case uint8:
		return int(v)
	case uint16:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		return int(v)
	}
	return -1
}
