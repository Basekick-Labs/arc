package wal

import (
	"encoding/binary"
	"errors"
	"os"
	"testing"
)

// ScanCovered yields only the matching covered entries, decoded, in file
// order; a torn tail ends the scan; fn's error stops it.
func TestScanCovered(t *testing.T) {
	writer, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	defer writer.Close()
	for cseq := uint64(1); cseq <= 4; cseq++ {
		key := "db1/cpu"
		if cseq == 3 {
			key = "db1/mem"
		}
		p, err := writer.PrepareCovered("db1", key, coveredTestPayload(t, int(cseq)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Commit(cseq); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Append([]map[string]interface{}{{"m": "x", "time": int64(1)}}); err != nil {
		t.Fatal(err)
	}
	path := writer.CurrentFile()
	if err := writer.Rotate(); err != nil {
		t.Fatal(err)
	}
	// A torn tail: half an entry header.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0, 0, 0})
	f.Close()

	var got []uint64
	r := NewReader(path, writer.logger)
	err = r.ScanCovered(func(c Coverage) bool { return c.Key == "db1/cpu" && c.CSeq != 2 }, func(e *Entry) error {
		if e.Coverage == nil || e.ColumnarData == nil || len(e.ColumnarData.Columns["value"]) != int(e.Coverage.CSeq) {
			t.Fatalf("entry %+v", e)
		}
		got = append(got, e.Coverage.CSeq)
		return nil
	})
	if err != nil || len(got) != 2 || got[0] != 1 || got[1] != 4 {
		t.Fatalf("got %v, err %v", got, err)
	}
	stop := errors.New("stop")
	n := 0
	err = NewReader(path, writer.logger).ScanCovered(func(Coverage) bool { return true }, func(*Entry) error {
		n++
		return stop
	})
	if !errors.Is(err, stop) || n != 1 {
		t.Fatalf("stop: n=%d err=%v", n, err)
	}
}

// A damaged entry read whole is skipped; a length that cannot be trusted
// ends the scan with an error.
func TestScanCoveredDamage(t *testing.T) {
	writer, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	defer writer.Close()
	for cseq := uint64(1); cseq <= 2; cseq++ {
		p, err := writer.PrepareCovered("db1", "db1/cpu", coveredTestPayload(t, 2))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Commit(cseq); err != nil {
			t.Fatal(err)
		}
	}
	path := writer.CurrentFile()
	if err := writer.Rotate(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip the last payload byte of the first entry.
	first := WALFileHeaderSize + WALEntryHeaderSize + int(binary.BigEndian.Uint32(raw[WALFileHeaderSize:]))
	bad := append([]byte(nil), raw...)
	bad[first-1] ^= 0xff
	if err := os.WriteFile(path, bad, 0o600); err != nil {
		t.Fatal(err)
	}
	var got []uint64
	r := NewReader(path, writer.logger)
	if err := r.ScanCovered(func(Coverage) bool { return true }, func(e *Entry) error {
		got = append(got, e.Coverage.CSeq)
		return nil
	}); err != nil || len(got) != 1 || got[0] != 2 || r.CorruptedEntries != 1 {
		t.Fatalf("got %v err %v corrupted %d", got, err, r.CorruptedEntries)
	}
	// An impossible length on the first entry.
	huge := append([]byte(nil), raw...)
	binary.BigEndian.PutUint32(huge[WALFileHeaderSize:], MaxWALPayloadSize+1)
	if err := os.WriteFile(path, huge, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := NewReader(path, writer.logger).ScanCovered(func(Coverage) bool { return true }, func(*Entry) error { return nil }); err == nil {
		t.Fatal("scan went on past an untrusted length")
	}
}

// An entry without coverage is skipped, not counted as damaged.
func TestScanCoveredSkipsUncoveredEntries(t *testing.T) {
	writer, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	defer writer.Close()
	if err := writer.Append([]map[string]interface{}{{"m": "x", "time": int64(1)}}); err != nil {
		t.Fatal(err)
	}
	path := writer.CurrentFile()
	if err := writer.Rotate(); err != nil {
		t.Fatal(err)
	}
	r := NewReader(path, writer.logger)
	if err := r.ScanCovered(func(Coverage) bool { return true }, func(*Entry) error { return nil }); err != nil || r.CorruptedEntries != 0 {
		t.Fatalf("err %v corrupted %d", err, r.CorruptedEntries)
	}
}
