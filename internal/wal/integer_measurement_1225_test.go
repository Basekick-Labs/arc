package wal

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/rs/zerolog"
)

// #1225: ingest accepts an integer measurement and names it
// "measurement_<n>"; the WAL keeps the payload the client sent. The reader
// used to accept only a string, so every such entry failed its file — even
// when it was already flushed — and the file was quarantined.
func TestIntegerMeasurementReadsBackAsIngestNamesIt(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(&WriterConfig{WALDir: dir, SyncMode: SyncModeFsync, MaxSizeBytes: 100 << 20, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	payload := func(m interface{}) []byte {
		raw, err := msgpack.Marshal(map[string]interface{}{
			"m":       m,
			"columns": map[string]interface{}{"time": []interface{}{int64(1700000000000000)}, "v": []interface{}{int64(1)}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	if err := w.AppendRawWithMeta("db", payload(int8(7))); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendRawWithMetaTracked("db", payload(uint64(1<<40))); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendRawWithMetaTracked("db", payload(int64(-3))); err != nil {
		t.Fatal(err)
	}
	p, err := w.PrepareCovered("db", "db/measurement_300", payload(300))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Commit(1); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendRawWithMeta("db", payload("cpu")); err != nil {
		t.Fatal(err)
	}
	file := w.CurrentFile()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := NewReader(file, zerolog.Nop())
	entries, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if r.CorruptedEntries != 0 {
		t.Fatalf("%d entries unreadable", r.CorruptedEntries)
	}
	var got []string
	for _, e := range entries {
		if e.ColumnarData == nil {
			t.Fatalf("entry read back without columnar data: %+v", e)
		}
		got = append(got, e.ColumnarData.Measurement)
	}
	want := []string{"measurement_7", "measurement_1099511627776", "measurement_-3", "measurement_300", "cpu"}
	if len(got) != len(want) {
		t.Fatalf("measurements %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("measurements %v, want %v", got, want)
		}
	}

	// Recovery replays the file and deletes it, instead of failing it.
	var replayed []string
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(context.Context, []map[string]interface{}) error { return nil },
		&RecoveryOptions{ColumnarCallback: func(_ context.Context, _, m string, _ map[string][]interface{}, _ string) error {
			replayed = append(replayed, m)
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	if stats.CorruptedEntries != 0 || stats.KeptFiles != 0 || stats.QuarantinedFiles != 0 {
		t.Fatalf("recovery stats %+v", stats)
	}
	sort.Strings(replayed)
	if len(replayed) != len(want) {
		t.Fatalf("replayed %v", replayed)
	}
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Fatalf("recovered file still present: %v", err)
	}
	if failed, _ := filepath.Glob(filepath.Join(dir, "*.failed")); len(failed) != 0 {
		t.Fatalf("quarantined: %v", failed)
	}
}

func TestMeasurementName(t *testing.T) {
	for _, c := range []struct {
		in   interface{}
		want string
		ok   bool
	}{
		{"cpu", "cpu", true}, {int(1), "measurement_1", true}, {int8(-1), "measurement_-1", true},
		{int16(2), "measurement_2", true}, {int32(3), "measurement_3", true}, {int64(4), "measurement_4", true},
		{uint(5), "measurement_5", true}, {uint8(6), "measurement_6", true}, {uint16(7), "measurement_7", true},
		{uint32(8), "measurement_8", true}, {uint64(9), "measurement_9", true},
		{uint64(math.MaxUint64), "measurement_18446744073709551615", true}, {int64(math.MinInt64), "measurement_-9223372036854775808", true},
		{1.5, "", false}, {nil, "", false}, {true, "", false},
	} {
		got, ok := MeasurementName(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("MeasurementName(%#v) = %q %v, want %q %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

// Clients usually send small integers in their compact msgpack forms
// (positive and negative fixint), not the sized ones Marshal picks above.
func TestCompactIntegerMeasurementReadsBack(t *testing.T) {
	cols, err := msgpack.Marshal(map[string]interface{}{"time": []interface{}{int64(1700000000000000)}, "v": []interface{}{int64(1)}})
	if err != nil {
		t.Fatal(err)
	}
	raw := func(m byte) []byte {
		b := []byte{0x82, 0xa1, 'm', m, 0xa7}
		b = append(b, "columns"...)
		return append(b, cols...)
	}
	dir := t.TempDir()
	w, err := NewWriter(&WriterConfig{WALDir: dir, SyncMode: SyncModeFsync, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []byte{0x05, 0xfd} {
		if err := w.AppendRawWithMeta("db", raw(m)); err != nil {
			t.Fatal(err)
		}
	}
	file := w.CurrentFile()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := NewReader(file, zerolog.Nop()).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].ColumnarData == nil || entries[1].ColumnarData == nil ||
		entries[0].ColumnarData.Measurement != "measurement_5" || entries[1].ColumnarData.Measurement != "measurement_-3" {
		t.Fatalf("entries %+v", entries)
	}
}

// An oversized payload is split into chunks that copy m; each chunk reads
// back under the same name.
func TestSplitIntegerMeasurementReadsBack(t *testing.T) {
	rows := 2000
	field := make([]interface{}, rows)
	times := make([]interface{}, rows)
	big := make([]byte, 64<<10)
	for i := range field {
		field[i] = string(big)
		times[i] = int64(1700000000000000 + i)
	}
	payload, err := msgpack.Marshal(map[string]interface{}{"m": 42, "columns": map[string]interface{}{"time": times, "f": field}})
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) <= MaxWALPayloadSize {
		t.Fatalf("test bug: payload %d not above the cap", len(payload))
	}
	w, err := NewWriter(&WriterConfig{WALDir: t.TempDir(), SyncMode: SyncModeAsync, MaxSizeBytes: 1 << 30, BufferSize: 100000, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendRawWithMeta("db", payload); err != nil {
		t.Fatal(err)
	}
	file := w.CurrentFile()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r := NewReader(file, zerolog.Nop())
	entries, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 || r.UndecodableEntries != 0 {
		t.Fatalf("%d entries, %d undecodable", len(entries), r.UndecodableEntries)
	}
	total := 0
	for _, e := range entries {
		if e.ColumnarData == nil || e.ColumnarData.Measurement != "measurement_42" {
			t.Fatalf("chunk %+v", e.ColumnarData)
		}
		total += len(e.ColumnarData.Columns["time"])
	}
	if total != rows {
		t.Fatalf("%d rows read back, want %d", total, rows)
	}
}

// An intact entry this build cannot decode no longer fails its file when it
// was already flushed: its identity is still read, so the checkpoint skips
// it. One that was not flushed still counts against the file.
func TestUndecodableEntryFailsItsFileOnlyWhenUnflushed(t *testing.T) {
	for _, flushed := range []bool{true, false} {
		dir := t.TempDir()
		w, err := NewWriter(&WriterConfig{WALDir: dir, SyncMode: SyncModeFsync, Logger: zerolog.Nop()})
		if err != nil {
			t.Fatal(err)
		}
		bad, err := msgpack.Marshal(map[string]interface{}{"m": 1.5, "columns": map[string]interface{}{"v": []interface{}{int64(1)}}})
		if err != nil {
			t.Fatal(err)
		}
		ids, err := w.AppendRawWithMetaTracked("db", bad)
		if err != nil {
			t.Fatal(err)
		}
		if flushed {
			if err := w.MarkFlushed(ids); err != nil {
				t.Fatal(err)
			}
		}
		file := w.CurrentFile()
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(),
			func(context.Context, []map[string]interface{}) error { return nil },
			&RecoveryOptions{ColumnarCallback: func(context.Context, string, string, map[string][]interface{}, string) error { return nil }})
		if err != nil {
			t.Fatal(err)
		}
		_, statErr := os.Stat(file)
		if flushed && (stats.KeptFiles != 0 || !os.IsNotExist(statErr)) {
			t.Fatalf("flushed undecodable entry failed its file: %+v %v", stats, statErr)
		}
		if !flushed && (stats.KeptFiles == 0 || statErr != nil) {
			t.Fatalf("unflushed undecodable entry did not count against its file: %+v %v", stats, statErr)
		}
	}
}
