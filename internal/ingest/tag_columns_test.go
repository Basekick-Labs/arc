package ingest

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/rs/zerolog"
)

func TestWritableTagColumns(t *testing.T) {
	columns := map[string]interface{}{
		"time": []int64{1}, "host": []string{"h"}, "region": []string{"r"},
		"_x": []string{"x"}, "a,b": []string{"y"}, "service.name": []string{"s"}, "n": []int64{1},
	}
	kept, dropped := writableTagColumns(columns, []string{
		"region", "host", "missing", "a,b", "_x", "time", "TIME", "", "host", "service.name", "n",
	})
	// Order is kept; a dotted name and a numeric column stay (compaction
	// decides about those, as before #1258).
	if want := []string{"region", "host", "service.name", "n"}; !reflect.DeepEqual(kept, want) {
		t.Errorf("kept = %v, want %v", kept, want)
	}
	if want := []string{"missing", "a,b", "_x", "time", "TIME", "", "host"}; !reflect.DeepEqual(dropped, want) {
		t.Errorf("dropped = %v, want %v", dropped, want)
	}
	if kept, dropped := writableTagColumns(columns, nil); kept != nil || dropped != nil {
		t.Errorf("no tags: kept=%v dropped=%v", kept, dropped)
	}
}

// fileTagMetadata flushes the buffer and returns the arc:tags value of every
// Parquet file it wrote ("<none>" when a file has no tag metadata).
func fileTagMetadata(t *testing.T, buf *ArrowBuffer, dir string) []string {
	t.Helper()
	if err := buf.FlushAllAndWait(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	paths := parquetPaths(t, dir)
	if len(paths) == 0 {
		t.Fatal("flush wrote no Parquet files")
	}
	var out []string
	for _, p := range paths {
		f, err := os.Open(filepath.Join(dir, p))
		if err != nil {
			t.Fatal(err)
		}
		rdr, err := file.NewParquetReader(f)
		if err != nil {
			f.Close()
			t.Fatalf("open %s: %v", p, err)
		}
		v := "<none>"
		if s := rdr.MetaData().KeyValueMetadata().FindValue("arc:tags"); s != nil {
			v = *s
		}
		rdr.Close()
		out = append(out, v)
	}
	return out
}

// Client tag names reach a file's arc:tags only when compaction can bind them
// (#1258). Every write path ends in the same flush, so the MessagePack
// decoders and line protocol are each driven through it, on a batch inside
// one hour and on one spanning two hours (the two places a file is written).
func TestFlushWritesOnlyUsableTagNames(t *testing.T) {
	const hour = int64(3_600_000_000)
	base := int64(1_700_000_000_000_000) / hour * hour
	for _, span := range []struct {
		name  string
		times []interface{}
		files int
	}{
		{"one_hour", []interface{}{base, base + 1}, 1},
		{"two_hours", []interface{}{base, base + hour}, 2},
	} {
		for _, typed := range []bool{false, true} {
			name := span.name + "/generic"
			if typed {
				name = span.name + "/typed"
			}
			t.Run("msgpack/"+name, func(t *testing.T) {
				dir := t.TempDir()
				buf := newReplayTestBuffer(t, dir)
				t.Cleanup(func() { _ = buf.Close() })
				payload, err := msgpack.Marshal(map[string]interface{}{
					"m":        "cpu",
					"tag_keys": []string{"host", "host_typo", "a,b", "time", "host", ""},
					"columns": map[string]interface{}{
						"time":  span.times,
						"host":  []interface{}{"h1", "h1"},
						"value": []interface{}{1.0, 2.0},
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				dec := NewMessagePackDecoder(zerolog.Nop())
				dec.SetTypedDecodeEnabled(typed)
				decoded, err := dec.Decode(payload)
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				if err := buf.Write(context.Background(), "db", decoded); err != nil {
					t.Fatalf("write: %v", err)
				}
				got := fileTagMetadata(t, buf, dir)
				if len(got) != span.files {
					t.Fatalf("files = %d, want %d", len(got), span.files)
				}
				for _, v := range got {
					if v != "host" {
						t.Errorf("arc:tags = %q, want %q", v, "host")
					}
				}
			})
		}
	}

	// Line protocol has fed arc:tags since before tag_keys: a tag key starting
	// with '_' names a column the file leaves out, and an escaped comma makes
	// one name that arc:tags splits in two.
	t.Run("line_protocol", func(t *testing.T) {
		dir := t.TempDir()
		buf := newReplayTestBuffer(t, dir)
		t.Cleanup(func() { _ = buf.Close() })
		records := NewLineProtocolParser().ParseBatch([]byte(
			"cpu,_x=1,a\\,b=2,host=h1 value=1 1700000000000000000\n" +
				"cpu,_x=1,a\\,b=2,host=h1 value=2 1700000001000000000\n"))
		if len(records) != 2 {
			t.Fatalf("parsed %d records", len(records))
		}
		for _, rec := range BatchToColumnar(records) {
			tags := append([]string(nil), rec.TagColumns...)
			sort.Strings(tags)
			if !reflect.DeepEqual(tags, []string{"_x", "a,b", "host"}) {
				t.Fatalf("fixture no longer carries the bad names: %v", rec.TagColumns)
			}
			if err := buf.WriteColumnarRecord(context.Background(), "db", rec); err != nil {
				t.Fatalf("write: %v", err)
			}
		}
		for _, v := range fileTagMetadata(t, buf, dir) {
			if v != "host" {
				t.Errorf("arc:tags = %q, want %q", v, "host")
			}
		}
	})
}
