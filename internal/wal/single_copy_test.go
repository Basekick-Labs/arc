package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/rs/zerolog"
)

// referenceTrackedEntry is the tracked-entry construction before the single
// allocation rewrite (envelopePayload -> trackedPayload -> entryData), kept
// here as the golden reference: the new path must produce identical bytes.
func referenceTrackedEntry(instance, seq, ts uint64, dbBytes []byte, withEnvelope bool, payload []byte) (entry, logical []byte) {
	logical = payload
	if withEnvelope {
		logical = make([]byte, 3+len(dbBytes)+len(payload))
		logical[0] = WALEnvelopeMarker
		binary.BigEndian.PutUint16(logical[1:3], uint16(len(dbBytes)))
		copy(logical[3:], dbBytes)
		copy(logical[3+len(dbBytes):], payload)
	}
	tracked := make([]byte, walTrackedHeaderSize+len(logical))
	tracked[0] = WALTrackedMarker
	binary.BigEndian.PutUint64(tracked[1:9], instance)
	binary.BigEndian.PutUint64(tracked[9:17], seq)
	copy(tracked[17:], logical)
	entry = make([]byte, WALEntryHeaderSize+len(tracked))
	binary.BigEndian.PutUint32(entry[0:4], uint32(len(tracked)))
	binary.BigEndian.PutUint64(entry[4:12], ts)
	binary.BigEndian.PutUint32(entry[12:16], crc32.ChecksumIEEE(tracked))
	copy(entry[WALEntryHeaderSize:], tracked)
	return entry, logical
}

// trackedFrames returns every tracked entry frame (header + payload) in a WAL
// file, in write order.
func trackedFrames(t *testing.T, path string) [][]byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read WAL file: %v", err)
	}
	r := bytes.NewReader(raw)
	// Skip the file header if present (the reader does the same).
	if bytes.HasPrefix(raw, WALMagic) {
		if _, err := r.Seek(int64(WALFileHeaderSize), io.SeekStart); err != nil {
			t.Fatalf("seek past file header: %v", err)
		}
	}
	var frames [][]byte
	for {
		var hdr [WALEntryHeaderSize]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break
		}
		n := binary.BigEndian.Uint32(hdr[0:4])
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			t.Fatalf("truncated frame")
		}
		if n > 0 && body[0] == WALTrackedMarker {
			frames = append(frames, append(hdr[:], body...))
		}
	}
	return frames
}

func frameIdentity(frame []byte) (instance, seq, ts uint64) {
	body := frame[WALEntryHeaderSize:]
	return binary.BigEndian.Uint64(body[1:9]), binary.BigEndian.Uint64(body[9:17]), binary.BigEndian.Uint64(frame[4:12])
}

// TestTrackedEntriesMatchReferenceBytes writes tracked entries through every
// caller of appendTrackedEntry, with and without a replication hook, and
// requires each frame on disk to be byte-identical to the pre-rewrite
// construction, and each hook payload to equal the old logical payload.
func TestTrackedEntriesMatchReferenceBytes(t *testing.T) {
	type write struct {
		name     string
		db       string // "" with envelope=false means no envelope
		envelope bool
		payload  []byte
		call     func(w *Writer) ([]string, error)
	}
	mp := func(n int) []byte {
		recs := make([]map[string]interface{}, n)
		for i := range recs {
			recs[i] = map[string]interface{}{"measurement": "cpu", "host": "h" + strconv.Itoa(i), "value": float64(i)}
		}
		b, err := msgpack.Marshal(recs)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		return b
	}
	db255 := strings.Repeat("d", 255)
	small, large := mp(1), mp(5000)

	for _, withHook := range []bool{false, true} {
		t.Run("hook="+strconv.FormatBool(withHook), func(t *testing.T) {
			writer, dir := newTestWriter(t, SyncModeFsync)
			defer os.RemoveAll(dir)

			var mu sync.Mutex
			var hooked [][]byte
			if withHook {
				writer.SetReplicationHook(func(e *ReplicationEntry) {
					mu.Lock()
					hooked = append(hooked, append([]byte(nil), e.Payload...))
					mu.Unlock()
				})
			}

			// One key per record: Go map iteration order would otherwise make
			// AppendTracked's encoding and this reference encoding differ.
			records := []map[string]interface{}{{"value": int64(42)}}
			recordsPayload, err := msgpack.Marshal(records)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			writes := []write{
				{name: "meta/db", db: "iot", envelope: true, payload: large,
					call: func(w *Writer) ([]string, error) { return w.AppendRawWithMetaTracked("iot", large) }},
				{name: "meta/empty-db", db: "", envelope: true, payload: small,
					call: func(w *Writer) ([]string, error) { return w.AppendRawWithMetaTracked("", small) }},
				{name: "meta/255-byte-db", db: db255, envelope: true, payload: small,
					call: func(w *Writer) ([]string, error) { return w.AppendRawWithMetaTracked(db255, small) }},
				{name: "raw/no-envelope", envelope: false, payload: large,
					call: func(w *Writer) ([]string, error) { return w.AppendRawTracked(large) }},
				{name: "records/no-envelope", envelope: false, payload: recordsPayload,
					call: func(w *Writer) ([]string, error) {
						return w.AppendTracked(records)
					}},
			}
			for _, wr := range writes {
				if hashes, err := wr.call(writer); err != nil || len(hashes) != 1 {
					t.Fatalf("%s: hashes=%v err=%v", wr.name, hashes, err)
				}
			}
			// The chunk loop reuses one envelope header across chunks: two
			// entries from the same header must both be intact.
			var envBuf [3 + 255]byte
			env := envBuf[:putEnvelopeHeader(envBuf[:], []byte("iot"))]
			for i := 0; i < 2; i++ {
				if _, err := writer.appendTrackedEntry(env, small); err != nil {
					t.Fatalf("chunk %d: %v", i, err)
				}
				writes = append(writes, write{name: "chunk-" + strconv.Itoa(i), db: "iot", envelope: true, payload: small})
			}

			path := writer.CurrentFile()
			if err := writer.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			frames := trackedFrames(t, path)
			if len(frames) != len(writes) {
				t.Fatalf("got %d tracked frames, want %d", len(frames), len(writes))
			}
			for i, wr := range writes {
				instance, seq, ts := frameIdentity(frames[i])
				want, logical := referenceTrackedEntry(instance, seq, ts, []byte(wr.db), wr.envelope, wr.payload)
				if !bytes.Equal(frames[i], want) {
					t.Fatalf("%s: frame differs from reference construction (len %d vs %d)", wr.name, len(frames[i]), len(want))
				}
				if withHook && !bytes.Equal(hooked[i], logical) {
					t.Fatalf("%s: hook payload differs from reference logical payload", wr.name)
				}
			}
			if !withHook && len(hooked) != 0 {
				t.Fatalf("hook fired without being set")
			}

			// And the reader still decodes every entry, envelope included.
			entries, err := NewReader(path, zerolog.Nop()).ReadAll()
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if len(entries) != len(writes) {
				t.Fatalf("ReadAll returned %d entries, want %d", len(entries), len(writes))
			}
		})
	}
}

// BenchmarkAppendRawWithMetaTracked reports allocations for the ingest WAL
// path: one payload-sized allocation per entry after the rewrite (was three).
func BenchmarkAppendRawWithMetaTracked(b *testing.B) {
	tmp := b.TempDir()
	w, err := NewWriter(&WriterConfig{WALDir: tmp, SyncMode: SyncModeAsync, MaxSizeBytes: 1 << 30,
		SyncBytes: 1 << 20, BufferSize: 1 << 16, Logger: zerolog.Nop()})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	payload := bytes.Repeat([]byte{0xa5}, 1<<20)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hashes, err := w.AppendRawWithMetaTracked("iot", payload)
		if err != nil {
			b.Fatal(err)
		}
		_ = w.MarkFlushed(hashes)
	}
}
