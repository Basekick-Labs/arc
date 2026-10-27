package ingest

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/stretchr/testify/require"
)

// libraryValueEnd is where the msgpack library's own Skip says the first value
// ends (the technique the WAL uses).
func libraryValueEnd(data []byte) (int, bool) {
	r := bytes.NewReader(data)
	if err := msgpack.NewDecoder(r).Skip(); err != nil {
		return 0, false
	}
	return len(data) - r.Len(), true
}

func walkerSeeds(t testing.TB) [][]byte {
	items := telegrafPayload()
	return [][]byte{
		mustMarshal(t, items),
		mustMarshal(t, map[string]interface{}{"batch": items}),
		mustMarshal(t, map[string]interface{}{"b": []byte{1, 2, 3}, "s": string(make([]byte, 300)), "f": 1.5, "f32": float32(2), "u": uint64(1 << 40), "i": int64(-1 << 40), "n": nil, "t": true}),
		mustMarshal(t, make([]interface{}, 70000)), // array32
		{0xd4, 0x01, 0x02},                         // fixext1
		{0xc7, 0x02, 0x05, 0xaa, 0xbb},             // ext8
	}
}

// On any input the walker agrees with the library's Skip where both accept,
// and never accepts a value the library cannot read.
func FuzzMsgpackValueEnd(f *testing.F) {
	for _, s := range walkerSeeds(f) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		end, ok := msgpackValueEnd(data, 0)
		if !ok {
			return
		}
		libEnd, libOK := libraryValueEnd(data)
		require.True(t, libOK, "walker accepted % x, library cannot skip it", data)
		require.Equal(t, libEnd, end, "% x", data)
	})
}

func TestMsgpackValueEndMatchesLibrary(t *testing.T) {
	for _, s := range walkerSeeds(t) {
		end, ok := msgpackValueEnd(s, 0)
		require.True(t, ok, "% x", s[:min(len(s), 16)])
		libEnd, libOK := libraryValueEnd(s)
		require.True(t, libOK)
		require.Equal(t, libEnd, end)
	}
}

// A few bytes declaring a huge bin, str, array or map made the generic decode
// allocate the declared size (2 GB for c6 7f ff ff ff); they are refused
// before anything is decoded.
func TestDecodeRefusesLengthsPastTheEnd(t *testing.T) {
	for _, in := range [][]byte{
		{0xc6, 0x7f, 0xff, 0xff, 0xff},                  // bin32 2 GB
		{0xdb, 0x7f, 0xff, 0xff, 0xff},                  // str32 2 GB
		{0xdd, 0x7f, 0xff, 0xff, 0xff},                  // array32
		{0xdf, 0x0f, 0xff, 0xff, 0xff},                  // map32
		{0x91, 0xc6, 0x7f, 0xff, 0xff, 0xff},            // inside an array
		{0x81, 0xa1, 'm', 0xc9, 0x7f, 0xff, 0xff, 0xff}, // ext32 in a map
		[]byte("ƨ,\xcf(Q\x93\xfe"),
		{0xc1},
	} {
		for _, typed := range []bool{false, true} {
			var a, b runtime.MemStats
			runtime.ReadMemStats(&a)
			_, err := newTypedTestDecoder(typed).Decode(in)
			runtime.ReadMemStats(&b)
			require.Error(t, err, "% x", in)
			require.Less(t, b.TotalAlloc-a.TotalAlloc, uint64(1<<20), "% x allocated %d bytes", in, b.TotalAlloc-a.TotalAlloc)
		}
	}
}
