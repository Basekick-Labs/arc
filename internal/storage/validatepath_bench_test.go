package storage

import (
	"testing"

	"github.com/rs/zerolog"
)

// The path shape every ingest, compaction and query read goes through:
// {database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{file}.parquet
const benchPath = "default/cpu/2026/09/12/07/1757683200000000000-abc123def456.parquet"

func benchBackend(b *testing.B) *LocalBackend {
	b.Helper()
	be, err := NewLocalBackend(b.TempDir(), zerolog.Nop())
	if err != nil {
		b.Fatal(err)
	}
	return be
}

func BenchmarkValidatePath(b *testing.B) {
	be := benchBackend(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := be.validatePath(benchPath); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValidatePathDeep(b *testing.B) {
	be := benchBackend(b)
	const deep = "default/cpu/2026/09/12/07/sub/dir/another/level/deeper/still/1757683200000000000-abc123def456.parquet"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := be.validatePath(deep); err != nil {
			b.Fatal(err)
		}
	}
}
