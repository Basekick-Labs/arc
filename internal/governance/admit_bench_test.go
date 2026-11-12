package governance

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	hraft "github.com/hashicorp/raft"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

// The query path's cost of governance (#1627): Admit, which every governed
// query runs once. Run on main and on a branch with -benchmem and compared
// with benchstat. "standalone" is a node whose policies are its own (the
// only mode before #1627); the seeded variants (policies read from the
// cluster state) come with the read path that adds them.

// benchLimit never refuses: the benchmarks measure admission, not refusal.
const benchLimit = 1 << 40

func benchManager(b *testing.B, cfg *config.GovernanceConfig) *Manager {
	b.Helper()
	metrics.Init(zerolog.Nop())
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	m, err := NewManager(&ManagerConfig{DB: db, Config: cfg, Logger: zerolog.Nop()})
	if err != nil {
		b.Fatal(err)
	}
	m.Start()
	b.Cleanup(func() { m.Stop() })
	return m
}

func benchPolicy(tokenID int64) *Policy {
	return &Policy{TokenID: tokenID, RateLimitPerMinute: benchLimit, RateLimitPerHour: benchLimit,
		MaxQueriesPerHour: benchLimit, MaxQueriesPerDay: benchLimit, MaxRowsPerQuery: 1000, MaxScanDurationSec: 30}
}

func benchDefaults() *config.GovernanceConfig {
	return &config.GovernanceConfig{DefaultRateLimitPerMin: benchLimit, DefaultRateLimitPerHour: benchLimit,
		DefaultMaxQueriesPerHour: benchLimit, DefaultMaxQueriesPerDay: benchLimit, DefaultMaxRowsPerQuery: 1000}
}

func BenchmarkAdmit(b *testing.B) {
	b.Run("standalone/policy", func(b *testing.B) {
		m := benchManager(b, &config.GovernanceConfig{})
		if _, err := m.CreatePolicy(context.Background(), benchPolicy(1)); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if r := m.Admit(1); !r.Allowed || r.MaxRows != 1000 {
				b.Fatalf("%+v", r)
			}
		}
	})
	b.Run("standalone/defaults", func(b *testing.B) {
		m := benchManager(b, benchDefaults())
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if r := m.Admit(2); !r.Allowed || r.MaxRows != 1000 {
				b.Fatalf("%+v", r)
			}
		}
	})
}

// BenchmarkAdmitParallel runs Admit from every P while one goroutine applies
// file registrations to a real ClusterFSM continuously, as ingest flushes do,
// and every 1000 of them a governance policy write where the build has one
// (benchGovernanceWriter). It reports the apply rate too: it must not drop.
func BenchmarkAdmitParallel(b *testing.B) {
	b.Run("standalone/fsm-apply-load", func(b *testing.B) {
		m := benchManager(b, benchDefaults())
		const tokens = 64
		for t := int64(1); t <= tokens; t++ {
			if _, err := m.CreatePolicy(context.Background(), benchPolicy(t)); err != nil {
				b.Fatal(err)
			}
		}
		fsm := raft.NewClusterFSM(zerolog.Nop())
		var idx uint64
		apply := func(typ raft.CommandType, p any) (uint64, interface{}) {
			raw, err := json.Marshal(p)
			if err != nil {
				b.Fatal(err)
			}
			data, err := json.Marshal(raft.Command{Type: typ, Payload: raw})
			if err != nil {
				b.Fatal(err)
			}
			idx++
			return idx, fsm.Apply(&hraft.Log{Index: idx, Data: data})
		}
		if _, res := apply(raft.CommandAddNode, raft.AddNodePayload{Node: raft.NodeInfo{ID: "w1", Role: "writer", State: "healthy"}}); res != nil {
			b.Fatal(res)
		}
		govWrite := benchGovernanceWriter(b, apply)

		var applies atomic.Int64
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				ts := base.Add(time.Duration(i%720) * time.Hour)
				path := fmt.Sprintf("db/cpu/%04d/%02d/%02d/%02d/f%d.parquet", ts.Year(), ts.Month(), ts.Day(), ts.Hour(), i)
				if _, res := apply(raft.CommandRegisterFile, raft.RegisterFilePayload{File: raft.FileEntry{
					Path: path, SHA256: "x", SizeBytes: 1, Database: "db", Measurement: "cpu",
					PartitionTime: ts, OriginNodeID: "w1", Tier: "hot", CreatedAt: base,
				}}); res != nil {
					b.Error(res)
					return
				}
				applies.Add(1)
				if i%1000 == 999 {
					govWrite(i)
				}
			}
		}()

		var next atomic.Int64
		b.ReportAllocs()
		start := time.Now()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			tok := next.Add(1)%tokens + 1
			for pb.Next() {
				if r := m.Admit(tok); !r.Allowed {
					b.Fatalf("%+v", r)
				}
			}
		})
		b.StopTimer()
		elapsed := time.Since(start)
		close(stop)
		wg.Wait()
		b.ReportMetric(float64(applies.Load())/elapsed.Seconds(), "applies/s")
	})
}
