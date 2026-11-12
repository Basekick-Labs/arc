package governance

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// The query path once the cluster owns the policies (#1627): Admit reads the
// cluster's view, built by the production SyncClusterView from a real
// ClusterFSM. Compared with BenchmarkAdmit's standalone variants on main.

const seededTokens = 64

// seededManager: a cluster-mode manager over a govFSM with seededTokens
// cluster tokens, each with benchPolicy's limits, wired as UseGovernance does.
func seededManager(b *testing.B) (*Manager, *govFSM, []int64, []string) {
	b.Helper()
	g := newGovFSM(b)
	ids := make([]int64, seededTokens)
	fps := make([]string, seededTokens)
	var entries []raft.GovernancePolicyEntry
	for i := range ids {
		ids[i], fps[i] = g.token(fmt.Sprintf("t%d", i))
		p := benchPolicy(ids[i])
		entries = append(entries, raft.GovernancePolicyEntry{TokenID: ids[i], RateLimitPerMinute: p.RateLimitPerMinute,
			RateLimitPerHour: p.RateLimitPerHour, MaxQueriesPerHour: p.MaxQueriesPerHour, MaxQueriesPerDay: p.MaxQueriesPerDay,
			MaxRowsPerQuery: p.MaxRowsPerQuery, MaxScanDurationSec: p.MaxScanDurationSec})
	}
	g.seed(entries...)
	m := clusterManager(b, openDB(b), benchDefaults())
	wire(m, g)
	return m, g, ids, fps
}

func BenchmarkAdmitSeeded(b *testing.B) {
	for _, c := range []struct {
		name string
		fp   func(fps []string) string
		rows int
	}{
		{"policy", func(fps []string) string { return fps[0] }, 1000},
		{"defaults", nil, 1000},
		{"fp-mismatch", func([]string) string { return "a pre-cluster token at the same ID" }, 1000},
	} {
		b.Run(c.name, func(b *testing.B) {
			m, _, ids, fps := seededManager(b)
			id, fp := ids[0], ""
			if c.fp != nil {
				fp = c.fp(fps)
			} else {
				id = ids[len(ids)-1] + 1000 // no policy: the defaults
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if r := m.AdmitToken(id, fp); !r.Allowed || r.MaxRows != c.rows {
					b.Fatalf("%+v", r)
				}
			}
		})
	}
}

// BenchmarkAdmitParallelSeeded is BenchmarkAdmitParallel with the policies in
// the cluster state: every 1000 file registrations a governance Set swaps
// the view on the apply goroutine, as an admin's change lands among ingest
// applies.
func BenchmarkAdmitParallelSeeded(b *testing.B) {
	b.Run("fsm-apply-load", func(b *testing.B) {
		m, g, ids, fps := seededManager(b)
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
				if res := g.apply(raft.CommandRegisterFile, raft.RegisterFilePayload{File: raft.FileEntry{
					Path: path, SHA256: "x", SizeBytes: 1, Database: "db", Measurement: "cpu",
					PartitionTime: ts, OriginNodeID: "w1", Tier: "hot", CreatedAt: base,
				}}); res != nil {
					b.Error(res)
					return
				}
				applies.Add(1)
				if i%1000 == 999 {
					p := benchPolicy(ids[0])
					if res := g.apply(raft.CommandSetGovernancePolicy, raft.SetGovernancePolicyPayload{TokenID: ids[0],
						RateLimitPerMinute: p.RateLimitPerMinute, RateLimitPerHour: p.RateLimitPerHour,
						MaxQueriesPerHour: p.MaxQueriesPerHour, MaxQueriesPerDay: p.MaxQueriesPerDay,
						MaxRowsPerQuery: p.MaxRowsPerQuery, MaxScanDurationSec: p.MaxScanDurationSec}); res != nil {
						b.Error(res)
						return
					}
				}
			}
		}()
		var next atomic.Int64
		start := time.Now()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			k := next.Add(1) % seededTokens
			for pb.Next() {
				if r := m.AdmitToken(ids[k], fps[k]); !r.Allowed {
					b.Errorf("%+v", r)
					return
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
