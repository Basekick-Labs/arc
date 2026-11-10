package cluster

import (
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/filereplication"
)

// cluster.replication_pull_bytes_per_second caps the puller only (#397). The
// coordinator's own fetcher — the staged compaction commit and the claimant's
// probes — stays uncapped: a staged fetch always starts from byte zero and the
// origin ends every body at replication_serve_timeout_ms, so a capped one could
// never deliver an output above serve timeout x cap.
func TestCoordinatorFetcherIsNotBandwidthCapped(t *testing.T) {
	c, _ := newShutdownRig(t, allocFreePort(t), 1000)
	c.mu.Lock()
	c.puller.Stop()
	c.puller = nil
	c.cfg.ReplicationPullBytesPerSecond = 1 << 20
	err := c.startFilePullerLocked()
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("startFilePullerLocked: %v", err)
	}
	defer func() {
		if err := c.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	fc, ok := c.fetcher.(*filereplication.FetchClient)
	if !ok {
		t.Fatalf("c.fetcher is %T, want *filereplication.FetchClient", c.fetcher)
	}
	if fc.BytesPerSecond != 0 {
		t.Fatalf("coordinator fetcher BytesPerSecond = %d, want 0 (uncapped)", fc.BytesPerSecond)
	}
}
