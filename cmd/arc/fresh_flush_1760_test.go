package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
)

// #1760: the predicates as implications, over every combination: a handler
// that waits has a buffer whose ledger and tracking are on; the visible-write
// forcer still needs its own key; the ledger is on only when one of them is.
func TestFreshFlushPredicates1760(t *testing.T) {
	for _, visible := range []bool{false, true} {
		for _, fresh := range []bool{false, true} {
			for _, cluster := range []bool{false, true} {
				for _, backend := range []string{"local", "s3"} {
					cfg := &config.Config{}
					cfg.Ingest.WaitVisibleEnabled = visible
					cfg.Query.FreshFlushEnabled = fresh
					cfg.Cluster.Enabled = cluster
					cfg.Storage.Backend = backend
					standaloneLocal := !cluster && backend == "local"
					ledger, tracking, forcer := keyFlushLedgerEnabled(cfg), freshFlushEnabled(cfg), writeForcingEnabled(cfg)
					fc := freshFlushConfig(cfg)
					handlerWaits := fc.Enabled && !fc.Clustered && fc.StorageBackend == "local"
					switch {
					case handlerWaits && !(ledger && tracking):
						t.Errorf("visible=%v fresh=%v cluster=%v %s: the handler waits without ledger %v and tracking %v", visible, fresh, cluster, backend, ledger, tracking)
					case forcer && !ledger:
						t.Errorf("visible=%v fresh=%v cluster=%v %s: a forcer without the ledger", visible, fresh, cluster, backend)
					case forcer != (visible && standaloneLocal):
						t.Errorf("visible=%v fresh=%v cluster=%v %s: forcer %v", visible, fresh, cluster, backend, forcer)
					case ledger != ((visible || fresh) && standaloneLocal):
						t.Errorf("visible=%v fresh=%v cluster=%v %s: ledger %v", visible, fresh, cluster, backend, ledger)
					case tracking != (fresh && standaloneLocal):
						t.Errorf("visible=%v fresh=%v cluster=%v %s: tracking %v", visible, fresh, cluster, backend, tracking)
					}
				}
			}
		}
	}
	cfg := &config.Config{}
	cfg.Query.FreshFlushMinIntervalMS, cfg.Query.FreshFlushMaxWaiters, cfg.WAL.RecoveryIntervalSeconds = 1500, 3, 10
	if fc := freshFlushConfig(cfg); fc.MinInterval != 1500*time.Millisecond || fc.MaxWaiters != 3 || fc.RetryAfter != 10*time.Second {
		t.Fatalf("freshFlushConfig = %+v", fc)
	}

	// Tracking is enabled before startup WAL recovery (a task detached before
	// it is not tracked), the forcer still on its own predicate, and the
	// handler configured.
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	newBuf := strings.Index(s, "arrowBuffer := ingest.NewArrowBuffer(")
	tracking := strings.Index(s, "if freshFlushEnabled(cfg) {\n\t\tarrowBuffer.EnableKeyFlushTracking()")
	recovery := strings.Index(s, "walRecovery.RecoverWithOptions(context.Background(), recoveryCallback,")
	switch {
	case newBuf < 0 || tracking < 0 || recovery < 0:
		t.Fatalf("main.go wiring not found (buffer %d, tracking %d, recovery %d)", newBuf, tracking, recovery)
	case !(newBuf < tracking && tracking < recovery):
		t.Fatal("task tracking must be enabled right after NewArrowBuffer and before startup WAL recovery")
	case strings.Count(s, "EnableKeyFlushLedger(") != 1:
		t.Fatal("the ledger must be enabled exactly once: the first budget wins")
	case !strings.Contains(s, "queryHandler.SetFreshFlush(freshFlushConfig(cfg), arrowBuffer)"):
		t.Fatal("the query handler is not configured for fresh queries")
	}
}
