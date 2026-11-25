package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
)

// #1759 (b): a visible write forces its measurements only where the ledger is
// on, and both come from one predicate: the key on, standalone, local.
func TestWriteForcingEnabled1759(t *testing.T) {
	cases := []struct {
		name    string
		enabled bool
		cluster bool
		backend string
		want    bool
	}{
		{"off", false, false, "local", false},
		{"on, local", true, false, "local", true},
		{"on, cluster", true, true, "local", false},
		{"on, s3", true, false, "s3", false},
		{"on, azure", true, false, "azure", false},
		{"off, cluster", false, true, "local", false},
	}
	for _, tc := range cases {
		cfg := &config.Config{}
		cfg.Ingest.WaitVisibleEnabled = tc.enabled
		cfg.Cluster.Enabled = tc.cluster
		cfg.Storage.Backend = tc.backend
		if got := writeForcingEnabled(cfg); got != tc.want {
			t.Errorf("%s: writeForcingEnabled = %v, want %v", tc.name, got, tc.want)
		}
	}
	cfg := &config.Config{}
	cfg.Ingest.WaitVisibleMinIntervalMS = 1000
	if got := waitVisibleForceInterval(cfg); got != time.Second {
		t.Fatalf("interval = %v, want 1s", got)
	}

	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	enable := strings.Index(s, "if writeForcingEnabled(cfg) {\n\t\tarrowBuffer.EnableKeyFlushLedger(ingest.ForceBudget{})")
	forcer := strings.Index(s, "if writeForcingEnabled(cfg) {\n\t\twaitVisible.SetForcer(arrowBuffer, waitVisibleForceInterval(cfg))")
	newBuf := strings.Index(s, "arrowBuffer := ingest.NewArrowBuffer(")
	recovery := strings.Index(s, "walRecovery.RecoverWithOptions(context.Background(), recoveryCallback,")
	switch {
	case enable < 0 || forcer < 0 || newBuf < 0 || recovery < 0:
		t.Fatalf("main.go wiring not found (enable %d, forcer %d, buffer %d, recovery %d)", enable, forcer, newBuf, recovery)
	case !(newBuf < enable && enable < recovery):
		t.Fatal("the ledger must be enabled right after NewArrowBuffer and before startup WAL recovery, so its limiter sees every detach")
	}
}
