package main

// #1206: the registrar that announces files flushed before the cluster is up
// must be on the ArrowBuffer before anything can flush. These pin the order in
// main.go itself, so moving the call back below startup recovery fails here.

import (
	"os"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

func TestEarlyFileRegistrarIsAttachedBeforeAnythingFlushes(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	pos := func(needle string) int {
		t.Helper()
		i := strings.Index(s, needle)
		if i < 0 {
			t.Fatalf("main.go no longer contains %q", needle)
		}
		return i
	}
	attach := pos("arrowBuffer.SetFileRegistrar(earlyRegistrar)")
	for _, flusher := range []string{
		"walRecovery.RecoverWithOptions(", // startup recovery flushes, then deletes the WAL
		"mqttManager.Start(",              // MQTT ingest
		"cluster.NewCoordinatorFileRegistrar(",
	} {
		if at := pos(flusher); attach > at {
			t.Errorf("the early registrar is attached after %s; files it flushes reach no manifest", flusher)
		}
	}
	handOff := pos("earlyRegistrar.HandOff(")
	if start := pos("clusterCoordinator.Start()"); handOff < start {
		t.Error("the hand-off runs before the cluster coordinator starts")
	}
	if abandon := pos("earlyRegistrar.Abandon()"); abandon < handOff {
		t.Error("Abandon must follow the cluster block that hands off")
	}
}

func TestNewEarlyFileRegistrarOnlyOnANodeThatCanAnnounce(t *testing.T) {
	base := func() *config.Config {
		cfg := &config.Config{}
		cfg.Cluster.Enabled = true
		cfg.Cluster.Role = "writer"
		cfg.Cluster.ClusterName = "c"
		cfg.Cluster.RaftDataDir = t.TempDir()
		return cfg
	}
	cases := []struct {
		name     string
		mutate   func(*config.Config)
		licensed bool
		want     bool
	}{
		{"writer", func(*config.Config) {}, true, true},
		{"cluster disabled", func(c *config.Config) { c.Cluster.Enabled = false }, true, false},
		{"no clustering licence", func(*config.Config) {}, false, false},
		{"no raft manifest", func(c *config.Config) { c.Cluster.RaftDataDir = "" }, true, false},
		{"reader", func(c *config.Config) { c.Cluster.Role = "reader" }, true, false},
		{"compactor", func(c *config.Config) { c.Cluster.Role = "compactor" }, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mutate(cfg)
			r := newEarlyFileRegistrar(cfg, tc.licensed, zerolog.Nop())
			if r != nil {
				r.Stop()
			}
			if got := r != nil; got != tc.want {
				t.Fatalf("built = %v, want %v", got, tc.want)
			}
		})
	}
}
