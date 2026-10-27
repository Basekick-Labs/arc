package cluster

import (
	"errors"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

func TestStandbyStoreCap(t *testing.T) {
	prev := diskSpace
	t.Cleanup(func() { diskSpace = prev })
	c := &Coordinator{cfg: &config.ClusterConfig{}, logger: zerolog.Nop()}
	diskSpace = func(string) (uint64, uint64, error) { return 20 << 30, 10 << 30, nil }
	if got := c.standbyStoreCap("/x"); got != 5<<30 {
		t.Fatalf("auto on 20 GiB = %d, want 5 GiB", got)
	}
	diskSpace = func(string) (uint64, uint64, error) { return 2 << 30, 1 << 30, nil }
	if got := c.standbyStoreCap("/x"); got != 1<<30 {
		t.Fatalf("auto on 2 GiB = %d, want the 1 GiB floor", got)
	}
	diskSpace = func(string) (uint64, uint64, error) { return 0, 0, errors.New("no statfs") }
	if got := c.standbyStoreCap("/x"); got != 1<<30 {
		t.Fatalf("no statfs = %d, want 1 GiB", got)
	}
	c.cfg.StandbyStoreMaxBytes = 123
	if got := c.standbyStoreCap("/x"); got != 123 {
		t.Fatalf("explicit = %d", got)
	}
	c.cfg.StandbyStoreMaxBytes = -1
	if got := c.standbyStoreCap("/x"); got != 0 {
		t.Fatalf("negative = %d, want 0 (no cap)", got)
	}
}

func TestLowSpaceHysteresis(t *testing.T) {
	total := uint64(100) << 30 // reserve = 10 GiB
	if !lowSpaceNext(false, total, 9<<30) {
		t.Fatal("not low below the reserve")
	}
	if !lowSpaceNext(true, total, 10<<30+(512<<20)) {
		t.Fatal("left low before reserve + 1 GiB")
	}
	if lowSpaceNext(true, total, 11<<30) {
		t.Fatal("still low at reserve + 1 GiB")
	}
	if got := standbyStoreReserve(4 << 30); got != 2<<30 {
		t.Fatalf("reserve on 4 GiB = %d, want the 2 GiB floor", got)
	}
}

// The auto cap has a ceiling: a store at its cap is read whole at startup.
func TestStandbyStoreAutoCapCeiling(t *testing.T) {
	prev := diskSpace
	t.Cleanup(func() { diskSpace = prev })
	c := &Coordinator{cfg: &config.ClusterConfig{}, logger: zerolog.Nop()}
	diskSpace = func(string) (uint64, uint64, error) { return 1400 << 30, 1000 << 30, nil }
	if got := c.standbyStoreCap("/x"); got != standbyStoreMaxAutoCap {
		t.Fatalf("auto on 1.4 TiB = %d, want the %d ceiling", got, standbyStoreMaxAutoCap)
	}
}
