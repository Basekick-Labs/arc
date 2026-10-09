package telemetry

import (
	"encoding/json"
	"reflect"
	"runtime"
	"testing"

	"github.com/basekick-labs/arc/internal/syscpu"
	"github.com/rs/zerolog"
)

// The payload reports the usable count as a THIRD field and leaves the two
// machine fields alone.
//
// The sentinel is what makes this fail against the unfixed code: asserting
// against syscpu.CoresAtStartup() would recompute the production expression,
// and on any runner without a CPU quota that equals runtime.NumCPU() — so it
// would also pass against a payload that never learned about quotas. No
// machine reports 4242.
func TestCollectPayloadReportsUsableCores(t *testing.T) {
	const sentinel = 4242
	original := usableCoresFn
	usableCoresFn = func() int { return sentinel }
	t.Cleanup(func() { usableCoresFn = original })

	c := &Collector{instanceID: "test", version: "test", logger: zerolog.Nop()}
	payload := c.collectPayload()

	if payload.CPU.UsableCores == nil {
		t.Fatal("usable_cores is nil; the field is always set")
	}
	if *payload.CPU.UsableCores != sentinel {
		t.Errorf("usable_cores = %d, want %d from the seam", *payload.CPU.UsableCores, sentinel)
	}

	// The continuity assertion. The fleet series for these two must not step
	// down on upgrade, which is why #1039 added a field instead of redefining
	// them — and this is the test that fails if someone later "simplifies" the
	// three into one.
	if payload.CPU.PhysicalCores == nil || *payload.CPU.PhysicalCores != runtime.NumCPU() {
		t.Errorf("physical_cores = %v, want runtime.NumCPU() = %d", payload.CPU.PhysicalCores, runtime.NumCPU())
	}
	if payload.CPU.LogicalCores == nil || *payload.CPU.LogicalCores != runtime.NumCPU() {
		t.Errorf("logical_cores = %v, want runtime.NumCPU() = %d", payload.CPU.LogicalCores, runtime.NumCPU())
	}
}

// The wire name is what the collector reads, so pin it rather than trusting
// the struct tag by eye.
func TestUsableCoresMarshalsUnderItsWireName(t *testing.T) {
	const sentinel = 4242
	original := usableCoresFn
	usableCoresFn = func() int { return sentinel }
	t.Cleanup(func() { usableCoresFn = original })

	c := &Collector{instanceID: "test", version: "test", logger: zerolog.Nop()}
	body, err := json.Marshal(c.collectPayload())
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	var decoded struct {
		CPU struct {
			UsableCores  *int `json:"usable_cores"`
			LogicalCores *int `json:"logical_cores"`
		} `json:"cpu"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if decoded.CPU.UsableCores == nil || *decoded.CPU.UsableCores != sentinel {
		t.Errorf("cpu.usable_cores = %v, want %d", decoded.CPU.UsableCores, sentinel)
	}
	if decoded.CPU.LogicalCores == nil || *decoded.CPU.LogicalCores != runtime.NumCPU() {
		t.Errorf("cpu.logical_cores = %v, want %d", decoded.CPU.LogicalCores, runtime.NumCPU())
	}
}

// Same guard as internal/license's: the collector is built long after a
// license may have pinned GOMAXPROCS down to its core limit, and it re-reads
// on every tick, so a live reader here would report the licensed count as the
// node's size. Identity rather than behaviour, for the reason spelled out in
// TestUsableCoresSeamIsTheStartupSnapshot.
func TestUsableCoresSeamIsTheStartupSnapshot(t *testing.T) {
	got := reflect.ValueOf(usableCoresFn).Pointer()
	if want := reflect.ValueOf(syscpu.CoresAtStartup).Pointer(); got != want {
		if got == reflect.ValueOf(syscpu.EffectiveCores).Pointer() {
			t.Fatal("usableCoresFn is syscpu.EffectiveCores: a telemetry tick after the license pins GOMAXPROCS would report the licensed core count as the node size (#1039)")
		}
		t.Fatal("usableCoresFn is not syscpu.CoresAtStartup; outbound reports must use the startup snapshot")
	}
}
