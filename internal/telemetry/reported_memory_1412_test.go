package telemetry

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/basekick-labs/arc/internal/sysmem"
	"github.com/rs/zerolog"
)

const gib = 1024 * 1024 * 1024

func withMemSeams(t *testing.T, physical, usable func() (uint64, sysmem.Source, bool)) {
	t.Helper()
	p, u := physicalMemFn, usableMemFn
	physicalMemFn, usableMemFn = physical, usable
	t.Cleanup(func() { physicalMemFn, usableMemFn = p, u })
}

func newTestCollector(t *testing.T) *Collector {
	t.Helper()
	c, err := New(&Config{DataDir: t.TempDir()}, "test", zerolog.Nop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// total_gb is the machine and usable_gb the limit, each from its own reader.
// Distinct sentinels catch a swap, which no bare-metal or macOS run can: there
// the two readers agree (#1412).
func TestCollectPayloadReportsMachineAndUsableMemory(t *testing.T) {
	withMemSeams(t,
		func() (uint64, sysmem.Source, bool) { return 64 * gib, sysmem.SourceMemInfo, true },
		func() (uint64, sysmem.Source, bool) { return 2 * gib, sysmem.SourceCgroupV2, true },
	)
	p := newTestCollector(t).collectPayload()
	if p.Memory.TotalGB == nil || *p.Memory.TotalGB != 64 {
		t.Errorf("total_gb = %v, want 64 from sysmem.Physical", p.Memory.TotalGB)
	}
	if p.Memory.UsableGB == nil || *p.Memory.UsableGB != 2 {
		t.Errorf("usable_gb = %v, want 2 from sysmem.Limit", p.Memory.UsableGB)
	}
}

// An undetectable reading is a null, never a guess.
func TestCollectPayloadMemoryNullWhenUndetectable(t *testing.T) {
	none := func() (uint64, sysmem.Source, bool) { return 0, sysmem.SourceUnknown, false }
	withMemSeams(t, none, none)
	p := newTestCollector(t).collectPayload()
	if p.Memory.TotalGB != nil || p.Memory.UsableGB != nil {
		t.Errorf("memory = (%v, %v), want both nil", p.Memory.TotalGB, p.Memory.UsableGB)
	}
}

// Read once at startup: a later change in what the readers return does not
// reach the payload.
func TestMemoryIsReadAtStartup(t *testing.T) {
	val := uint64(8 * gib)
	read := func() (uint64, sysmem.Source, bool) { return val, sysmem.SourceMemInfo, true }
	withMemSeams(t, read, read)
	c := newTestCollector(t)
	val = 1 * gib
	p := c.collectPayload()
	if p.Memory.TotalGB == nil || *p.Memory.TotalGB != 8 {
		t.Errorf("total_gb = %v, want the startup reading 8", p.Memory.TotalGB)
	}
	if p.Memory.UsableGB == nil || *p.Memory.UsableGB != 8 {
		t.Errorf("usable_gb = %v, want the startup reading 8", p.Memory.UsableGB)
	}
}

// Wire names, and null (not absent) when undetectable: the collector stores
// usable_gb as memory_usable_gb and keys "fixed reporter" off the key.
func TestMemoryMarshalsUnderItsWireNames(t *testing.T) {
	withMemSeams(t,
		func() (uint64, sysmem.Source, bool) { return 64 * gib, sysmem.SourceMemInfo, true },
		func() (uint64, sysmem.Source, bool) { return 0, sysmem.SourceUnknown, false },
	)
	body, err := json.Marshal(newTestCollector(t).collectPayload())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded struct {
		Memory map[string]*float64 `json:"memory"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v := decoded.Memory["total_gb"]; v == nil || *v != 64 {
		t.Errorf("memory.total_gb = %v, want 64", v)
	}
	if v, present := decoded.Memory["usable_gb"]; !present || v != nil {
		t.Errorf("memory.usable_gb = %v (present %v), want an explicit null", v, present)
	}
}

// The production seams are sysmem's readers, in the right slots.
func TestMemorySeamsAreSysmem(t *testing.T) {
	if reflect.ValueOf(physicalMemFn).Pointer() != reflect.ValueOf(sysmem.Physical).Pointer() {
		t.Error("physicalMemFn is not sysmem.Physical")
	}
	if reflect.ValueOf(usableMemFn).Pointer() != reflect.ValueOf(sysmem.Limit).Pointer() {
		t.Error("usableMemFn is not sysmem.Limit")
	}
}
