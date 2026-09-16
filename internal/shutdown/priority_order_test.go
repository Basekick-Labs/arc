package shutdown

// Hooks and components of equal priority run in the order they were
// registered. The previous selection sort reordered equal keys, so the
// relative order of the ten hooks sharing one priority depended on how many
// lower-priority hooks happened to exist — which varies with configuration,
// so the same binary shut down differently in OSS and in a cluster (#854).

import (
	"reflect"
	"testing"
)

func TestSortHooksByPriority_KeepsRegistrationOrderWithinAPriority(t *testing.T) {
	hooks := []namedHook{
		{name: "sched-a", priority: PriorityScheduler},
		{name: "sched-b", priority: PriorityScheduler},
		{name: "coordinator", priority: PriorityCompaction},
		{name: "http", priority: PriorityHTTPServer},
		{name: "sched-c", priority: PriorityScheduler},
		{name: "audit", priority: PriorityCompaction},
		{name: "db", priority: PriorityDatabase},
	}
	sortHooksByPriority(hooks)

	var got []string
	for _, h := range hooks {
		got = append(got, h.name)
	}
	want := []string{"http", "sched-a", "sched-b", "sched-c", "coordinator", "audit", "db"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("order = %v\nwant     %v", got, want)
	}
}

// The order must not depend on how many unrelated lower-priority hooks are
// registered, which is what made it configuration-dependent.
func TestSortHooksByPriority_IsIndependentOfUnrelatedHooks(t *testing.T) {
	order := func(extra int) []string {
		hooks := []namedHook{
			{name: "first", priority: PriorityCompaction},
			{name: "second", priority: PriorityCompaction},
			{name: "third", priority: PriorityCompaction},
		}
		for i := 0; i < extra; i++ {
			hooks = append(hooks, namedHook{name: "other", priority: PriorityHTTPServer})
		}
		sortHooksByPriority(hooks)
		var out []string
		for _, h := range hooks {
			if h.name != "other" {
				out = append(out, h.name)
			}
		}
		return out
	}
	base := order(0)
	for _, extra := range []int{1, 2, 3, 7} {
		if got := order(extra); !reflect.DeepEqual(got, base) {
			t.Errorf("with %d unrelated hooks the order became %v; want %v", extra, got, base)
		}
	}
}

func TestSortComponentsByPriority_KeepsRegistrationOrderWithinAPriority(t *testing.T) {
	components := []namedComponent{
		{name: "b", priority: PriorityStorage},
		{name: "a", priority: PriorityStorage},
		{name: "early", priority: PriorityBuffer},
		{name: "c", priority: PriorityStorage},
	}
	sortComponentsByPriority(components)

	var got []string
	for _, c := range components {
		got = append(got, c.name)
	}
	want := []string{"early", "b", "a", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("order = %v\nwant     %v", got, want)
	}
}

// The cluster-gated schedulers must stop before the coordinator they ask for
// permission, so a tick in flight never finds it gone.
func TestPriorityScheduler_RunsBeforeTheCoordinator(t *testing.T) {
	if PriorityScheduler >= PriorityCompaction {
		t.Fatalf("PriorityScheduler (%d) must be below PriorityCompaction (%d), where the cluster coordinator stops", PriorityScheduler, PriorityCompaction)
	}
	if PriorityScheduler <= PriorityWAL {
		t.Errorf("PriorityScheduler (%d) must run after the WAL flush (%d), or a scheduler could still be producing work while the WAL closes", PriorityScheduler, PriorityWAL)
	}
}
