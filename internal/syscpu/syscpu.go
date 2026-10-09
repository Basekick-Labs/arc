// Package syscpu answers how many CPUs this process may actually use.
//
// It exists as a leaf so the packages that report a core count outbound —
// internal/license and internal/telemetry — can ask the question without
// taking on internal/config's dependency closure, which reaches
// internal/storage and from there the AWS and Azure SDKs. internal/license
// imported no other internal package before this one, and it holds signature
// verification and machine fingerprinting; pulling 100-odd cloud-SDK packages
// into it to obtain min(a, b) is not a trade worth making. Measured: license
// 192 -> 193 transitive deps with this leaf, against 439 for config.
//
// Importing config would also have worked — there is no cycle and the linked
// size barely moves. This is a judgement call about keeping that package's
// dependency surface small, not a correctness constraint.
//
// Follows internal/sysmem as the precedent for a tiny leaf that config
// consumes. Unlike sysmem it needs no injectable seam of its own, because
// effectiveCores is pure; the unexported seams live in the consumers.
//
// internal/config forwards to this package, so there is one implementation.
package syscpu

import "runtime"

// EffectiveCores reports how many CPUs this process may actually use, read now.
//
// runtime.NumCPU() is not that number: it reflects cpuset/affinity but NOT a
// CFS quota, and Kubernetes limits.cpu and docker --cpus are quotas — so a
// 2-CPU pod on a 64-core host reports 64 (#1026, #1030). runtime.GOMAXPROCS(0)
// IS quota-aware; since Go 1.25 the runtime computes
// min(affinity_cpus, max(ceil(quota), 2)) and this module's go directive enables
// it. Measured on an 8-CPU VM: --cpus=2, --cpus=1.5 and --cpus=0.5 all give 2
// (the runtime floors at 2), while --cpuset-cpus=0-1 gives NumCPU() == 2.
//
// The minimum of the two is taken because neither alone is the answer. The
// GOMAXPROCS environment variable overrides the runtime's detection with no
// clamp, so GOMAXPROCS=128 on an 8-CPU box really does report 128; NumCPU
// covers cpuset limits that no quota expresses.
//
// Two residuals are deliberate. A GOMAXPROCS between the quota and the machine
// size is honoured (GOMAXPROCS=32 under --cpus=2 yields 32), and
// GODEBUG=containermaxprocs=0 disables the runtime's cgroup read altogether.
// Both are an operator explicitly overriding their own runtime's container
// awareness. Reading cpu.max here instead would out-guess them at the price of
// reimplementing, for a third time, the cgroup parsing #1026 deleted.
//
// Callers deciding how much work to run want this live form. Callers REPORTING
// the node's size want CoresAtStartup — see its doc for why they differ.
func EffectiveCores() int {
	return effectiveCores(runtime.NumCPU(), runtime.GOMAXPROCS(0))
}

// startupCores is EffectiveCores as it stood before anything in this process
// could pin GOMAXPROCS. Package-level initialization runs before main, and the
// runtime has already applied the GOMAXPROCS environment variable and its own
// cgroup read by then, so this is the container's answer and nothing else's.
var startupCores = effectiveCores(runtime.NumCPU(), runtime.GOMAXPROCS(0))

// CoresAtStartup is the usable core count as it was at process start.
//
// Use this, not EffectiveCores, for any value reported OUTWARD — to the
// activation server, to telemetry — because a licence enforces itself by
// pinning GOMAXPROCS down to its core limit (applyLicenseCoreLimits in
// cmd/arc/main.go, unconditionally whenever MaxCores is positive, and
// deliberately so: the pin is what stops the runtime re-reading a CPU quota
// that was raised later). After that pin a live EffectiveCores() read returns
// min(quota, licensed_cores), so on 64-core bare metal with a 32-core licence
// it answers 32 — a LICENCE number, from a function whose documented subject is
// the CPU quota, on a machine that has no quota at all.
//
// That matters because the reads are not all at boot: periodic re-validation
// re-activates whenever the server has reaped this machine's activation
// (a routine condition — see ActivateOrVerify), and the telemetry collector is
// built long after the pin and re-reads on every tick. Reporting a snapshot
// makes all of them agree, and keeps one process's reported size stable for
// its whole life the way runtime.NumCPU() always was.
//
// cmd/arc/main.go's DuckDB log line deliberately reads the live form instead,
// because there the subject really is "what may this process use now".
//
// Two residuals, both inherited from EffectiveCores and both worth knowing
// here because outbound reporters land on this function first:
// GODEBUG=containermaxprocs=0 disables the runtime's cgroup read, so this
// equals runtime.NumCPU() and reporting it changes nothing; and the snapshot
// is taken once, so a CPU limit changed IN PLACE without a restart (an
// in-place pod resize, docker update --cpus) is not picked up — the Go runtime
// re-reads the cgroup within about a second, but this value does not. Both are
// accepted: a live read is wrong on every LICENSED node, which is the larger
// and permanent error.
func CoresAtStartup() int { return startupCores }

// effectiveCores is the pure form, so its table tests need not mutate
// process-global runtime state to cover it.
func effectiveCores(numCPU, gomaxprocs int) int {
	cores := numCPU
	if gomaxprocs > 0 && gomaxprocs < cores {
		cores = gomaxprocs
	}
	if cores < 1 {
		cores = 1
	}
	return cores
}
