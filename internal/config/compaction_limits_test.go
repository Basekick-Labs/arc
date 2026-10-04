package config

import (
	"fmt"
	"testing"

	"github.com/basekick-labs/arc/internal/sysmem"
)

func TestDeriveCompactionMemoryLimit(t *testing.T) {
	tests := []struct {
		name          string
		dbLimit       string
		maxConcurrent int
		want          string
		// skipRegexCheck marks fallback outputs that intentionally return the
		// (invalid) input verbatim rather than a derivable limit.
		skipRegexCheck bool
	}{
		{name: "even GB division", dbLimit: "8GB", maxConcurrent: 2, want: "4GB"},
		{name: "odd GB division keeps decimals", dbLimit: "7GB", maxConcurrent: 2, want: "3.5GB"},
		{name: "floors to 2 decimals, never rounds total up", dbLimit: "14GB", maxConcurrent: 3, want: "4.66GB"},
		{name: "MB unit preserved", dbLimit: "512MB", maxConcurrent: 2, want: "256MB"},
		{name: "decimal input", dbLimit: "2.5GB", maxConcurrent: 2, want: "1.25GB"},
		{name: "sub-GB decimal result", dbLimit: "0.5GB", maxConcurrent: 2, want: "0.25GB"},
		{name: "explicit B unit works", dbLimit: "100000B", maxConcurrent: 2, want: "50000B"},
		{name: "max_concurrent 1 returns input verbatim", dbLimit: "8GB", maxConcurrent: 1, want: "8GB"},
		{name: "max_concurrent 0 treated as default 2", dbLimit: "8GB", maxConcurrent: 0, want: "4GB"},
		{name: "whitespace tolerated like the validation regex", dbLimit: "8 GB", maxConcurrent: 2, want: "4GB"},
		// DuckDB's SET memory_limit rejects percent and unit-less forms, and
		// either as database.memory_limit aborts startup at the main DB's loud
		// SET — derive nothing rather than smuggle an un-SETtable value
		// downstream to the subprocess's warn-only SET.
		{name: "percent input derives nothing", dbLimit: "80%", maxConcurrent: 2, want: ""},
		{name: "unit-less input derives nothing", dbLimit: "1000000", maxConcurrent: 2, want: ""},
		{name: "percent with max_concurrent 1 still derives nothing", dbLimit: "80%", maxConcurrent: 1, want: ""},
		// Defensive fallbacks — unreachable through Load (dbLimit is already
		// regex-validated there); return the input verbatim, i.e. the
		// pre-derivation behavior of one full database limit per subprocess.
		{name: "unparseable input returned verbatim (defensive)", dbLimit: "lots", maxConcurrent: 2, want: "lots", skipRegexCheck: true},
		{name: "near-zero result falls back to input", dbLimit: "0.01GB", maxConcurrent: 2, want: "0.01GB"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deriveCompactionMemoryLimit(tt.dbLimit, tt.maxConcurrent)
			if got != tt.want {
				t.Errorf("deriveCompactionMemoryLimit(%q, %d) = %q, want %q", tt.dbLimit, tt.maxConcurrent, got, tt.want)
			}
			// Whatever we derive must itself pass the memory_limit validation
			// regex — the derived value is later handed to DuckDB SET.
			if got != "" && !tt.skipRegexCheck && !memoryLimitRe.MatchString(got) {
				t.Errorf("derived value %q does not match memoryLimitRe", got)
			}
		})
	}
}

// TestValidateCompactionMemoryLimit covers the forms DuckDB's SET
// memory_limit accepts vs rejects. Unlike database.memory_limit (whose failed
// SET aborts startup loudly), the compaction subprocess only warns on a failed
// SET — so validation must reject anything DuckDB would refuse, or the
// subprocess runs silently unbounded.
func TestValidateCompactionMemoryLimit(t *testing.T) {
	valid := []string{"", "2GB", "512MB", "0.5GB", "8 GB", "100000B", "1.5TB", "4KB"}
	for _, v := range valid {
		if err := validateCompactionMemoryLimit(v); err != nil {
			t.Errorf("validateCompactionMemoryLimit(%q) = %v, want nil", v, err)
		}
	}
	invalid := []string{"40%", "80 %", "1000000", "0.5", "bogus", "GB", "-1GB"}
	for _, v := range invalid {
		if err := validateCompactionMemoryLimit(v); err == nil {
			t.Errorf("validateCompactionMemoryLimit(%q) = nil, want error", v)
		}
	}
}

// TestGetDefaultCompactionThreads_UsesEffectiveCores drives the effective-core
// count through the injectable seam, because CI runners may have no CPU quota.
func TestGetDefaultCompactionThreads_UsesEffectiveCores(t *testing.T) {
	original := effectiveCoresFn
	defer func() { effectiveCoresFn = original }()

	for _, c := range []struct {
		cores, maxConcurrent, want int
	}{
		{8, 2, 4},  // the default stays byte-for-byte equivalent to the prior half-core value
		{16, 4, 4}, // elevated concurrency divides available cores; the old quota formula returns 3
		{8, 4, 2},  // a constrained process uses the same concurrency rule
		{8, 0, 4},  // non-positive values retain NewManager's default of two
	} {
		t.Run(fmt.Sprintf("cores_%d_concurrent_%d", c.cores, c.maxConcurrent), func(t *testing.T) {
			effectiveCoresFn = func() int { return c.cores }
			if got := getDefaultCompactionThreads(c.maxConcurrent); got != c.want {
				t.Errorf("with %d effective cores and max_concurrent=%d: got %d, want %d", c.cores, c.maxConcurrent, got, c.want)
			}
		})
	}
	if got := defaultCompactionThreads(16, 4); got == 16/5 {
		t.Errorf("raised-concurrency result %d matches the old max_concurrent+1 divisor result", got)
	}
}

// TestLoad_CompactionThreadsResolvesFromEffectiveCores pins Load's wiring with
// fixed injected core counts; it runs deterministically on low-core CI too.
func TestLoad_CompactionThreadsResolvesFromEffectiveCores(t *testing.T) {
	original := effectiveCoresFn
	defer func() { effectiveCoresFn = original }()
	t.Chdir(t.TempDir())

	for _, c := range []struct {
		cores, maxConcurrent, want int
	}{
		{8, 2, 4},
		{16, 4, 4},
	} {
		t.Run(fmt.Sprintf("cores_%d_concurrent_%d", c.cores, c.maxConcurrent), func(t *testing.T) {
			effectiveCoresFn = func() int { return c.cores }
			t.Setenv("ARC_COMPACTION_MAX_CONCURRENT", fmt.Sprint(c.maxConcurrent))
			t.Setenv("ARC_COMPACTION_THREADS", "0")
			t.Chdir(t.TempDir())

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.Compaction.Threads != c.want {
				t.Errorf("with %d effective cores and max_concurrent=%d: Compaction.Threads = %d, want %d", c.cores, c.maxConcurrent, cfg.Compaction.Threads, c.want)
			}
		})
	}
}

func TestLoad_ExplicitCompactionThreadsSurvives(t *testing.T) {
	original := effectiveCoresFn
	defer func() { effectiveCoresFn = original }()
	effectiveCoresFn = func() int { return 2 }
	t.Setenv("ARC_COMPACTION_THREADS", "6")
	t.Chdir(t.TempDir())

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Compaction.Threads != 6 {
		t.Errorf("Compaction.Threads = %d, want 6 (explicit values are not rewritten)", cfg.Compaction.Threads)
	}
}

// TestDeriveCompactionMemoryLimit_EmptyDatabaseLimitDerivesFromSystem pins the
// contract change in #1026.
//
// This case used to return "", which was correct while database.memory_limit
// always had a value. It no longer does: Arc now leaves that empty so DuckDB
// applies its own cgroup-aware default. Returning "" here would mean EVERY
// compaction subprocess also falls back to DuckDB's default and takes 80% of the
// same cgroup — a main process plus the default two subprocesses budgeting 240%
// of the container. subprocess.go only warns when a SET fails, so it would do
// that silently.
//
// The share is detected*0.8/(maxConcurrent+1): the +1 reserves the main
// process's share, because the subprocesses are separate processes in the SAME
// cgroup rather than independent budgets.
func TestDeriveCompactionMemoryLimit_EmptyDatabaseLimitDerivesFromSystem(t *testing.T) {
	detected, _, ok := sysmem.Limit()
	if !ok {
		t.Skip("no memory limit detectable on this platform")
	}

	for _, maxConcurrent := range []int{1, 2, 4} {
		got := deriveCompactionMemoryLimit("", maxConcurrent)
		if got == "" {
			t.Fatalf("maxConcurrent=%d: derived \"\", so each subprocess would take DuckDB's own 80%% of the whole cgroup (#1026)", maxConcurrent)
		}
		if !memoryLimitRe.MatchString(got) {
			t.Fatalf("maxConcurrent=%d: derived %q, which config validation would reject", maxConcurrent, got)
		}
		if err := validateCompactionMemoryLimit(got); err != nil {
			t.Fatalf("maxConcurrent=%d: derived %q, which DuckDB would reject: %v", maxConcurrent, got, err)
		}

		// The whole point: a share, never the whole box.
		var bytes uint64
		if _, err := fmt.Sscanf(got, "%dB", &bytes); err != nil {
			t.Fatalf("derived %q is not the exact <bytes>B form: %v", got, err)
		}
		if bytes >= detected {
			t.Fatalf("maxConcurrent=%d: derived %d bytes from a detected limit of %d — that is not a share", maxConcurrent, bytes, detected)
		}
		want := uint64(float64(detected)*0.8) / uint64(maxConcurrent+1)
		if bytes != want {
			t.Fatalf("maxConcurrent=%d: derived %d bytes, want %d (detected*0.8/(maxConcurrent+1))", maxConcurrent, bytes, want)
		}
	}
}

// TestFormatDuckDBBytes_IsAcceptedEverywhere pins the string form.
//
// memoryLimitRe makes the unit OPTIONAL, so a bare number passes config
// validation and then hard-fails inside DuckDB with `Unknown unit for memory:
// ”` — a startup crash in every deployment. The binary units that would be
// exact (MiB/GiB) are the ones memoryLimitRe rejects, and MB/GB are powers of
// 1000 so they cannot render a byte count exactly. "<bytes>B" is the only form
// that is both exact and accepted by both validators.
func TestFormatDuckDBBytes_IsAcceptedEverywhere(t *testing.T) {
	for _, b := range []uint64{1, 256 << 20, 536870912, 1 << 30, 1<<30 + 1} {
		got := formatDuckDBBytes(b)
		if !memoryLimitRe.MatchString(got) {
			t.Fatalf("formatDuckDBBytes(%d) = %q, which memoryLimitRe rejects", b, got)
		}
		if err := validateDuckDBMemoryLimit("database.memory_limit", got); err != nil {
			t.Fatalf("formatDuckDBBytes(%d) = %q, rejected by the DuckDB rule: %v", b, got, err)
		}
	}
}

// TestValidateDuckDBMemoryLimit_RejectsWhatDuckDBRejects closes a pre-existing
// startup crash: database.memory_limit was checked against the loose regex only,
// so "50%" and a bare number passed config load and then failed inside DuckDB.
func TestValidateDuckDBMemoryLimit_RejectsWhatDuckDBRejects(t *testing.T) {
	for _, bad := range []string{"50%", "0", "536870912", "100"} {
		if err := validateDuckDBMemoryLimit("database.memory_limit", bad); err == nil {
			t.Errorf("validateDuckDBMemoryLimit accepted %q; DuckDB fails with \"Unknown unit for memory\" and the node would crash at startup", bad)
		}
	}
	for _, good := range []string{"", "8GB", "512MB", "536870912B", "1.5GB"} {
		if err := validateDuckDBMemoryLimit("database.memory_limit", good); err != nil {
			t.Errorf("validateDuckDBMemoryLimit rejected %q: %v", good, err)
		}
	}
}
