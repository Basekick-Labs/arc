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
		{16, 4, 4}, // raised concurrency divides the cores; the pre-#1037 always-halve rule returns 8
		{8, 4, 2},  // a constrained process uses the same concurrency rule
		{8, 0, 4},  // an explicit max_concurrent = 0 falls back to two, as NewManager does
	} {
		t.Run(fmt.Sprintf("cores_%d_concurrent_%d", c.cores, c.maxConcurrent), func(t *testing.T) {
			effectiveCoresFn = func() int { return c.cores }
			if got := getDefaultCompactionThreads(c.maxConcurrent); got != c.want {
				t.Errorf("with %d effective cores and max_concurrent=%d: got %d, want %d", c.cores, c.maxConcurrent, got, c.want)
			}
		})
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
// fallback used when Load cannot derive a cgroup main-database limit. The
// subprocesses share detected*0.8/(maxConcurrent+1); the +1 reserves a share
// for the main process when DuckDB's own limit remains in effect.
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

func TestLoad_CgroupMemoryDefaultLeavesHeadroom(t *testing.T) {
	original := systemMemoryLimitFn
	t.Cleanup(func() { systemMemoryLimitFn = original })
	systemMemoryLimitFn = func() (uint64, sysmem.Source, bool) {
		return 3 << 30, sysmem.SourceCgroupV2, true
	}

	t.Chdir(t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("ARC_DATABASE_MEMORY_LIMIT", "")
	t.Setenv("ARC_COMPACTION_MEMORY_LIMIT", "")
	t.Setenv("ARC_COMPACTION_MAX_CONCURRENT", "2")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	const cgroupLimit = uint64(3 << 30)
	wantDatabase := formatDuckDBBytes(cgroupLimit * 3 / 5)
	wantCompaction := formatDuckDBBytes(cgroupLimit / 5 / 2)
	if cfg.Database.MemoryLimit != wantDatabase {
		t.Errorf("database.memory_limit = %q, want %q (60%% of cgroup)", cfg.Database.MemoryLimit, wantDatabase)
	}
	if cfg.Compaction.MemoryLimit != wantCompaction {
		t.Errorf("compaction.memory_limit = %q, want %q (20%% cgroup budget split across two subprocesses)", cfg.Compaction.MemoryLimit, wantCompaction)
	}
}

func TestDeriveDefaultDatabaseMemoryLimitSupportsBothCgroups(t *testing.T) {
	want := formatDuckDBBytes(uint64(3<<30) * 3 / 5)
	for _, source := range []sysmem.Source{sysmem.SourceCgroupV1, sysmem.SourceCgroupV2} {
		if got := deriveDefaultDatabaseMemoryLimit(3<<30, source, true); got != want {
			t.Errorf("source %q: database memory limit = %q, want %q", source, got, want)
		}
	}
}

func TestDeriveDefaultCgroupCompactionMemoryLimitSharesBudget(t *testing.T) {
	detected := uint64(3 << 30)
	for _, maxConcurrent := range []int{0, 1, 2, 4} {
		got := deriveDefaultCgroupCompactionMemoryLimit(detected, maxConcurrent)
		if got == "" {
			t.Fatalf("maxConcurrent=%d: got empty memory limit", maxConcurrent)
		}
		var share uint64
		if _, err := fmt.Sscanf(got, "%dB", &share); err != nil {
			t.Fatalf("maxConcurrent=%d: invalid derived limit %q: %v", maxConcurrent, got, err)
		}
		effectiveConcurrent := maxConcurrent
		if effectiveConcurrent <= 0 {
			effectiveConcurrent = 2
		}
		compactionLimit := uint64(float64(detected) * defaultCompactionBudgetFraction)
		if total := share * uint64(effectiveConcurrent); total > compactionLimit {
			t.Errorf("maxConcurrent=%d: aggregate compaction budget %d exceeds 20%% of %d", maxConcurrent, total, detected)
		}
		databaseLimit := uint64(float64(detected) * defaultDatabaseMemoryFraction)
		combinedLimit := uint64(float64(detected) * duckdbDefaultMemoryFraction)
		if total := databaseLimit + share*uint64(effectiveConcurrent); total > combinedLimit {
			t.Errorf("maxConcurrent=%d: combined DuckDB budget %d exceeds 80%% of %d", maxConcurrent, total, detected)
		}
	}
}

func TestLoad_ExplicitDatabaseMemoryLimitStillWins(t *testing.T) {
	original := systemMemoryLimitFn
	t.Cleanup(func() { systemMemoryLimitFn = original })
	var detections int
	systemMemoryLimitFn = func() (uint64, sysmem.Source, bool) {
		detections++
		return 3 << 30, sysmem.SourceCgroupV2, true
	}

	t.Chdir(t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("ARC_DATABASE_MEMORY_LIMIT", "2GB")
	t.Setenv("ARC_COMPACTION_MEMORY_LIMIT", "")
	t.Setenv("ARC_COMPACTION_MAX_CONCURRENT", "2")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Database.MemoryLimit != "2GB" {
		t.Errorf("database.memory_limit = %q, want explicit value 2GB", cfg.Database.MemoryLimit)
	}
	if cfg.Compaction.MemoryLimit != "1GB" {
		t.Errorf("compaction.memory_limit = %q, want existing explicit-limit derivation 1GB", cfg.Compaction.MemoryLimit)
	}
	if detections != 0 {
		t.Errorf("system memory detected %d times with an explicit database.memory_limit; want no probe", detections)
	}
}

func TestLoad_ExplicitCompactionMemoryLimitStillWins(t *testing.T) {
	original := systemMemoryLimitFn
	t.Cleanup(func() { systemMemoryLimitFn = original })
	systemMemoryLimitFn = func() (uint64, sysmem.Source, bool) {
		return 3 << 30, sysmem.SourceCgroupV2, true
	}

	t.Chdir(t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("ARC_DATABASE_MEMORY_LIMIT", "")
	t.Setenv("ARC_COMPACTION_MEMORY_LIMIT", "256MB")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Database.MemoryLimit != formatDuckDBBytes(uint64(3<<30)*3/5) {
		t.Errorf("database.memory_limit = %q, want the cgroup-derived default", cfg.Database.MemoryLimit)
	}
	if cfg.Compaction.MemoryLimit != "256MB" {
		t.Errorf("compaction.memory_limit = %q, want explicit value 256MB", cfg.Compaction.MemoryLimit)
	}
}

func TestLoad_WithoutCgroupKeepsDuckDBDatabaseDefault(t *testing.T) {
	original := systemMemoryLimitFn
	t.Cleanup(func() { systemMemoryLimitFn = original })
	detected := uint64(3 << 30)
	systemMemoryLimitFn = func() (uint64, sysmem.Source, bool) {
		return detected, sysmem.SourceMemInfo, true
	}

	t.Chdir(t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("ARC_DATABASE_MEMORY_LIMIT", "")
	t.Setenv("ARC_COMPACTION_MEMORY_LIMIT", "")
	t.Setenv("ARC_COMPACTION_MAX_CONCURRENT", "2")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Database.MemoryLimit != "" {
		t.Errorf("database.memory_limit = %q, want empty without a cgroup limit", cfg.Database.MemoryLimit)
	}
	wantCompaction := formatDuckDBBytes(uint64(float64(detected)*duckdbDefaultMemoryFraction) / 3)
	if cfg.Compaction.MemoryLimit != wantCompaction {
		t.Errorf("compaction.memory_limit = %q, want legacy host-derived value %q", cfg.Compaction.MemoryLimit, wantCompaction)
	}
}

func TestLoad_SmallCgroupKeepsDuckDBDatabaseDefault(t *testing.T) {
	original := systemMemoryLimitFn
	t.Cleanup(func() { systemMemoryLimitFn = original })
	systemMemoryLimitFn = func() (uint64, sysmem.Source, bool) {
		return 128 << 20, sysmem.SourceCgroupV2, true
	}

	t.Chdir(t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("ARC_DATABASE_MEMORY_LIMIT", "")
	t.Setenv("ARC_COMPACTION_MEMORY_LIMIT", "")
	t.Setenv("ARC_COMPACTION_MAX_CONCURRENT", "2")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Database.MemoryLimit != "" {
		t.Errorf("database.memory_limit = %q, want empty below the 128 MiB useful limit", cfg.Database.MemoryLimit)
	}
	cgroupLimit := uint64(128 << 20)
	wantCompaction := formatDuckDBBytes(uint64(float64(cgroupLimit)*duckdbDefaultMemoryFraction) / 3)
	if cfg.Compaction.MemoryLimit != wantCompaction {
		t.Errorf("compaction.memory_limit = %q, want legacy fallback %q", cfg.Compaction.MemoryLimit, wantCompaction)
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
