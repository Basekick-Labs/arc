package config

import (
	"runtime"
	"testing"
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
		{name: "empty input returns empty (nothing to derive)", dbLimit: "", maxConcurrent: 2, want: ""},
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

func TestGetDefaultCompactionThreads(t *testing.T) {
	got := getDefaultCompactionThreads()
	if got < 1 {
		t.Errorf("getDefaultCompactionThreads() = %d, want >= 1", got)
	}
	if want := runtime.NumCPU() / 2; want >= 1 && got != want {
		t.Errorf("getDefaultCompactionThreads() = %d, want %d (half of %d cores)", got, want, runtime.NumCPU())
	}
}
