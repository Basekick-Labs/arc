package config

import (
	"strings"
	"testing"
	"time"
)

// backup.operation_timeout replaces the 2h that was hardcoded at both backup
// API routes (#1085). It is read with time.ParseDuration over GetString, the
// way compaction.cycle_timeout is, because there is no GetDuration call in
// this repo; the shape of this test follows that one.
//
// The default case is the one that matters most: unset must still be 2h, so
// no deployment changes behaviour by upgrading.
func TestBackupOperationTimeout(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{"default", "", 2 * time.Hour, false},
		{"nondefault", "30m", 30 * time.Minute, false},
		{"long", "12h", 12 * time.Hour, false},
		{"seconds", "90s", 90 * time.Second, false},
		// Zero is a load-time error rather than an unbounded operation: a
		// context with a zero timeout is already expired, and "no timeout"
		// would leave a wedged run holding the single-operation slot forever.
		{"zero", "0s", 0, true},
		{"negative", "-5m", 0, true},
		{"malformed", "banana", 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_BACKUP_OPERATION_TIMEOUT", tc.value)

			cfg, err := Load()
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "backup.operation_timeout") {
					t.Fatalf("got %v, want a backup.operation_timeout validation error naming the key", err)
				}
				// The message must name the offending value, so an operator
				// can see what they typed.
				if tc.value != "" && !strings.Contains(err.Error(), tc.value) {
					t.Errorf("error %v does not name the rejected value %q", err, tc.value)
				}
				return
			}

			if err != nil {
				t.Fatal(err)
			}
			if cfg.Backup.OperationTimeout != tc.want {
				t.Fatalf("OperationTimeout = %s, want %s", cfg.Backup.OperationTimeout, tc.want)
			}
		})
	}
}

// The Azure key prefix is an operator-facing key on both the primary store and
// the cold tier (#1102), so both must have a default and both must read back.
func TestAzurePrefixConfigKeys(t *testing.T) {
	t.Run("defaults to the container root", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Storage.AzurePrefix != "" {
			t.Errorf("storage.azure_prefix defaults to %q, want the container root", cfg.Storage.AzurePrefix)
		}
		if cfg.TieredStorage.Cold.AzurePrefix != "" {
			t.Errorf("tiered_storage.cold.azure_prefix defaults to %q, want the container root", cfg.TieredStorage.Cold.AzurePrefix)
		}
	})

	t.Run("reads back and is trimmed", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_STORAGE_AZURE_PREFIX", "  arc/hot  ")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Storage.AzurePrefix != "arc/hot" {
			t.Errorf("storage.azure_prefix = %q, want %q", cfg.Storage.AzurePrefix, "arc/hot")
		}
	})

	// The cold-tier trim only runs when the tier is actually enabled, matching
	// every other cold-tier value (config.Load gates the whole block), so the
	// cold case must enable it to see the trimmed value.
	t.Run("cold tier reads back when enabled", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_TIERED_STORAGE_ENABLED", "true")
		t.Setenv("ARC_TIERED_STORAGE_COLD_ENABLED", "true")
		t.Setenv("ARC_TIERED_STORAGE_COLD_BACKEND", "azure")
		t.Setenv("ARC_TIERED_STORAGE_COLD_AZURE_CONTAINER", "coldcont")
		t.Setenv("ARC_TIERED_STORAGE_COLD_AZURE_ACCOUNT_NAME", "acct")
		t.Setenv("ARC_TIERED_STORAGE_COLD_AZURE_PREFIX", "  cold  ")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.TieredStorage.Cold.AzurePrefix != "cold" {
			t.Errorf("tiered_storage.cold.azure_prefix = %q, want %q", cfg.TieredStorage.Cold.AzurePrefix, "cold")
		}
	})
}

// H1: every object-store prefix key is validated at LOAD, so an unusable one
// is a load-time error naming the key and the value, not a backend
// construction failure.
//
// The cold keys are why this exists. An unusable cold prefix fails
// storage.NewBackend at a call site that logs at Error and CONTINUES with a
// nil cold backend, so the tier would be silently dead; and that error reaches
// the operator through zerolog, where installErrSanitizer masks quoted spans
// globally, showing dots for both the value and the offending character. A
// load-time error is printed before the logger exists.
func TestObjectPrefixKeysAreValidatedAtLoad(t *testing.T) {
	// Each case sets its key to a prefix the storage contract refuses, and
	// must be refused by Load with the key named.
	cases := []struct {
		name  string
		key   string
		value string // the unusable prefix, as the error must echo it
		env   map[string]string
	}{
		{
			name:  "storage.s3_prefix",
			key:   "storage.s3_prefix",
			value: "a//b",
			env: map[string]string{
				"ARC_STORAGE_BACKEND":   "s3",
				"ARC_STORAGE_S3_BUCKET": "bkt",
				"ARC_STORAGE_S3_PREFIX": "a//b",
			},
		},
		{
			name:  "storage.azure_prefix",
			key:   "storage.azure_prefix",
			value: "a b",
			env: map[string]string{
				"ARC_STORAGE_BACKEND":            "azure",
				"ARC_STORAGE_AZURE_CONTAINER":    "cont",
				"ARC_STORAGE_AZURE_ACCOUNT_NAME": "acct",
				"ARC_STORAGE_AZURE_PREFIX":       "a b",
			},
		},
		{
			name:  "tiered_storage.cold.s3_prefix",
			key:   "tiered_storage.cold.s3_prefix",
			value: ".",
			env: map[string]string{
				"ARC_TIERED_STORAGE_ENABLED":        "true",
				"ARC_TIERED_STORAGE_COLD_ENABLED":   "true",
				"ARC_TIERED_STORAGE_COLD_BACKEND":   "s3",
				"ARC_TIERED_STORAGE_COLD_S3_BUCKET": "bkt",
				"ARC_TIERED_STORAGE_COLD_S3_PREFIX": ".",
			},
		},
		{
			name:  "tiered_storage.cold.azure_prefix",
			key:   "tiered_storage.cold.azure_prefix",
			value: "/",
			env: map[string]string{
				"ARC_TIERED_STORAGE_ENABLED":                 "true",
				"ARC_TIERED_STORAGE_COLD_ENABLED":            "true",
				"ARC_TIERED_STORAGE_COLD_BACKEND":            "azure",
				"ARC_TIERED_STORAGE_COLD_AZURE_CONTAINER":    "cont",
				"ARC_TIERED_STORAGE_COLD_AZURE_ACCOUNT_NAME": "acct",
				"ARC_TIERED_STORAGE_COLD_AZURE_PREFIX":       "/",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			if err == nil {
				t.Fatalf("Load accepted %s = %q", tc.key, tc.value)
			}
			if !strings.Contains(err.Error(), tc.key) {
				t.Errorf("error does not name the key %s; got: %v", tc.key, err)
			}
			// The value must survive into the message: a rejection that does
			// not echo what was configured is the masked-error problem this
			// check exists to avoid.
			if !strings.Contains(err.Error(), tc.value) {
				t.Errorf("error does not name the rejected value %q; got: %v", tc.value, err)
			}
		})
	}
}

// Every object-store prefix that ValidateObjectPrefix accepts loads without a
// warning, including one whose last segment is year-shaped.
//
// Such a prefix USED to warn: the query path read a database and measurement
// off the end of a storage path by scanning backwards for a partition year,
// so a prefix of three or more segments ending in one made it resolve the two
// prefix segments in front of that year instead. The query path now removes
// the configured prefix before that scan (#1108), so the shape is no longer
// special and advising against it would send an operator to rewrite every key
// in their bucket for nothing.
//
// The prefixes below are kept exactly as the warning case listed them, now
// asserting silence, so the two halves of this change are pinned together:
// the first group is the shape that warned, the second the shapes that never
// did.
func TestYearShapedPrefixTailLoadsWithoutWarning(t *testing.T) {
	load := func(t *testing.T, prefix string) *Config {
		t.Helper()
		t.Chdir(t.TempDir())
		t.Setenv("ARC_STORAGE_BACKEND", "s3")
		t.Setenv("ARC_STORAGE_S3_BUCKET", "bkt")
		t.Setenv("ARC_STORAGE_S3_PREFIX", prefix)
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load(%q): %v", prefix, err)
		}
		return cfg
	}

	for _, prefix := range []string{
		// Formerly warned: three or more segments ending in a year.
		"arc/prod/2026", "arc/prod/2026/", "a/b/2026", "w/x/y/2026",
		// Never warned: not year-shaped, or too short for the old scan.
		"arc", "arc/prod", "arc/2026/prod", "1999", "20260", "202",
		"2026", "2026/", "arc/2026", "arc/2026/",
	} {
		if cfg := load(t, prefix); len(cfg.Warnings) != 0 {
			t.Errorf("prefix %q warned: %+v", prefix, cfg.Warnings)
		}
	}

	// There is deliberately no "and an unset prefix is silent" case here.
	// t.Setenv holds for the whole test function, so a Load after the loop
	// still sees the last prefix it set — the assertion that used to sit here
	// read as a default-configuration check and was never one. The empty
	// prefix is covered by TestObjectPrefixKeysAreValidatedAtLoad.
}
