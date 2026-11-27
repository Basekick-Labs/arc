package config

import (
	"reflect"
	"testing"
)

// TestListKeysReadEverySpelling pins #1680: edge_sync.*.allowed_dirs was read
// with v.GetStringSlice, which splits a string on whitespace only, so a path
// containing a space became separate allow-list entries, one of them resolved
// against the working directory. sort_keys/decimal_columns keep their
// whitespace entry separator (each entry contains commas) and are pinned here
// so nobody "fixes" them to the comma rule. compaction.exclude_databases is
// pinned by exclude_databases_test.go: a comma there is deliberately kept
// whole, because a spoke ID may contain one (#940).
func TestListKeysReadEverySpelling(t *testing.T) {
	cases := []struct {
		name   string
		toml   string
		envKey string
		envVal string
		get    func(*Config) []string
		want   []string
	}{
		// edge_sync.*.allowed_dirs: comma only on a string; spaces belong to the path.
		{name: "import dirs: default", get: importDirs, want: nil},
		{name: "import dirs: env path with a space", envKey: "ARC_EDGE_SYNC_IMPORT_ALLOWED_DIRS", envVal: "/Volumes/Air Gap", get: importDirs, want: []string{"/Volumes/Air Gap"}},
		{name: "import dirs: env comma list", envKey: "ARC_EDGE_SYNC_IMPORT_ALLOWED_DIRS", envVal: "/Volumes/Air Gap, /mnt/bundles", get: importDirs, want: []string{"/Volumes/Air Gap", "/mnt/bundles"}},
		{name: "import dirs: TOML array", toml: "[edge_sync.import]\nallowed_dirs = [\"/Volumes/Air Gap\", \"/mnt/b\"]\n", get: importDirs, want: []string{"/Volumes/Air Gap", "/mnt/b"}},
		{name: "import dirs: TOML string comma", toml: "[edge_sync.import]\nallowed_dirs = \"/a,/b\"\n", get: importDirs, want: []string{"/a", "/b"}},
		{name: "bundle dirs: env path with a space", envKey: "ARC_EDGE_SYNC_SPOKE_BUNDLE_ALLOWED_DIRS", envVal: "/Volumes/Air Gap,/mnt/x", get: bundleDirs, want: []string{"/Volumes/Air Gap", "/mnt/x"}},
		{name: "import dirs: TOML array element with a comma kept whole", toml: "[edge_sync.import]\nallowed_dirs = [\"/mnt/a,b\"]\n", get: importDirs, want: []string{"/mnt/a,b"}},
		{name: "import dirs: trailing comma", envKey: "ARC_EDGE_SYNC_IMPORT_ALLOWED_DIRS", envVal: "/a,/b,", get: importDirs, want: []string{"/a", "/b"}},
		{name: "bundle dirs: TOML array", toml: "[edge_sync.spoke.bundle]\nallowed_dirs = [\"/Volumes/Air Gap\"]\n", get: bundleDirs, want: []string{"/Volumes/Air Gap"}},

		// ingest.sort_keys / decimal_columns: unchanged. Entries contain
		// commas, so whitespace is the only entry separator a string can use.
		{name: "sort keys: env one entry", envKey: "ARC_INGEST_SORT_KEYS", envVal: "cpu:host,region", get: sortKeys, want: []string{"cpu:host,region"}},
		{name: "sort keys: env two entries", envKey: "ARC_INGEST_SORT_KEYS", envVal: "cpu:host,region mem:host", get: sortKeys, want: []string{"cpu:host,region", "mem:host"}},
		{name: "sort keys: TOML array", toml: "[ingest]\nsort_keys = [\"cpu:host,region\", \"mem:host\"]\n", get: sortKeys, want: []string{"cpu:host,region", "mem:host"}},
		{name: "decimal columns: env two entries", envKey: "ARC_INGEST_DECIMAL_COLUMNS", envVal: "trades:price=18,4;qty=10,2 fx:rate=12,6", get: decimalCols, want: []string{"trades:price=18,4;qty=10,2", "fx:rate=12,6"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeArcToml(t, tc.toml)
			if tc.envKey != "" {
				t.Setenv(tc.envKey, tc.envVal)
			}
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load(): %v", err)
			}
			got := tc.get(cfg)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func importDirs(c *Config) []string  { return c.EdgeSync.Import.AllowedDirs }
func bundleDirs(c *Config) []string  { return c.EdgeSync.Spoke.Bundle.AllowedDirs }
func sortKeys(c *Config) []string    { return c.Ingest.SortKeys }
func decimalCols(c *Config) []string { return c.Ingest.DecimalColumns }

// TestAllowedDirsRefuseRelativeEntries: a relative allowed_dirs entry would be
// resolved against Arc's working directory, and the realistic way to get one
// is a split in the wrong place, e.g. a comma inside a path in the string
// spelling. Startup must refuse it and name the fragment.
func TestAllowedDirsRefuseRelativeEntries(t *testing.T) {
	cases := []struct {
		name, toml, envKey, envVal, want string
	}{
		{
			name:   "import: comma inside a path in the env spelling",
			toml:   "[edge_sync]\nhub_id = \"hub1\"\n[edge_sync.import]\nenabled = true\n",
			envKey: "ARC_EDGE_SYNC_IMPORT_ALLOWED_DIRS", envVal: "/mnt/a,b",
			want: `edge_sync.import.allowed_dirs: "b" is not an absolute path`,
		},
		{
			name: "import: relative entry in the TOML array",
			toml: "[edge_sync]\nhub_id = \"hub1\"\n[edge_sync.import]\nenabled = true\nallowed_dirs = [\"bundles\"]\n",
			want: `edge_sync.import.allowed_dirs: "bundles" is not an absolute path`,
		},
		{
			name: "bundle: relative entry",
			toml: "[edge_sync.spoke]\nspoke_id = \"s1\"\nhub_id = \"hub1\"\n[edge_sync.spoke.bundle]\nenabled = true\nallowed_dirs = [\"/mnt/ok\", \"./out\"]\n",
			want: `edge_sync.spoke.bundle.allowed_dirs: "./out" is not an absolute path`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeArcToml(t, tc.toml)
			if tc.envKey != "" {
				t.Setenv(tc.envKey, tc.envVal)
			}
			t.Setenv("ARC_EDGE_SYNC_SPOKE_SECRET", "test-secret")
			mustLoadError(t, tc.want)
		})
	}
}
