package raft

import (
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
)

// TestManifestReachableKeysAreStorageRejected defines, and pins, the exact set
// of storage keys that can enter the cluster manifest and then fail every
// storage call made with them.
//
// ValidateManifestPath is deliberately looser than storage.ValidateKey and must
// stay that way: it runs inside FSM Apply, which includes log replay, so
// tightening it makes a node reject an entry an older binary accepted and two
// versions build different state from one log. The consequence is this gap, and
// #747 closes it at the consumers rather than here.
//
// This test is the gap's definition. Every quarantine branch added for #747 is
// written against these six spellings, and the table in
// internal/storage/invalid_path_contract_test.go mirrors them. If someone
// later tightens ValidateManifestPath, this test fails and names exactly which
// consumer branches became unreachable, instead of leaving dead code behind.
func TestManifestReachableKeysAreStorageRejected(t *testing.T) {
	reachable := []struct {
		name string
		key  string
	}{
		{"trailing separator", "testdb/cpu/2026/04/11/14/"},
		{"dot segment", "testdb/./cpu/2026/04/11/14/f.parquet"},
		{"empty interior segment", "testdb//cpu/2026/04/11/14/f.parquet"},
		{"backslash", `testdb\cpu/2026/04/11/14/f.parquet`},
		{"oversize segment", "testdb/" + strings.Repeat("m", storage.MaxKeySegmentLen+1) + "/f.parquet"},
		{"oversize key", "testdb/" + strings.Repeat(strings.Repeat("a", 200)+"/", 6) + "f.parquet"},
	}

	for _, tc := range reachable {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateManifestPath(tc.key); err != nil {
				t.Fatalf("ValidateManifestPath(%q) = %v; this key is no longer reachable, so the #747 consumer branches written for it are now dead code and should be revisited", tc.key, err)
			}
			if err := storage.ValidateKey(tc.key); err == nil {
				t.Fatalf("storage.ValidateKey(%q) now accepts this key, so it is no longer part of the gap", tc.key)
			}
		})
	}
}

// TestKeysRejectedByBothValidatorsAreUnreachable records the other half: these
// spellings are refused by ValidateManifestPath too, so they can never reach a
// cleanup or replication loop from the manifest.
//
// It exists because they are the obvious thing to reach for when writing a test
// for an "invalid path", and a consumer-side test built on one of them would be
// testing a code path no manifest entry can trigger.
func TestKeysRejectedByBothValidatorsAreUnreachable(t *testing.T) {
	for _, key := range []string{"", "/testdb/cpu/f.parquet", "testdb/../cpu/f.parquet"} {
		if err := ValidateManifestPath(key); err == nil {
			t.Errorf("ValidateManifestPath(%q) was accepted; the reachable set in TestManifestReachableKeysAreStorageRejected is now incomplete", key)
		}
		if err := storage.ValidateKey(key); err == nil {
			t.Errorf("storage.ValidateKey(%q) was accepted", key)
		}
	}
}
