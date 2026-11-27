package raft

import (
	"encoding/json"
	"strings"
	"testing"
)

// #1731: the database-drop notice validates its name and calls the observer.
func TestIcebergDatabaseDroppedNotice(t *testing.T) {
	for _, bad := range []string{"", ".", "..", "a/b", `a\b`, "a\x00b", strings.Repeat("x", MaxIcebergNoticeKeyLen+1)} {
		if ValidateIcebergDatabaseDropped(IcebergDatabaseDroppedPayload{Database: bad}) == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
	f := &ClusterFSM{}
	var got []string
	f.SetIcebergDatabaseDroppedCallback(func(db string) { got = append(got, db) })
	raw, _ := json.Marshal(IcebergDatabaseDroppedPayload{Database: "dropme"})
	if r := f.applyIcebergDatabaseDropped(raw); r != nil {
		t.Fatalf("apply = %v", r)
	}
	bad, _ := json.Marshal(IcebergDatabaseDroppedPayload{Database: "../x"})
	if r := f.applyIcebergDatabaseDropped(bad); r == nil {
		t.Fatal("an invalid notice applied")
	}
	if len(got) != 1 || got[0] != "dropme" {
		t.Fatalf("observer got %v", got)
	}
}
