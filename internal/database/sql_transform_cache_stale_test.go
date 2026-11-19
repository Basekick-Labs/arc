package database

import (
	"fmt"
	"testing"
)

// An entry stored with a staleness check is a miss from the moment the check
// says so, and a hit again once it is stored afresh; an entry stored without
// one only expires.
func TestSQLTransformCacheStaleCheck(t *testing.T) {
	c := NewSQLTransformCache(0, 0)
	stale := false
	c.SetWithMetadataCheck("q", "transformed", []string{"m"}, func() bool { return stale })
	c.SetWithMetadata("plain", "plain-transformed", nil)

	if got, meta, ok := c.GetWithMetadata("q"); !ok || got != "transformed" || len(meta) != 1 || meta[0] != "m" {
		t.Fatalf("fresh entry: %q %v %v", got, meta, ok)
	}
	stale = true
	if _, _, ok := c.GetWithMetadata("q"); ok {
		t.Error("an entry whose check reports stale was served")
	}
	if got, ok := c.Get("plain"); !ok || got != "plain-transformed" {
		t.Errorf("an entry without a check was affected: %q %v", got, ok)
	}
	c.SetWithMetadataCheck("q", "again", nil, func() bool { return false })
	if got, _, ok := c.GetWithMetadata("q"); !ok || got != "again" {
		t.Errorf("stored afresh: %q %v", got, ok)
	}
}

// In a full shard, a stale entry can be replaced by a fresh one for the same
// statement, and stale entries make room for new statements. Otherwise a
// statement whose cached form went stale would be transformed again on every
// query until the entry expired.
func TestSQLTransformCacheStaleEntriesDoNotHoldAFullShard(t *testing.T) {
	c := NewSQLTransformCache(0, cacheShardCount) // one entry per shard
	stale := false
	c.SetWithMetadataCheck("q", "old", nil, func() bool { return stale })
	stale = true
	c.SetWithMetadataCheck("q", "new", nil, func() bool { return false })
	if got, ok := c.Get("q"); !ok || got != "new" {
		t.Errorf("replacing a stale entry in a full shard: %q %v, want the new transform", got, ok)
	}

	// A different statement in the same shard gets the room a stale entry held.
	stale = false
	c.SetWithMetadataCheck("q", "old", nil, func() bool { return stale })
	other := ""
	for i := 0; other == ""; i++ {
		if k := fmt.Sprintf("other-%d", i); c.shardFor(k) == c.shardFor("q") {
			other = k
		}
	}
	c.Set(other, "x")
	if _, ok := c.Get(other); ok {
		t.Fatal("the shard was not full: this test pins nothing")
	}
	stale = true
	c.Set(other, "x")
	if got, ok := c.Get(other); !ok || got != "x" {
		t.Errorf("a new statement was refused while a stale entry held the shard: %q %v", got, ok)
	}
}
