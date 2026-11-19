package database

import "testing"

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
