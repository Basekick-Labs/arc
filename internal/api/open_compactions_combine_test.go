package api

import (
	"context"
	"testing"
)

type fakeOpen struct {
	keys []string
	open bool
	gen  uint64
}

func (f *fakeOpen) ExcludedFor(context.Context, string) []string { return f.keys }
func (f *fakeOpen) Open(string) bool                             { return f.open }
func (f *fakeOpen) Generation() uint64                           { return f.gen }

func TestCombineOpenCompactions(t *testing.T) {
	if CombineOpenCompactions(nil, nil) != nil {
		t.Fatal("no sources should combine to nil")
	}
	a := &fakeOpen{keys: []string{"a"}}
	if got := CombineOpenCompactions(nil, a); got != OpenCompactions(a) {
		t.Fatal("a single source should be returned as is")
	}
	b := &fakeOpen{keys: []string{"b"}, open: true, gen: 7}
	c := CombineOpenCompactions(a, nil, b)
	if got := c.ExcludedFor(context.Background(), "db/m/"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("ExcludedFor = %v", got)
	}
	if !c.Open("db/m/") {
		t.Fatal("Open must be true when any source is open")
	}
	g := c.Generation()
	a.gen = 1
	if c.Generation() == g {
		t.Fatal("generation must change when any source's does")
	}
	b.open = false
	if c.Open("db/m/") {
		t.Fatal("Open with no source open")
	}
}
