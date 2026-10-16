package main

import (
	"context"
	"errors"
	"testing"

	"github.com/basekick-labs/arc/internal/tiering"
)

// The cold check fails safe: a metadata error answers cold (#1071).
func TestTierColdCheck(t *testing.T) {
	for name, tc := range map[string]struct {
		f    *tiering.FileMetadata
		err  error
		want bool
	}{
		"cold":   {&tiering.FileMetadata{Tier: tiering.TierCold}, nil, true},
		"hot":    {&tiering.FileMetadata{Tier: tiering.TierHot}, nil, false},
		"no row": {nil, nil, false},
		"error":  {nil, errors.New("db locked"), true},
	} {
		got := tierColdCheck(func(context.Context, string) (*tiering.FileMetadata, error) { return tc.f, tc.err })(context.Background(), "p")
		if got != tc.want {
			t.Fatalf("%s: %v", name, got)
		}
	}
}

// Without a coordinator edge sync holds nothing.
func TestEdgeSyncHoldsNeedACoordinator(t *testing.T) {
	if edgeSyncHoldPath(nil) != nil || edgeSyncHoldPaths(nil) != nil {
		t.Fatal("holds without a coordinator")
	}
}
