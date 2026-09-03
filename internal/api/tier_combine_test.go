package api

// combineTierPruneResults rules (#686 follow-up): a verified-Empty tier may
// only be dropped on the strength of a positive Pruned result from another
// tier. Without one (spoke-namespace queries generate too-shallow paths that
// existence-filter to Empty; the other tier may be Fallback after a listing
// error) the Empty verdict must not hide a tier's data.

import (
	"testing"

	"github.com/basekick-labs/arc/internal/pruning"
)

func TestCombineTierPruneResults(t *testing.T) {
	hot := tierPruneResult{glob: "hot/**", paths: []string{"hot/h1", "hot/h2"}}
	cold := tierPruneResult{glob: "cold/**", paths: []string{"cold/d1"}}

	cases := []struct {
		name       string
		results    []tierPruneResult
		wantPaths  []string
		wantPruned int
	}{
		{
			name: "pruned plus empty drops the empty tier",
			results: []tierPruneResult{
				{glob: hot.glob, paths: hot.paths, outcome: pruning.TierPrunePruned},
				{glob: cold.glob, outcome: pruning.TierPruneEmpty},
			},
			wantPaths:  []string{"hot/h1", "hot/h2"},
			wantPruned: 2,
		},
		{
			name: "empty plus fallback keeps BOTH full globs",
			results: []tierPruneResult{
				{glob: hot.glob, outcome: pruning.TierPruneEmpty},
				{glob: cold.glob, outcome: pruning.TierPruneFallback},
			},
			wantPaths:  []string{"hot/**", "cold/**"},
			wantPruned: 0,
		},
		{
			name: "all empty falls back to full globs unpruned",
			results: []tierPruneResult{
				{glob: hot.glob, outcome: pruning.TierPruneEmpty},
				{glob: cold.glob, outcome: pruning.TierPruneEmpty},
			},
			wantPaths:  []string{"hot/**", "cold/**"},
			wantPruned: 0,
		},
		{
			name: "both pruned concatenates",
			results: []tierPruneResult{
				{glob: hot.glob, paths: hot.paths, outcome: pruning.TierPrunePruned},
				{glob: cold.glob, paths: cold.paths, outcome: pruning.TierPrunePruned},
			},
			wantPaths:  []string{"hot/h1", "hot/h2", "cold/d1"},
			wantPruned: 2,
		},
		{
			name: "all fallback keeps full globs",
			results: []tierPruneResult{
				{glob: hot.glob, outcome: pruning.TierPruneFallback},
				{glob: cold.glob, outcome: pruning.TierPruneFallback},
			},
			wantPaths:  []string{"hot/**", "cold/**"},
			wantPruned: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			paths, pruned := combineTierPruneResults(tc.results)
			if pruned != tc.wantPruned {
				t.Fatalf("prunedTiers = %d, want %d", pruned, tc.wantPruned)
			}
			if len(paths) != len(tc.wantPaths) {
				t.Fatalf("paths = %v, want %v", paths, tc.wantPaths)
			}
			for i := range paths {
				if paths[i] != tc.wantPaths[i] {
					t.Fatalf("paths[%d] = %q, want %q", i, paths[i], tc.wantPaths[i])
				}
			}
		})
	}
}
