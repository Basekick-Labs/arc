package api

import (
	"context"
	"fmt"
)

// CombineOpenCompactions unions the sources a read must account for (the
// compaction manager's own jobs, the cluster's staged jobs and pending local
// deletes). Nil members are skipped; with none left the result is nil, so
// reads are untouched.
func CombineOpenCompactions(sources ...OpenCompactions) OpenCompactions {
	var live []OpenCompactions
	for _, s := range sources {
		if s != nil {
			live = append(live, s)
		}
	}
	switch len(live) {
	case 0:
		return nil
	case 1:
		return live[0]
	}
	return combinedOpenCompactions(live)
}

type combinedOpenCompactions []OpenCompactions

func (c combinedOpenCompactions) ExcludedFor(ctx context.Context, prefix string) []string {
	var out []string
	for _, s := range c {
		out = append(out, s.ExcludedFor(ctx, prefix)...)
	}
	return out
}

func (c combinedOpenCompactions) Open(prefix string) bool {
	for _, s := range c {
		if s.Open(prefix) {
			return true
		}
	}
	return false
}

func (c combinedOpenCompactions) Generation() uint64 {
	var g uint64
	for _, s := range c {
		g = g*1000003 + s.Generation()
	}
	return g
}

// StrictExcluder is an OpenCompactions source that can answer for a reader
// that must not guess (the Iceberg export): an error where ExcludedFor would
// count a file next to its replacement.
type StrictExcluder interface {
	ExcludedForStrict(ctx context.Context, prefix string) ([]string, error)
}

func (c combinedOpenCompactions) ExcludedForStrict(ctx context.Context, prefix string) ([]string, error) {
	var out []string
	for _, s := range c {
		ss, ok := s.(StrictExcluder)
		if !ok {
			return nil, fmt.Errorf("%T cannot name the files to leave out strictly", s)
		}
		keys, err := ss.ExcludedForStrict(ctx, prefix)
		if err != nil {
			return nil, err
		}
		out = append(out, keys...)
	}
	return out, nil
}
