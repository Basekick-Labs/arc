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

// ReplacementExcluder is a StrictExcluder that can also name, in the same
// answer, the compaction outputs whose inputs it leaves out
// (compaction.OpenJobs). The Iceberg export keeps exactly those outputs even
// while their crash-recovery manifests still list inputs on disk: the inputs
// are left out, so leaving the outputs out too would drop the job's rows from
// the table (#1722).
type ReplacementExcluder interface {
	ExcludedWithOutputsStrict(ctx context.Context, prefix string) (excluded, outputs []string, err error)
}

// ExcludedWithOutputsStrict unions the members' answers. A member that cannot
// name outputs contributes only the files it leaves out, which keeps every
// output of a job it knows out of the export, as before.
func (c combinedOpenCompactions) ExcludedWithOutputsStrict(ctx context.Context, prefix string) (excluded, outputs []string, err error) {
	for _, s := range c {
		if rs, ok := s.(ReplacementExcluder); ok {
			keys, outs, err := rs.ExcludedWithOutputsStrict(ctx, prefix)
			if err != nil {
				return nil, nil, err
			}
			excluded = append(excluded, keys...)
			outputs = append(outputs, outs...)
			continue
		}
		ss, ok := s.(StrictExcluder)
		if !ok {
			return nil, nil, fmt.Errorf("%T cannot name the files to leave out strictly", s)
		}
		keys, err := ss.ExcludedForStrict(ctx, prefix)
		if err != nil {
			return nil, nil, err
		}
		excluded = append(excluded, keys...)
	}
	return excluded, outputs, nil
}
