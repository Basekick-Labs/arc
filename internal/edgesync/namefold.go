package edgesync

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// nameFoldProbes are the name pairs ProbeNameFolding writes and looks up:
// one per fold the registry's collision key applies. Case-sensitive APFS
// still normalizes, so neither pair stands in for the other.
var nameFoldProbes = []struct{ kind, write, lookup string }{
	{"case", "case-probe", "CASE-PROBE"},
	{"normalization", "norm-\u00e9", "norm-e\u0301"}, // NFC "\u00e9", then NFD "e" + combining acute
}

// NameFolds is which folds a filesystem applies to names (#740).
type NameFolds struct {
	Case          bool
	Normalization bool
}

// AllNameFolds is what a hub assumes when it cannot tell.
var AllNameFolds = NameFolds{Case: true, Normalization: true}

// Any reports whether any fold applies.
func (f NameFolds) Any() bool { return f.Case || f.Normalization }

// ProbeNameFolding reports which folds the filesystem under dir applies to
// names: letter case, Unicode normalization, both or neither (#740). macOS
// APFS does both by default, case-sensitive APFS only normalization, Windows
// NTFS case; Linux filesystems and object stores neither.
//
// It creates and removes a scratch directory under dir. On any error it
// answers AllNameFolds with the error: a hub that cannot tell must assume the
// collision is real.
func ProbeNameFolding(dir string) (NameFolds, error) {
	scratch, err := os.MkdirTemp(dir, ".arc-namefold-")
	if err != nil {
		return AllNameFolds, fmt.Errorf("edgesync: probe name folding in %s: %w", dir, err)
	}
	defer os.RemoveAll(scratch)

	var folds NameFolds
	for _, pair := range nameFoldProbes {
		if err := os.WriteFile(filepath.Join(scratch, pair.write), nil, 0o600); err != nil {
			return AllNameFolds, fmt.Errorf("edgesync: probe name folding in %s: %w", dir, err)
		}
		_, err := os.Stat(filepath.Join(scratch, pair.lookup))
		switch {
		case err == nil:
			switch pair.kind {
			case "case":
				folds.Case = true
			case "normalization":
				folds.Normalization = true
			}
		case !errors.Is(err, os.ErrNotExist):
			return AllNameFolds, fmt.Errorf("edgesync: probe name folding in %s: %w", dir, err)
		}
	}
	return folds, nil
}
