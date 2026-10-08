package replicaview

import (
	"errors"
	"fmt"
)

var ErrUnavailable = errors.New("replication query sources are unavailable")

type Retirement struct {
	Database, Measurement string
	Hour                  int64
	Coverage              Coverage
}

// SetCanonicalState installs one consistent manifest observation together with
// its retirement ledger. Replica publication remains independent; both take the
// same view lock so a flush cannot be lost while a manifest is reconciled.
func (v *View) SetCanonicalState(files []File, retirements []Retirement, blocked map[string]error) error {
	normalized := make([]File, len(files))
	seen := make(map[string]bool, len(files))
	for i, f := range files {
		var err error
		normalized[i], err = normalizeFile(f)
		if err != nil {
			return err
		}
		if f.Metadata.IsReplica() || seen[f.Path] {
			return fmt.Errorf("invalid canonical state")
		}
		seen[f.Path] = true
	}
	retired := make(map[partition]Coverage, len(retirements))
	for _, r := range retirements {
		coverage, err := Normalize(r.Coverage)
		if err != nil {
			return err
		}
		key := partition{r.Database, r.Measurement, r.Hour}
		retired[key] = Union(retired[key], coverage)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, f := range normalized {
		if old, exists := v.files[f.Path]; exists && old.SHA256 != f.SHA256 {
			return fmt.Errorf("canonical path changed version in place: %s", f.Path)
		}
	}
	for key, f := range v.files {
		if !f.Metadata.IsReplica() {
			v.removeFileLocked(key)
		}
	}
	for _, f := range normalized {
		v.addFileLocked(f)
	}
	v.retired = retired
	v.blocked = make(map[string]error, len(blocked))
	for key, err := range blocked {
		v.blocked[key] = err
	}
	v.unavailable = nil
	return nil
}

func MeasurementKey(database, measurement string) string { return database + "\x00" + measurement }

func (v *View) SetUnavailable(err error) { v.mu.Lock(); v.unavailable = err; v.mu.Unlock() }
