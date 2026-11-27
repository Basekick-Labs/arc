package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// #1552: a restore writes a cold object only inside the cold source's
// GuardColdWrite, which raises the cluster's cold mark first; refused, the
// object is not written and the restore fails.

type markingColdSource struct {
	*fakeColdSource
	coldDir      string
	keys         []string
	existedFirst bool
	refuse       error
}

func (g *markingColdSource) GuardColdWrite(_ context.Context, keys []string, write func() error) error {
	g.keys = append(g.keys, keys...)
	for _, k := range keys {
		if _, err := os.Stat(filepath.Join(g.coldDir, k)); err == nil {
			g.existedFirst = true
		}
	}
	if g.refuse != nil {
		return g.refuse
	}
	return write()
}

func TestRestoreGuardsItsColdWrites(t *testing.T) {
	ctx := context.Background()
	for _, refuse := range []error{nil, errors.New("no quorum")} {
		rig := newColdRig(t, nil)
		const coldPath = "prod/cpu/2026/01/01/00/cold.parquet"
		rig.writeCold(t, coldPath, "PAR1cold")
		result, err := rig.m.CreateBackup(ctx, BackupOptions{})
		if err != nil {
			t.Fatalf("CreateBackup: %v", err)
		}
		if err := os.RemoveAll(filepath.Join(rig.coldDir, "prod")); err != nil {
			t.Fatal(err)
		}
		g := &markingColdSource{fakeColdSource: rig.cold, coldDir: rig.coldDir, refuse: refuse}
		rig.m.coldSource = g

		_, err = rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: result.Manifest.BackupID, RestoreData: true})
		if len(g.keys) != 1 || g.keys[0] != coldPath || g.existedFirst {
			t.Fatalf("guarded keys %v, object existed before the guard: %v", g.keys, g.existedFirst)
		}
		_, statErr := os.Stat(filepath.Join(rig.coldDir, coldPath))
		if refuse == nil && (err != nil || statErr != nil) {
			t.Fatalf("restore %v, cold object %v", err, statErr)
		}
		if refuse != nil && (err == nil || statErr == nil) {
			t.Fatalf("refused mark: restore %v, cold object written %v", err, statErr == nil)
		}
	}
}
