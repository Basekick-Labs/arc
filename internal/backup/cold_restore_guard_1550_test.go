package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// #1550: a restore that may write to the cold tier tells the cold source
// before its first cold write and after it has recorded the rows, so the
// cold source stops trusting its cold rows across the restore.

type guardedColdSource struct {
	*fakeColdSource
	coldPath          string
	coldDir           string
	begins, ends      int
	coldExistedAtOpen bool
	recordsAtEnd      int32
	beginErr          error
}

func (g *guardedColdSource) BeginColdRestore() error {
	g.begins++
	if _, err := os.Stat(filepath.Join(g.coldDir, g.coldPath)); err == nil {
		g.coldExistedAtOpen = true
	}
	return g.beginErr
}

func (g *guardedColdSource) EndColdRestore() {
	g.ends++
	g.recordsAtEnd = g.records.Load()
}

func TestDataRestoreOpensAndClosesTheColdRestoreWindow(t *testing.T) {
	ctx := context.Background()
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
	g := &guardedColdSource{fakeColdSource: rig.cold, coldPath: coldPath, coldDir: rig.coldDir}
	rig.m.coldSource = g

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: result.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	if g.begins != 1 || g.ends != 1 {
		t.Fatalf("begin %d, end %d; want one of each", g.begins, g.ends)
	}
	if g.coldExistedAtOpen {
		t.Fatal("the window opened after the cold object was written")
	}
	if g.recordsAtEnd == 0 {
		t.Fatal("the window closed before the restored cold rows were recorded")
	}
}

// No cold tier on the restoring node: nothing can be written to cold, and
// the window is not opened.
func TestDataRestoreWithoutAColdTierOpensNoWindow(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	rig.writeCold(t, "prod/cpu/2026/01/01/00/cold.parquet", "PAR1cold")
	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	g := &guardedColdSource{fakeColdSource: &fakeColdSource{backend: nil}}
	rig.m.coldSource = g
	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: result.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	if g.begins != 0 {
		t.Fatalf("window opened %d times on a node with no cold tier", g.begins)
	}
}

// A cold source that cannot record that its rows may become incomplete
// refuses the restore before anything is written: a crash mid-restore would
// otherwise leave restored cold objects its rows vouch do not exist.
func TestDataRestoreRefusedWhenTheColdWindowCannotOpen(t *testing.T) {
	ctx := context.Background()
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
	g := &guardedColdSource{fakeColdSource: rig.cold, coldPath: coldPath, coldDir: rig.coldDir, beginErr: errors.New("disk full")}
	rig.m.coldSource = g

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: result.Manifest.BackupID, RestoreData: true}); err == nil {
		t.Fatal("RestoreBackup succeeded although the cold window could not open")
	}
	if g.ends != 0 {
		t.Fatalf("EndColdRestore called %d times for a window that never opened", g.ends)
	}
	if _, err := os.Stat(filepath.Join(rig.coldDir, coldPath)); !os.IsNotExist(err) {
		t.Fatalf("the cold object was restored anyway (stat: %v)", err)
	}
}
