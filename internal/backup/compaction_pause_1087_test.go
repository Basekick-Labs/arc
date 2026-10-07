package backup

// The cluster-wide compaction pause as a restore takes it (#1087): before its
// first manifest access in both modes, released after its last register and
// on every failure path, a refused pause fails the restore before anything is
// touched, and a pause lost mid-run fails it.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestRestore_PausesCompactionForEveryClusterRestoreIssue1087(t *testing.T) {
	for _, mode := range []string{RestoreModeMerge, RestoreModeReplace} {
		t.Run(mode, func(t *testing.T) {
			backupDir, backupID, keys, _ := seedClusterBackup(t, 3)
			dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
			const stale = "db/cpu/2026/09/13/00/stale.parquet"
			mustWrite(t, dest, stale, []byte("current"))
			cm := &fakeClusterManifest{entries: []ManifestFile{entryFor(stale, []byte("current"))}}
			m := newClusterManager(t, dest, backupDir, cm, nil)

			if _, err := m.RestoreBackup(context.Background(), RestoreOptions{BackupID: backupID, RestoreData: true, Mode: mode}); err != nil {
				t.Fatalf("RestoreBackup: %v", err)
			}
			p := m.GetProgress()
			if p.Status != "completed" || p.CompactionPause != "released" || p.FilesRegistered != int64(len(keys)) {
				t.Fatalf("status=%s compaction_pause=%s registered=%d, want completed released %d", p.Status, p.CompactionPause, p.FilesRegistered, len(keys))
			}
			ops := cm.pauseOps()
			if len(ops) < 3 || ops[0] != "pause" || ops[len(ops)-1] != "resume" {
				t.Fatalf("call order = %s, want the pause first and the resume last", strings.Join(ops, ","))
			}
			pauses, resumes := 0, 0
			for _, op := range ops {
				switch op {
				case "pause":
					pauses++
				case "resume":
					resumes++
				}
			}
			if pauses != 1 || resumes != 1 {
				t.Fatalf("call order = %s, want exactly one pause and one resume", strings.Join(ops, ","))
			}
			if mode == RestoreModeReplace && !strings.Contains(strings.Join(ops, ","), "pause,sync,read,delete") {
				t.Fatalf("call order = %s, want the delete half after the pause", strings.Join(ops, ","))
			}
			if len(cm.pauseReasons) != 1 || cm.pauseReasons[0] != "restore "+backupID {
				t.Fatalf("pause reasons = %v, want [restore %s]", cm.pauseReasons, backupID)
			}
		})
	}
}

// A refused pause fails the restore before the manifest is read or a byte is
// written, and the progress says where it stopped.
func TestRestore_PauseRefusedFailsBeforeAnyManifestAccessIssue1087(t *testing.T) {
	backupDir, backupID, keys, _ := seedClusterBackup(t, 2)
	dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	cm := &fakeClusterManifest{pauseErr: errors.New("compaction is already paused by writer-2 (restore b9) until 2026-10-06T10:02:00Z")}
	m := newClusterManager(t, dest, backupDir, cm, nil)

	_, err := m.RestoreBackup(context.Background(), RestoreOptions{BackupID: backupID, RestoreData: true, Mode: RestoreModeReplace})
	if err == nil || !strings.Contains(err.Error(), "restore refused: compaction could not be paused cluster-wide") || !strings.Contains(err.Error(), "already paused by writer-2") {
		t.Fatalf("err = %v", err)
	}
	p := m.GetProgress()
	if p.Status != "failed" || p.CompactionPause != "waiting" {
		t.Fatalf("status=%s compaction_pause=%s, want failed waiting", p.Status, p.CompactionPause)
	}
	if ops := cm.pauseOps(); strings.Join(ops, ",") != "pause" {
		t.Fatalf("call order = %v, want the refused pause and nothing else", ops)
	}
	if existsIn(t, dest, keys[0]) {
		t.Fatal("a file was written although the pause was refused")
	}
}

// The pause is released when the restore fails part-way.
func TestRestore_ReleasesThePauseWhenTheRestoreFailsIssue1087(t *testing.T) {
	backupDir, backupID, _, _ := seedClusterBackup(t, 2)
	dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	cm := &fakeClusterManifest{failRegisterCall: 1}
	m := newClusterManager(t, dest, backupDir, cm, nil)

	if _, err := m.RestoreBackup(context.Background(), RestoreOptions{BackupID: backupID, RestoreData: true}); err == nil {
		t.Fatal("a refused register batch did not fail the restore")
	}
	p := m.GetProgress()
	if p.Status != "failed" || p.CompactionPause != "released" {
		t.Fatalf("status=%s compaction_pause=%s, want failed released", p.Status, p.CompactionPause)
	}
	if ops := cm.pauseOps(); ops[len(ops)-1] != "resume" {
		t.Fatalf("call order = %v, want the resume last", ops)
	}
}

// A pause lost mid-run fails the restore at the next manifest batch, names
// the batch left unregistered, and is reported as lost, not released.
func TestRestore_LostPauseFailsTheRestoreIssue1087(t *testing.T) {
	t.Run("between register batches", func(t *testing.T) {
		setBatchCaps(t, 2, 1<<20) // 3 files, 2 batches
		backupDir, backupID, _, _ := seedClusterBackup(t, 3)
		dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
		cm := &fakeClusterManifest{loseAfterRegisters: 1}
		m := newClusterManager(t, dest, backupDir, cm, nil)

		_, err := m.RestoreBackup(context.Background(), RestoreOptions{BackupID: backupID, RestoreData: true})
		if err == nil || !strings.Contains(err.Error(), "compaction pause was lost") || !strings.Contains(err.Error(), "take a fresh backup") {
			t.Fatalf("err = %v", err)
		}
		p := m.GetProgress()
		if p.Status != "failed" || p.CompactionPause != "lost" {
			t.Fatalf("status=%s compaction_pause=%s, want failed lost", p.Status, p.CompactionPause)
		}
		if len(cm.registers) != 1 {
			t.Fatalf("register batches = %d, want 1 (the second must not be registered under a lost pause)", len(cm.registers))
		}
		if p.RegistrationFailed != 1 || len(p.RegistrationFailedSample) != 1 {
			t.Fatalf("registration_failed = %d sample = %v, want the one file of the second batch", p.RegistrationFailed, p.RegistrationFailedSample)
		}
		if ops := cm.pauseOps(); ops[len(ops)-1] != "resume" {
			t.Fatalf("call order = %v, want the resume last even when lost", ops)
		}
	})
	t.Run("before the final summary", func(t *testing.T) {
		backupDir, backupID, _, _ := seedClusterBackup(t, 2)
		dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
		cm := &fakeClusterManifest{loseAfterRegisters: 1} // one batch: lost only after the last register
		m := newClusterManager(t, dest, backupDir, cm, nil)

		_, err := m.RestoreBackup(context.Background(), RestoreOptions{BackupID: backupID, RestoreData: true})
		if err == nil || !strings.Contains(err.Error(), "compaction pause was lost") {
			t.Fatalf("err = %v", err)
		}
		p := m.GetProgress()
		if p.Status != "failed" || p.CompactionPause != "lost" || p.FilesRegistered != 2 {
			t.Fatalf("status=%s compaction_pause=%s registered=%d, want failed lost 2", p.Status, p.CompactionPause, p.FilesRegistered)
		}
	})
}

// A standalone node has no cluster hook and takes no pause; the progress
// field stays empty.
func TestRestore_StandaloneTakesNoPauseIssue1087(t *testing.T) {
	backupDir, backupID, _, _ := seedClusterBackup(t, 2)
	var dest storage.Backend = mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	m, err := NewManager(&ManagerConfig{DataStorage: dest, BackupPath: backupDir, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.RestoreBackup(context.Background(), RestoreOptions{BackupID: backupID, RestoreData: true}); err != nil {
		t.Fatal(err)
	}
	if p := m.GetProgress(); p.Status != "completed" || p.CompactionPause != "" {
		t.Fatalf("status=%s compaction_pause=%q, want completed and empty", p.Status, p.CompactionPause)
	}
}
