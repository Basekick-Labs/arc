package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// noBucketYet answers Exists the way an S3 backend does for a bucket that
// has not been created: ErrStoreNotFound (#1242). Most S3-compatible stores
// create the bucket on the first write, so the write paths still work.
type noBucketYet struct{ storage.Backend }

func (noBucketYet) Exists(context.Context, string) (bool, error) {
	return false, fmt.Errorf("failed to check S3 object existence: bucket b: %w", storage.ErrStoreNotFound)
}

type unreachableExists struct{ storage.Backend }

func (unreachableExists) Exists(context.Context, string) (bool, error) {
	return false, errors.New("dial tcp 10.0.0.1:443: i/o timeout")
}

// The probe is a backup's first touch of a destination; a bucket the first
// copy will create has answered, a timeout has not.
func TestProbeDestinationAcceptsABucketNotCreatedYet(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close() })
	m := &Manager{logger: zerolog.Nop()}
	if err := m.probeDestination(context.Background(), backupTarget{backend: noBucketYet{local}, name: "t", remote: true}, "backup-x"); err != nil {
		t.Fatalf("probe of a bucket not created yet: %v", err)
	}
	if err := m.probeDestination(context.Background(), backupTarget{backend: unreachableExists{local}, name: "t", remote: true}, "backup-x"); err == nil {
		t.Fatal("probe of an unreachable destination succeeded")
	}
}

// A restore that routes a file to a cold bucket not created yet writes it
// there instead of refusing.
func TestRestoreToAColdBucketNotCreatedYet(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	const path = "prod/cpu/2026/01/01/00/data.parquet"
	const body = "PAR1payload"
	if err := rig.data.Write(ctx, path, []byte(body)); err != nil {
		t.Fatal(err)
	}
	b, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if err := os.Remove(filepath.Join(rig.dataDir, path)); err != nil {
		t.Fatal(err)
	}
	rig.cold.rows[path] = int64(len(body))
	real := rig.cold.backend
	rig.cold.backend = noBucketYet{real}

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: b.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup into a cold bucket not created yet: %v", err)
	}
	if got, err := real.Read(ctx, path); err != nil || string(got) != body {
		t.Fatalf("cold copy = %q, %v; want %q", got, err, body)
	}
}
