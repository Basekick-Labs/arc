package tiering

import "testing"

// notifyMigrationComplete must fire when files moved OR when orphan
// reconciliation deleted hot files (a zero-migration crash-recovery cycle
// still changes which globs match), and stay quiet on a no-op cycle.
func TestNotifyMigrationComplete(t *testing.T) {
	m := &Manager{}
	fired := 0
	m.SetOnMigrationComplete(func() { fired++ })

	m.notifyMigrationComplete(0, 0)
	if fired != 0 {
		t.Fatal("no-op cycle must not fire the callback")
	}
	m.notifyMigrationComplete(3, 0)
	if fired != 1 {
		t.Fatalf("migrated>0: fired = %d, want 1", fired)
	}
	m.notifyMigrationComplete(0, 2)
	if fired != 2 {
		t.Fatalf("orphansDeleted>0: fired = %d, want 2", fired)
	}

	var nilCallback *Manager = &Manager{}
	nilCallback.notifyMigrationComplete(1, 1) // must not panic
}
