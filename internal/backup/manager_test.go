package backup

import (
	"testing"
	"time"
)

func TestMarshalUnmarshalManifest(t *testing.T) {
	original := &Manifest{
		Version:        "dev",
		BackupID:       "backup-20260202-150000-abcd1234",
		CreatedAt:      time.Date(2026, 2, 2, 15, 0, 0, 0, time.UTC),
		BackupType:     "full",
		TotalFiles:     42,
		TotalSizeBytes: 1024 * 1024 * 100,
		HasMetadata:    true,
		HasConfig:      true,
		Databases: []DatabaseInfo{
			{
				Name:      "production",
				FileCount: 30,
				SizeBytes: 1024 * 1024 * 80,
				Measurements: []MeasurementInfo{
					{Name: "cpu", FileCount: 20, SizeBytes: 1024 * 1024 * 60},
					{Name: "mem", FileCount: 10, SizeBytes: 1024 * 1024 * 20},
				},
			},
			{
				Name:      "staging",
				FileCount: 12,
				SizeBytes: 1024 * 1024 * 20,
				Measurements: []MeasurementInfo{
					{Name: "cpu", FileCount: 12, SizeBytes: 1024 * 1024 * 20},
				},
			},
		},
	}

	data, err := MarshalManifest(original)
	if err != nil {
		t.Fatalf("MarshalManifest failed: %v", err)
	}

	restored, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatalf("UnmarshalManifest failed: %v", err)
	}

	if restored.BackupID != original.BackupID {
		t.Errorf("BackupID mismatch: got %s, want %s", restored.BackupID, original.BackupID)
	}
	if restored.TotalFiles != original.TotalFiles {
		t.Errorf("TotalFiles mismatch: got %d, want %d", restored.TotalFiles, original.TotalFiles)
	}
	if restored.TotalSizeBytes != original.TotalSizeBytes {
		t.Errorf("TotalSizeBytes mismatch: got %d, want %d", restored.TotalSizeBytes, original.TotalSizeBytes)
	}
	if len(restored.Databases) != 2 {
		t.Fatalf("expected 2 databases, got %d", len(restored.Databases))
	}
	if restored.Databases[0].Measurements[0].Name != "cpu" {
		t.Errorf("expected first measurement 'cpu', got %s", restored.Databases[0].Measurements[0].Name)
	}
	if !restored.HasMetadata {
		t.Error("expected HasMetadata=true")
	}
	if !restored.HasConfig {
		t.Error("expected HasConfig=true")
	}
}

func TestSummaryFromManifest(t *testing.T) {
	m := &Manifest{
		BackupID:       "backup-test",
		CreatedAt:      time.Now(),
		BackupType:     "full",
		TotalFiles:     100,
		TotalSizeBytes: 5000,
		Databases: []DatabaseInfo{
			{Name: "db1"},
			{Name: "db2"},
			{Name: "db3"},
		},
	}

	s := SummaryFromManifest(m)
	if s.BackupID != "backup-test" {
		t.Errorf("expected backup-test, got %s", s.BackupID)
	}
	if s.DatabaseCount != 3 {
		t.Errorf("expected 3 databases, got %d", s.DatabaseCount)
	}
	if s.TotalFiles != 100 {
		t.Errorf("expected 100 files, got %d", s.TotalFiles)
	}
}

func TestGenerateBackupID(t *testing.T) {
	id := generateBackupID()
	if len(id) < 20 {
		t.Errorf("backup ID too short: %s", id)
	}
	if id[:7] != "backup-" {
		t.Errorf("expected backup- prefix, got %s", id)
	}

	// Should be unique
	id2 := generateBackupID()
	if id == id2 {
		t.Error("two generated IDs should not be equal")
	}
}

func TestParseDBMeasurement(t *testing.T) {
	tests := []struct {
		path    string
		wantDB  string
		wantM   string
	}{
		{"mydb/cpu/2026/01/30/14/cpu_123.parquet", "mydb", "cpu"},
		{"prod/memory/2026/02/01/00/memory_456.parquet", "prod", "memory"},
		{"singlepart", "singlepart", "unknown"},
	}

	for _, tt := range tests {
		db, m := parseDBMeasurement(tt.path)
		if db != tt.wantDB {
			t.Errorf("parseDBMeasurement(%s): db=%s, want %s", tt.path, db, tt.wantDB)
		}
		if m != tt.wantM {
			t.Errorf("parseDBMeasurement(%s): measurement=%s, want %s", tt.path, m, tt.wantM)
		}
	}
}

func TestUnmarshalManifest_Invalid(t *testing.T) {
	_, err := UnmarshalManifest([]byte("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}
