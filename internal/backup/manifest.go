package backup

import (
	"encoding/json"
	"fmt"
	"time"
)

// Manifest describes the contents of a backup.
type Manifest struct {
	Version        string         `json:"version"`
	BackupID       string         `json:"backup_id"`
	CreatedAt      time.Time      `json:"created_at"`
	BackupType     string         `json:"backup_type"` // "full" (future: "incremental")
	Databases      []DatabaseInfo `json:"databases"`
	TotalFiles     int64          `json:"total_files"`
	TotalSizeBytes int64          `json:"total_size_bytes"`
	HasMetadata    bool           `json:"has_metadata"`
	HasConfig      bool           `json:"has_config"`
}

// DatabaseInfo describes a single database within a backup.
type DatabaseInfo struct {
	Name         string            `json:"name"`
	Measurements []MeasurementInfo `json:"measurements"`
	FileCount    int               `json:"file_count"`
	SizeBytes    int64             `json:"size_bytes"`
}

// MeasurementInfo describes a single measurement within a database backup.
type MeasurementInfo struct {
	Name      string `json:"name"`
	FileCount int    `json:"file_count"`
	SizeBytes int64  `json:"size_bytes"`
}

// BackupSummary is a compact representation of a backup for listing.
type BackupSummary struct {
	BackupID      string    `json:"backup_id"`
	CreatedAt     time.Time `json:"created_at"`
	BackupType    string    `json:"backup_type"`
	TotalFiles    int64     `json:"total_files"`
	TotalBytes    int64     `json:"total_size_bytes"`
	DatabaseCount int       `json:"database_count"`
}

// Progress tracks the state of a running backup or restore operation.
type Progress struct {
	Operation      string     `json:"operation"` // "backup" or "restore"
	BackupID       string     `json:"backup_id"`
	Status         string     `json:"status"` // "running", "completed", "failed"
	TotalFiles     int64      `json:"total_files"`
	ProcessedFiles int64      `json:"processed_files"`
	TotalBytes     int64      `json:"total_bytes"`
	ProcessedBytes int64      `json:"processed_bytes"`
	StartedAt      time.Time  `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	Error          string     `json:"error,omitempty"`
}

// MarshalManifest serializes a manifest to JSON.
func MarshalManifest(m *Manifest) ([]byte, error) {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest: %w", err)
	}
	return data, nil
}

// UnmarshalManifest deserializes a manifest from JSON.
func UnmarshalManifest(data []byte) (*Manifest, error) {
	m := &Manifest{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest: %w", err)
	}
	return m, nil
}

// SummaryFromManifest creates a compact summary from a full manifest.
func SummaryFromManifest(m *Manifest) BackupSummary {
	return BackupSummary{
		BackupID:      m.BackupID,
		CreatedAt:     m.CreatedAt,
		BackupType:    m.BackupType,
		TotalFiles:    m.TotalFiles,
		TotalBytes:    m.TotalSizeBytes,
		DatabaseCount: len(m.Databases),
	}
}
