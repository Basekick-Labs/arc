package compaction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (m *Manager) writeRecoveredOutputWrittenManifest(ctx context.Context, manifest *Manifest) error {
	if err := validateJobID(manifest.JobID); err != nil {
		return fmt.Errorf("invalid recovery job ID: %w", err)
	}
	completionPath := filepath.Join(m.CompletionDir, manifest.JobID+".json")
	completion, err := readCompletionManifest(completionPath)
	if err == nil {
		if completion.JobID != manifest.JobID {
			return fmt.Errorf("completion manifest job ID %q does not match recovery job %q", completion.JobID, manifest.JobID)
		}
		if completion.State == CompletionStateOutputWritten || completion.State == CompletionStateSourcesDeleted {
			matched := false
			for _, output := range completion.Outputs {
				if output.Path == manifest.OutputPath {
					if output.SizeBytes != manifest.OutputSize {
						return fmt.Errorf("completion manifest for job %q records output size %d, recovery manifest records %d", manifest.JobID, output.SizeBytes, manifest.OutputSize)
					}
					matched = true
					break
				}
			}
			if !matched {
				return fmt.Errorf("completion manifest for job %q does not include output %q", manifest.JobID, manifest.OutputPath)
			}
			if completion.State == CompletionStateSourcesDeleted && !sameStrings(completion.DeletedSources, manifest.InputFiles) {
				return fmt.Errorf("completion manifest for job %q records different deleted sources", manifest.JobID)
			}
			return nil
		}
		if completion.State != CompletionStateWritingOutput {
			return fmt.Errorf("completion manifest for job %q has unknown state %q", manifest.JobID, completion.State)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	partitionTime, err := recoveryPartitionTime(manifest)
	if err != nil {
		return err
	}
	hasher := sha256.New()
	var bytesRead byteCounter
	if err := m.StorageBackend.ReadTo(ctx, manifest.OutputPath, io.MultiWriter(hasher, &bytesRead)); err != nil {
		return fmt.Errorf("hash recovered compaction output %q: %w", manifest.OutputPath, err)
	}
	if int64(bytesRead) != manifest.OutputSize {
		return fmt.Errorf("recovered output %q has %d bytes, manifest records %d", manifest.OutputPath, bytesRead, manifest.OutputSize)
	}

	now := time.Now().UTC()
	createdAt := manifest.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	if completion == nil {
		completion = &CompletionManifest{JobID: manifest.JobID, CreatedAt: createdAt}
	}
	completion.Database = canonicalManifestDatabase(manifest.Database)
	completion.Measurement = manifest.Measurement
	completion.PartitionPath = manifest.PartitionPath
	completion.Tier = manifest.Tier
	completion.State = CompletionStateOutputWritten
	completion.Outputs = []CompactedOutput{{
		Path:          manifest.OutputPath,
		SHA256:        hex.EncodeToString(hasher.Sum(nil)),
		SizeBytes:     manifest.OutputSize,
		Database:      canonicalManifestDatabase(manifest.Database),
		Measurement:   manifest.Measurement,
		PartitionTime: partitionTime,
		Tier:          manifest.Tier,
		CreatedAt:     createdAt,
	}}
	completion.DeletedSources = nil
	completion.UpdatedAt = now
	if err := writeCompletionManifest(m.CompletionDir, completion); err != nil {
		return fmt.Errorf("write recovered output completion manifest: %w", err)
	}
	return nil
}

func (m *Manager) writeRecoveredSourcesDeletedManifest(_ context.Context, manifest *Manifest) error {
	if err := validateJobID(manifest.JobID); err != nil {
		return fmt.Errorf("invalid recovery job ID: %w", err)
	}
	completionPath := filepath.Join(m.CompletionDir, manifest.JobID+".json")
	completion, err := readCompletionManifest(completionPath)
	if err != nil {
		return fmt.Errorf("read output completion manifest before recording deleted sources: %w", err)
	}
	if completion.JobID != manifest.JobID {
		return fmt.Errorf("completion manifest job ID %q does not match recovery job %q", completion.JobID, manifest.JobID)
	}
	if completion.State == CompletionStateSourcesDeleted {
		if !sameStrings(completion.DeletedSources, manifest.InputFiles) {
			return fmt.Errorf("completion manifest for job %q records different deleted sources", manifest.JobID)
		}
		return nil
	}
	if completion.State != CompletionStateOutputWritten {
		return fmt.Errorf("completion manifest for job %q is in state %q after source deletion", manifest.JobID, completion.State)
	}
	completion.State = CompletionStateSourcesDeleted
	completion.DeletedSources = append([]string(nil), manifest.InputFiles...)
	completion.UpdatedAt = time.Now().UTC()
	if err := writeCompletionManifest(m.CompletionDir, completion); err != nil {
		return fmt.Errorf("write deleted-sources completion manifest: %w", err)
	}
	return nil
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func recoveryPartitionTime(manifest *Manifest) (time.Time, error) {
	if !manifest.PartitionTime.IsZero() {
		return manifest.PartitionTime.UTC(), nil
	}
	parts := strings.Split(strings.Trim(filepath.ToSlash(manifest.PartitionPath), "/"), "/")
	var layout string
	var partCount int
	switch manifest.Tier {
	case "hourly":
		layout, partCount = "2006/01/02/15", 4
	case "daily":
		layout, partCount = "2006/01/02", 3
	default:
		return time.Time{}, fmt.Errorf("cannot derive partition time for tier %q", manifest.Tier)
	}
	if len(parts) < partCount {
		return time.Time{}, fmt.Errorf("cannot derive partition time from path %q", manifest.PartitionPath)
	}
	partitionTime, err := time.Parse(layout, strings.Join(parts[len(parts)-partCount:], "/"))
	if err != nil {
		return time.Time{}, fmt.Errorf("parse partition time from path %q: %w", manifest.PartitionPath, err)
	}
	return partitionTime, nil
}

type byteCounter int64

func (counter *byteCounter) Write(p []byte) (int, error) {
	*counter += byteCounter(len(p))
	return len(p), nil
}
