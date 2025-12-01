# Arc Storage Backend Enhancements - Implementation Plan

**Date:** 2024-12-01
**Branch:** `feature/storage-backend-enhancements`

## Overview

This plan covers implementing S3 delete operations, SHOW/LIST commands, retention policy execution, continuous query support, Azure Blob Storage backend, and Arrow writer row-to-columnar conversion, along with comprehensive test coverage.

---

## Phase 1: Backend Interface Extensions

**Goal:** Add extended interfaces to support directory listing and batch operations without breaking existing implementations.

### Files to Modify
- `internal/storage/backend.go` - Add new interfaces
- `internal/storage/local.go` - Implement extended interfaces
- `internal/storage/s3.go` - Implement extended interfaces

### Changes

Add interface composition in `backend.go`:
```go
// DirectoryLister lists immediate subdirectories at a prefix
type DirectoryLister interface {
    ListDirectories(ctx context.Context, prefix string) ([]string, error)
}

// BatchDeleter supports efficient batch deletion
type BatchDeleter interface {
    DeleteBatch(ctx context.Context, paths []string) error
}

// ObjectInfo provides metadata about objects
type ObjectInfo struct {
    Path         string
    Size         int64
    LastModified time.Time
}

// ObjectLister lists objects with metadata
type ObjectLister interface {
    ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error)
}
```

---

## Phase 2: S3 Delete Operations

**Goal:** Make DELETE SQL commands work with S3 backend.

### Files to Modify
- `internal/api/delete.go` - Refactor for backend interface

### Changes
1. Refactor `findAffectedFiles()` to use `storage.Backend.List()`
2. Add `getQueryPath()` method for DuckDB-compatible paths
3. Refactor `rewriteFileWithoutDeletedRows()` for S3
4. Use `storage.Backend.Delete()` instead of `os.Remove()`

---

## Phase 3: S3 SHOW/LIST Commands

**Goal:** Make SHOW DATABASES and SHOW TABLES work with S3 backend.

### Files to Modify
- `internal/api/query.go` - Refactor SHOW commands

---

## Phase 4: S3 Retention Policy Execution

**Goal:** Enable retention policies to delete old files from S3.

### Files to Modify
- `internal/api/retention.go` - Refactor for backend interfaces

---

## Phase 5: S3 Continuous Query Support

**Goal:** Enable continuous queries to read from and write to S3.

### Files to Modify
- `internal/api/continuous_query.go` - Fix path generation for S3

---

## Phase 6: Azure Blob Storage Backend

**Goal:** Add Azure Blob Storage as a storage backend with feature parity to S3.

### Authentication Methods
1. Connection String
2. SAS Token
3. Managed Identity

---

## Phase 7: Arrow Writer Row-to-Columnar Conversion

**Goal:** Implement row format handling in Arrow writer.

### Files to Modify
- `internal/ingest/arrow_writer.go` - Implement row-to-columnar conversion

---

## Phase 8: Test Coverage

### Unit Tests (Mock-Based)
- `internal/storage/local_test.go`
- `internal/storage/s3_test.go`
- `internal/storage/azure_test.go`
- `internal/api/delete_test.go`
- `internal/api/retention_test.go`
- `internal/ingest/arrow_writer_test.go`

---

## Implementation Order

| Order | Phase | Description |
|-------|-------|-------------|
| 1 | Phase 1 | Backend Interface Extensions |
| 2 | Phase 2 | S3 Delete Operations |
| 3 | Phase 3 | S3 SHOW/LIST Commands |
| 4 | Phase 4 | S3 Retention Policy Execution |
| 5 | Phase 5 | S3 Continuous Query Support |
| 6 | Phase 7 | Arrow Writer Row-to-Columnar |
| 7 | Phase 6 | Azure Blob Storage Backend |
| 8 | Phase 8 | Testing (ongoing throughout) |
