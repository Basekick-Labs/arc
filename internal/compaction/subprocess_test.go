package compaction

import (
	"errors"
	"os/exec"
	"testing"
)

func TestClassifySubprocessError(t *testing.T) {
	tests := []struct {
		name            string
		err             error
		stderr          string
		wantRecoverable bool
		wantReason      string
	}{
		{
			name:            "nil error",
			err:             nil,
			stderr:          "",
			wantRecoverable: false,
			wantReason:      "",
		},
		{
			name:            "segmentation fault in error message",
			err:             errors.New("subprocess failed: signal: segmentation fault"),
			stderr:          "",
			wantRecoverable: true,
			wantReason:      "segfault",
		},
		{
			name:            "killed signal",
			err:             errors.New("subprocess failed: signal: killed"),
			stderr:          "",
			wantRecoverable: true,
			wantReason:      "killed",
		},
		{
			name:            "out of memory in stderr",
			err:             errors.New("subprocess failed"),
			stderr:          "Error: Out of memory while processing",
			wantRecoverable: true,
			wantReason:      "memory_error",
		},
		{
			name:            "cannot allocate in stderr",
			err:             errors.New("subprocess failed"),
			stderr:          "cannot allocate memory for buffer",
			wantRecoverable: true,
			wantReason:      "memory_error",
		},
		{
			name:            "memory allocation failed",
			err:             errors.New("subprocess failed"),
			stderr:          "Memory allocation failed in DuckDB",
			wantRecoverable: true,
			wantReason:      "memory_error",
		},
		{
			name:            "permission denied - not recoverable",
			err:             errors.New("subprocess failed"),
			stderr:          "Error: Permission denied: /data/file.parquet",
			wantRecoverable: false,
			wantReason:      "permanent_error",
		},
		{
			name:            "no such file - not recoverable",
			err:             errors.New("subprocess failed"),
			stderr:          "Error: No such file or directory",
			wantRecoverable: false,
			wantReason:      "permanent_error",
		},
		{
			name:            "access denied - not recoverable",
			err:             errors.New("subprocess failed"),
			stderr:          "Error: Access denied to bucket",
			wantRecoverable: false,
			wantReason:      "permanent_error",
		},
		{
			name:            "file not found - not recoverable",
			err:             errors.New("subprocess failed"),
			stderr:          "file not found: data.parquet",
			wantRecoverable: false,
			wantReason:      "permanent_error",
		},
		{
			name:            "unknown error - default recoverable",
			err:             errors.New("some unknown error occurred"),
			stderr:          "some log output",
			wantRecoverable: true,
			wantReason:      "unknown",
		},
		{
			// Regression: the trades partition (#no-time-column) failed with a
			// Binder Error via the subprocess JSON result (exit 0, success=false),
			// so the error text is in err, not stderr. Pre-fix this classified as
			// "unknown → recoverable" and the adaptive splitter walked the whole
			// 30→15→7→3 ladder re-downloading files on a deterministic failure.
			name:            "duckdb binder error in err - not recoverable",
			err:             errors.New(`failed to compact files: failed to execute compaction query: Binder Error: Column "time" in REPLACE list not found in FROM clause`),
			stderr:          "",
			wantRecoverable: false,
			wantReason:      "sql_error",
		},
		{
			name:            "duckdb catalog error in err - not recoverable",
			err:             errors.New("failed to execute compaction query: Catalog Error: Table with name x does not exist"),
			stderr:          "",
			wantRecoverable: false,
			wantReason:      "sql_error",
		},
		{
			name:            "duckdb parser error in err - not recoverable",
			err:             errors.New("failed to execute compaction query: Parser Error: syntax error at or near \"FROM\""),
			stderr:          "",
			wantRecoverable: false,
			wantReason:      "sql_error",
		},
		{
			name:            "duckdb binder error in stderr - not recoverable",
			err:             errors.New("subprocess failed: exit status 1"),
			stderr:          `{"level":"error","error":"Binder Error: Column \"time\" in REPLACE list not found in FROM clause"}`,
			wantRecoverable: false,
			wantReason:      "sql_error",
		},
		{
			// DuckDB's own OOM surfaces through the same JSON-result path as the
			// binder error. It must STAY recoverable (retrying a smaller batch is
			// exactly the right response) and must not be caught by the sql_error
			// class.
			name:            "duckdb out of memory in err - recoverable",
			err:             errors.New("failed to execute compaction query: Out of Memory Error: could not allocate block"),
			stderr:          "",
			wantRecoverable: true,
			wantReason:      "memory_error",
		},
		{
			// A crashed subprocess embeds its whole stderr in the error string,
			// and that stderr may quote SQL error text in warn lines logged
			// before the crash. Process-level evidence (signal, OOM exit code)
			// must win: the crash is memory-dependent and retryable even though
			// an sql_error marker is present.
			name:            "crash evidence wins over sql marker in embedded stderr",
			err:             errors.New(`subprocess failed: signal: killed (stderr: {"level":"warn","message":"probe failed: Binder Error: something"})`),
			stderr:          `{"level":"warn","message":"probe failed: Binder Error: something"}`,
			wantRecoverable: true,
			wantReason:      "killed",
		},
		{
			name:            "case insensitive stderr matching",
			err:             errors.New("failed"),
			stderr:          "PERMISSION DENIED",
			wantRecoverable: false,
			wantReason:      "permanent_error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRecoverable, gotReason := ClassifySubprocessError(tt.err, tt.stderr)

			if gotRecoverable != tt.wantRecoverable {
				t.Errorf("ClassifySubprocessError() recoverable = %v, want %v", gotRecoverable, tt.wantRecoverable)
			}
			if gotReason != tt.wantReason {
				t.Errorf("ClassifySubprocessError() reason = %v, want %v", gotReason, tt.wantReason)
			}
		})
	}
}

// TestClassifySubprocessError_ExitCodes tests exit code classification
func TestClassifySubprocessError_ExitCodes(t *testing.T) {
	// We can't easily create exec.ExitError with specific codes in tests,
	// but we can verify the error message parsing works correctly.
	tests := []struct {
		name            string
		errMsg          string
		stderr          string
		wantRecoverable bool
	}{
		{
			name:            "signal segfault",
			errMsg:          "exit status 139",
			stderr:          "",
			wantRecoverable: true, // Unknown but recoverable by default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errMsg)
			gotRecoverable, _ := ClassifySubprocessError(err, tt.stderr)

			if gotRecoverable != tt.wantRecoverable {
				t.Errorf("ClassifySubprocessError() recoverable = %v, want %v", gotRecoverable, tt.wantRecoverable)
			}
		})
	}
}

// mockExitError creates a mock exec.ExitError for testing
// Note: This is a simplified test since we can't easily mock exec.ExitError
func TestClassifySubprocessError_WithRealExitError(t *testing.T) {
	// Run a command that will fail with a known exit code
	cmd := exec.Command("sh", "-c", "exit 137")
	err := cmd.Run()

	if exitErr, ok := err.(*exec.ExitError); ok {
		recoverable, reason := ClassifySubprocessError(exitErr, "")
		if !recoverable {
			t.Errorf("Exit code 137 (OOM kill) should be recoverable, got recoverable=%v, reason=%s", recoverable, reason)
		}
		if reason != "oom_killed" {
			t.Errorf("Exit code 137 should have reason 'oom_killed', got %s", reason)
		}
	} else {
		t.Skipf("Could not get exec.ExitError from command: %v", err)
	}
}

func TestClassifySubprocessError_WithSIGSEGV(t *testing.T) {
	// Run a command that will fail with exit code 139 (128 + 11 = SIGSEGV)
	cmd := exec.Command("sh", "-c", "exit 139")
	err := cmd.Run()

	if exitErr, ok := err.(*exec.ExitError); ok {
		recoverable, reason := ClassifySubprocessError(exitErr, "")
		if !recoverable {
			t.Errorf("Exit code 139 (SIGSEGV) should be recoverable, got recoverable=%v, reason=%s", recoverable, reason)
		}
		if reason != "segfault" {
			t.Errorf("Exit code 139 should have reason 'segfault', got %s", reason)
		}
	} else {
		t.Skipf("Could not get exec.ExitError from command: %v", err)
	}
}
