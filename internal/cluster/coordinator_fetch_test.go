package cluster

import (
	"strings"
	"testing"
)

// TestSanitizeFetchPath exercises the path validator used by the fetch
// handler to reject malformed or malicious path arguments from peers.
// The broader handler integration is covered by the in-process integration
// test (filereplication_integration_test.go).
func TestSanitizeFetchPath(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantErr string // substring; empty if the path should be accepted
		want    string // cleaned output when accepted
	}{
		{
			name:  "valid relative path",
			input: "mydb/cpu/2026/04/11/14/file-xxx.parquet",
			want:  "mydb/cpu/2026/04/11/14/file-xxx.parquet",
		},
		{
			name:  "valid deep path",
			input: "prod/mem/2026/04/11/14/abcdef.parquet",
			want:  "prod/mem/2026/04/11/14/abcdef.parquet",
		},
		{
			name:    "empty path",
			input:   "",
			wantErr: "empty",
		},
		{
			name:    "absolute path",
			input:   "/etc/passwd",
			wantErr: "absolute",
		},
		{
			name:    "path traversal at start",
			input:   "../etc/passwd",
			wantErr: "traversal",
		},
		{
			name:    "path traversal embedded",
			input:   "mydb/cpu/../../etc/passwd",
			wantErr: "pre-cleaned",
		},
		{
			name:    "parent dir only",
			input:   "..",
			wantErr: "traversal",
		},
		{
			name:    "null byte",
			input:   "mydb/cpu/\x00file.parquet",
			wantErr: "null byte",
		},
		{
			name:    "non-canonical slashes",
			input:   "mydb//cpu///file.parquet",
			wantErr: "pre-cleaned",
		},
		{
			name:    "trailing slash",
			input:   "mydb/cpu/",
			wantErr: "pre-cleaned",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sanitizeFetchPath(tc.input)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil (cleaned=%q)", tc.wantErr, got)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("sanitized path: got %q, want %q", got, tc.want)
			}
		})
	}
}
