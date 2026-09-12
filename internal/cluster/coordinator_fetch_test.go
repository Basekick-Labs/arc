package cluster

import (
	"errors"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
)

// TestSanitizeFetchPath exercises the path validator used by the fetch
// handler to reject malformed or malicious path arguments from peers.
// The broader handler integration is covered by the in-process integration
// test (filereplication_integration_test.go).
//
// Rejections are asserted with errors.Is against storage.ErrInvalidPath rather
// than by message substring: the rule is now the shared key contract (#746),
// and pinning its wording here would make the contract's error text a public
// interface of the cluster package.
func TestSanitizeFetchPath(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantErr bool
		want    string // output when accepted
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
			// Dots inside a segment name one object on every backend and are
			// accepted by the contract, so they must be fetchable.
			name:  "dots inside a segment",
			input: "mydb/a..b/2026/04/11/14/f.parquet",
			want:  "mydb/a..b/2026/04/11/14/f.parquet",
		},
		{name: "empty path", input: "", wantErr: true},
		{name: "absolute path", input: "/etc/passwd", wantErr: true},
		{name: "path traversal at start", input: "../etc/passwd", wantErr: true},
		{name: "path traversal embedded", input: "mydb/cpu/../../etc/passwd", wantErr: true},
		{name: "parent dir only", input: "..", wantErr: true},
		{name: "current dir only", input: ".", wantErr: true},
		{name: "null byte", input: "mydb/cpu/\x00file.parquet", wantErr: true},
		{name: "non-canonical slashes", input: "mydb//cpu///file.parquet", wantErr: true},
		{name: "trailing slash", input: "mydb/cpu/", wantErr: true},

		// The four below were ACCEPTED before #746. The old validator was
		// believed to be stricter than the contract because it required
		// path.Clean(p) == p; it was the opposite, since Clean-idempotence is
		// implied by the contract and adds nothing to it.
		{name: "backslash separator", input: "mydb\\cpu/f.parquet", wantErr: true},
		{name: "single dot segment", input: "mydb/./cpu/f.parquet", wantErr: true},
		{name: "oversize segment", input: "mydb/" + strings.Repeat("a", 256) + "/f.parquet", wantErr: true},
		{name: "oversize key", input: strings.Repeat("ab/", 400) + "f.parquet", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sanitizeFetchPath(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected a rejection, got nil (path=%q)", got)
				}
				if !errors.Is(err, storage.ErrInvalidPath) {
					t.Errorf("error %v does not wrap storage.ErrInvalidPath", err)
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
