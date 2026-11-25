package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestPrintBootstrapBanner_TokenValue(t *testing.T) {
	const secret = "arc_0123456789abcdef0123456789abcdef0123456789ab"

	tests := []struct {
		name       string
		configured bool
		force      bool
		wantValue  bool     // the token value appears in the output
		want       []string // lines that must appear
		notWant    []string // lines that must not appear
	}{
		{
			name:      "generated token is printed",
			wantValue: true,
			want:      []string{"FIRST RUN - INITIAL ADMIN TOKEN GENERATED", "Admin API token: ", "SAVE THIS TOKEN"},
		},
		{
			name:       "configured token is not printed",
			configured: true,
			want:       []string{"INITIAL ADMIN TOKEN INSTALLED FROM CONFIGURATION", "ARC_AUTH_BOOTSTRAP_TOKEN"},
			notWant:    []string{"FIRST RUN", "Admin API token", "SAVE THIS TOKEN"},
		},
		{
			name:       "configured recovery token is not printed",
			configured: true,
			force:      true,
			want:       []string{"RECOVERY TOKEN ADDED", "Remove ARC_AUTH_FORCE_BOOTSTRAP after recovery."},
			notWant:    []string{"FIRST RUN", "Admin API token", "SAVE THIS TOKEN"},
		},
		{
			name:      "generated recovery token is printed",
			force:     true,
			wantValue: true,
			want:      []string{"RECOVERY TOKEN ADDED", "Admin API token: ", "Remove ARC_AUTH_FORCE_BOOTSTRAP after recovery."},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			printBootstrapBanner(&buf, secret, tt.configured, tt.force)
			out := buf.String()

			if got := strings.Contains(out, secret); got != tt.wantValue {
				t.Errorf("token value in output = %v, want %v\n%s", got, tt.wantValue, out)
			}
			for _, s := range tt.want {
				if !strings.Contains(out, s) {
					t.Errorf("output is missing %q\n%s", s, out)
				}
			}
			for _, s := range tt.notWant {
				if strings.Contains(out, s) {
					t.Errorf("output must not contain %q\n%s", s, out)
				}
			}
		})
	}
}
