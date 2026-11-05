package config

import (
	"strings"
	"testing"
)

// #1468: ingest.max_buffered_bytes.
func TestLoad_MaxBufferedBytes(t *testing.T) {
	cases := []struct {
		env      string // "" = unset (the default)
		wantAuto bool
		want     int64
		errPart  string
	}{
		{env: "", wantAuto: true},
		{env: "auto", wantAuto: true},
		{env: " AUTO ", wantAuto: true},
		{env: "0"},
		{env: "0MB"},
		{env: "512MB", want: 512 << 20},
		{env: "2GB", want: 2 << 30},
		{env: "1MB", want: 1 << 20},
		{env: "1KB", errPart: "at least 1MB"},
		{env: "1023KB", errPart: "at least 1MB"},
		{env: "lots", errPart: "max_buffered_bytes"},
		{env: "-1GB", errPart: "max_buffered_bytes"},
	}
	for _, tc := range cases {
		t.Run(tc.env, func(t *testing.T) {
			t.Chdir(t.TempDir())
			if tc.env != "" {
				t.Setenv("ARC_INGEST_MAX_BUFFERED_BYTES", tc.env)
			}
			cfg, err := Load()
			if tc.errPart != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errPart) {
					t.Fatalf("err = %v, want one containing %q", err, tc.errPart)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Ingest.MaxBufferedBytesAuto != tc.wantAuto || cfg.Ingest.MaxBufferedBytes != tc.want {
				t.Fatalf("got auto %v, bytes %d; want auto %v, bytes %d",
					cfg.Ingest.MaxBufferedBytesAuto, cfg.Ingest.MaxBufferedBytes, tc.wantAuto, tc.want)
			}
		})
	}
}
