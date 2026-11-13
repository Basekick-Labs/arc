package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestMaxSQLLengthDefault(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	if got := v.GetInt("query.max_sql_length"); got != 10000 {
		t.Fatalf("query.max_sql_length default = %d, want 10000", got)
	}
}

func TestMaxSQLLengthFromEnv(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("ARC_QUERY_MAX_SQL_LENGTH", "250000")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Query.MaxSQLLength != 250000 {
		t.Fatalf("MaxSQLLength = %d, want 250000", cfg.Query.MaxSQLLength)
	}
}

func TestMaxSQLLengthRange(t *testing.T) {
	for _, tc := range []struct {
		value, payload string
		ok             bool
	}{
		{"1000", "", true},
		{"1048576", "", true},
		{"999", "", false},
		{"1048577", "", false},
		{"0", "", false},
		{"-1", "", false},
		{"20000", "16KB", false}, // the body limit is enforced first
		{"20000", "64KB", true},
	} {
		t.Run(tc.value+"/"+tc.payload, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_QUERY_MAX_SQL_LENGTH", tc.value)
			if tc.payload != "" {
				t.Setenv("ARC_SERVER_MAX_PAYLOAD_SIZE", tc.payload)
			}
			_, err := Load()
			if tc.ok && err != nil {
				t.Fatalf("refused: %v", err)
			}
			if !tc.ok && (err == nil || !strings.Contains(err.Error(), "query.max_sql_length")) {
				t.Fatalf("accepted or wrong error: %v", err)
			}
		})
	}
}
