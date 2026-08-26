package logger

import (
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// A fabricated DuckDB-shaped error: quoted value + LINE echo of the full query.
func TestSanitizeErrTextStripsQueryEchoAndValues(t *testing.T) {
	raw := "Conversion Error: Could not convert string 'ZZSENTINELZZ' to INT64\n" +
		"\nLINE 1: SELECT v FROM cpu WHERE host = 'ZZSENTINELZZ'\n" +
		"                                        ^"
	got := sanitizeErrText(raw)
	if strings.Contains(got, "ZZSENTINELZZ") {
		t.Fatalf("sentinel leaked: %q", got)
	}
	if strings.Contains(got, "LINE 1") || strings.Contains(got, "SELECT") {
		t.Fatalf("query echo survived: %q", got)
	}
	if !strings.Contains(got, "Could not convert string") {
		t.Fatalf("message class lost: %q", got)
	}
}

// The hook must actually be installed by Setup, and fire on Err(err).
func TestErrSanitizerInstalledBySetup(t *testing.T) {
	Setup("info", "json")
	var buf strings.Builder
	// zerolog's ErrorMarshalFunc is package-global; any logger picks it up.
	l := zerolog.New(&buf)
	l.Error().Err(errors.New("bad value 'ZZSENTINELZZ' here\nLINE 1: SELECT 'ZZSENTINELZZ'")).Msg("x")
	out := buf.String()
	if strings.Contains(out, "ZZSENTINELZZ") {
		t.Fatalf("Err(err) leaked the sentinel: %s", out)
	}
	if !strings.Contains(out, "bad value") {
		t.Fatalf("error text vanished entirely: %s", out)
	}
}
