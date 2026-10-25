package api

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/stretchr/testify/require"
)

// lpTextBinary answers KnownTextBinaryColumns from a fixed map.
type lpTextBinary map[string]map[string]bool

func (lpTextBinary) EnsureFile(context.Context, string, string, *arrow.Schema, []string, string) error {
	return nil
}

func (r lpTextBinary) KnownTextBinaryColumns(database, measurement string) map[string]bool {
	return r[database+"/"+measurement]
}

// A line protocol write with a string for a binary column is refused with
// 400 as a whole: the other measurements of the request are not written
// first (#440). Both the typed path and the record path (decimal columns).
func TestLPWriteColumnTypeChangeRefusesTheWholeRequest(t *testing.T) {
	for name, cfg := range map[string]*config.IngestConfig{
		"typed":  nil,
		"record": {DecimalColumns: []string{"other:price=18,4"}},
	} {
		t.Run(name, func(t *testing.T) {
			r := newLPWriteRig(t, cfg)
			r.buf.SetFieldSchema(lpTextBinary{"d/geo": {"geom": true}})
			body := "a v=1 1760000000000000000\nb v=2 1760000000000000000\ngeo geom=\"POINT(1 2)\" 1760000000000000000\nc v=3 1760000000000000000\n"
			status, out := r.post(t, body)
			require.Equal(t, 400, status, out)
			require.Contains(t, out, "column type change refused")
			rows, _ := r.stored(t)
			require.Empty(t, rows, "a refused request wrote measurements")
			require.NotContains(t, r.log.String(), "Failed to write to Arrow buffer", "a client error was logged as a write failure")
		})
	}
}
