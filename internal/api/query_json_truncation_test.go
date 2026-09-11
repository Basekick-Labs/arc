//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// Tests for #723. A JSON query response opened with {"success":true,...} and
// closed the envelope whether or not the stream failed, so a client that was
// still connected received HTTP 200, valid JSON, success:true, and silently
// fewer rows than its query matched.

// truncatingScanner yields n rows and then reports a deferred error, the shape
// of a mid-stream failure (a timeout, or a row-fetch failure from *sql.Rows).
type truncatingScanner struct {
	rows int
	at   int
	err  error
}

func (s *truncatingScanner) Next() bool { return s.at < s.rows }
func (s *truncatingScanner) Scan(dest ...interface{}) error {
	s.at++
	if p, ok := dest[0].(*interface{}); ok {
		*p = int64(s.at)
	}
	return nil
}
func (s *truncatingScanner) Err() error { return s.err }

func streamJSONForTest(t *testing.T, sc *truncatingScanner) map[string]interface{} {
	t.Helper()
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	_, _ = streamTypedJSON(context.Background(), w, []string{"id"}, []colType{colInt64},
		sc, 0, nil, time.Now(), "2026-09-11T00:00:00Z")
	w.Flush()

	var doc map[string]interface{}
	// Unmarshal, not a substring match: the whole promise of this shape is
	// that the document still parses.
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("response is not valid JSON: %v\nbody: %s", err, buf.String())
	}
	return doc
}

func TestStreamTypedJSONMarksTruncation(t *testing.T) {
	t.Run("a complete stream carries no marker", func(t *testing.T) {
		doc := streamJSONForTest(t, &truncatingScanner{rows: 3})
		if _, ok := doc["truncated"]; ok {
			t.Error("a successful response must not carry the truncation marker")
		}
		if doc["success"] != true {
			t.Errorf("success = %v, want true", doc["success"])
		}
		if doc["row_count"].(float64) != 3 {
			t.Errorf("row_count = %v, want 3", doc["row_count"])
		}
	})

	t.Run("a failed stream is marked and still parses", func(t *testing.T) {
		doc := streamJSONForTest(t, &truncatingScanner{rows: 2, err: errors.New("simulated mid-stream failure")})
		if doc["truncated"] != true {
			t.Fatalf("truncated = %v, want true; the client cannot tell it lost data", doc["truncated"])
		}
		if reason, _ := doc["truncation_reason"].(string); reason == "" {
			t.Error("truncation_reason is empty; the client is told it lost data but not why")
		}
		// row_count stays the rows actually delivered; truncated is what says
		// the set is short.
		if doc["row_count"].(float64) != 2 {
			t.Errorf("row_count = %v, want 2", doc["row_count"])
		}
	})

	t.Run("a failure before the first row is marked", func(t *testing.T) {
		// The most damaging case: an empty result reads as a legitimate
		// "no data" rather than as a failure.
		doc := streamJSONForTest(t, &truncatingScanner{rows: 0, err: errors.New("failed before the first row")})
		if doc["truncated"] != true {
			t.Fatal("an empty truncated result is indistinguishable from an empty successful one")
		}
		if doc["row_count"].(float64) != 0 {
			t.Errorf("row_count = %v, want 0", doc["row_count"])
		}
	})

	t.Run("a reason containing JSON metacharacters keeps the document valid", func(t *testing.T) {
		// The sanitiser masks the CONTENTS of quoted spans but leaves the
		// quotes themselves, so a raw concatenation would emit a document
		// that does not parse. Backslashes, newlines and control bytes are
		// the same class of hazard.
		nasty := "Could not read \"s3://b/k.parquet\": back\\slash\nnewline\x01ctrl"
		doc := streamJSONForTest(t, &truncatingScanner{rows: 1, err: errors.New(nasty)})
		if doc["truncated"] != true {
			t.Fatal("expected the truncation marker")
		}
		if _, ok := doc["truncation_reason"].(string); !ok {
			t.Errorf("truncation_reason is not a string: %#v", doc["truncation_reason"])
		}
	})

	t.Run("a governance cap is not a truncation", func(t *testing.T) {
		var buf bytes.Buffer
		w := bufio.NewWriter(&buf)
		_, _ = streamTypedJSON(context.Background(), w, []string{"id"}, []colType{colInt64},
			&truncatingScanner{rows: 10}, 4, nil, time.Now(), "2026-09-11T00:00:00Z")
		w.Flush()
		var doc map[string]interface{}
		if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
			t.Fatalf("not valid JSON: %v", err)
		}
		// A capped result is deliberate and complete as far as policy is
		// concerned; marking it truncated would cry wolf. Tracked as #724.
		if _, ok := doc["truncated"]; ok {
			t.Error("a governance-capped result must not be marked truncated")
		}
	})
}
