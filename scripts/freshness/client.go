package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// measurementLimit is the largest limit /api/v1/query/:measurement accepts.
const measurementLimit = "1000000"

type client struct {
	base  string
	token string
	http  *http.Client
}

func (c *client) do(ctx context.Context, method, path string, header map[string]string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	return c.http.Do(req)
}

// drain reads the rest of a body so the connection is reused, and returns a
// short error for a status other than want.
func drain(resp *http.Response, want int) error {
	defer resp.Body.Close()
	if resp.StatusCode == want {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
	return fmt.Errorf("%s %s: HTTP %d: %s", resp.Request.Method, resp.Request.URL.Path, resp.StatusCode, strings.TrimSpace(string(msg)))
}

// write sends line protocol. It returns once Arc has acknowledged (204): the
// rows are in the ingest buffer, or in the WAL when one is on.
func (c *client) write(ctx context.Context, database string, body []byte) error {
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/write/line-protocol", map[string]string{"x-arc-database": database}, body)
	if err != nil {
		return err
	}
	return drain(resp, http.StatusNoContent)
}

// flush asks Arc to write every buffer to storage now (admin token).
func (c *client) flush(ctx context.Context) error {
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/write/line-protocol/flush", nil, nil)
	if err != nil {
		return err
	}
	return drain(resp, http.StatusOK)
}

// queryMarkers runs sql on the JSON endpoint and returns its first column.
func (c *client) queryMarkers(ctx context.Context, sql string) ([]int64, error) {
	return jsonProbe{sql: sql}.run(ctx, c)
}

// probe is one way of asking Arc which marker rows are visible.
type probe interface {
	run(ctx context.Context, c *client) ([]int64, error)
	describe() string
}

func newProbe(endpoint, database, measurement, run string, from, to time.Time) (probe, error) {
	where := fmt.Sprintf("time >= %s AND time < %s AND run = '%s' AND marker > 0", timeLiteral(from), timeLiteral(to), run)
	sql := fmt.Sprintf("SELECT marker FROM %s.%s WHERE %s", database, measurement, where)
	switch endpoint {
	case "json":
		return jsonProbe{sql: sql}, nil
	case "msgpack":
		return msgpackProbe{sql: sql}, nil
	case "arrow":
		return arrowProbe{sql: sql}, nil
	case "measurement":
		q := url.Values{}
		q.Set("database", database)
		// The endpoint defaults to 100 rows, newest first: without these it
		// would silently stop returning the oldest outstanding markers.
		// 1,000,000 is the most it accepts; a run with more marker rows than
		// that needs another endpoint.
		q.Set("limit", measurementLimit)
		q.Set("order_by", "time ASC")
		q.Set("where", where)
		return measurementProbe{path: "/api/v1/query/" + url.PathEscape(measurement) + "?" + q.Encode(), where: where}, nil
	}
	return nil, fmt.Errorf("unknown -endpoint %q (json, msgpack, arrow, measurement)", endpoint)
}

func sqlBody(sql string) []byte {
	b, _ := json.Marshal(map[string]string{"sql": sql})
	return b
}

var jsonHeader = map[string]string{"Content-Type": "application/json"}

// rowsResponse is the row-major JSON both /api/v1/query and
// /api/v1/query/:measurement answer with.
type rowsResponse struct {
	Success bool     `json:"success"`
	Error   string   `json:"error"`
	Columns []string `json:"columns"`
	Data    [][]any  `json:"data"`
}

func (r *rowsResponse) markers() ([]int64, error) {
	if !r.Success {
		return nil, fmt.Errorf("query failed: %s", r.Error)
	}
	col := -1
	for i, name := range r.Columns {
		if name == "marker" {
			col = i
		}
	}
	if col < 0 {
		if len(r.Data) == 0 {
			return nil, nil // a measurement with no file yet answers with no columns
		}
		return nil, fmt.Errorf("no marker column in %v", r.Columns)
	}
	out := make([]int64, 0, len(r.Data))
	for _, row := range r.Data {
		if col >= len(row) {
			return nil, fmt.Errorf("row with %d values, marker is column %d", len(row), col)
		}
		n, ok := row[col].(json.Number)
		if !ok {
			return nil, fmt.Errorf("marker value %v is %T, not a number", row[col], row[col])
		}
		v, err := n.Int64()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func decodeRows(resp *http.Response) ([]int64, error) {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return nil, fmt.Errorf("%s: HTTP %d: %s", resp.Request.URL.Path, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	var r rowsResponse
	if err := dec.Decode(&r); err != nil {
		return nil, err
	}
	return r.markers()
}

type jsonProbe struct{ sql string }

func (p jsonProbe) describe() string { return p.sql }
func (p jsonProbe) run(ctx context.Context, c *client) ([]int64, error) {
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/query", jsonHeader, sqlBody(p.sql))
	if err != nil {
		return nil, err
	}
	return decodeRows(resp)
}

type measurementProbe struct{ path, where string }

func (p measurementProbe) describe() string { return "GET /api/v1/query/:measurement where " + p.where }
func (p measurementProbe) run(ctx context.Context, c *client) ([]int64, error) {
	resp, err := c.do(ctx, http.MethodGet, p.path, nil, nil)
	if err != nil {
		return nil, err
	}
	return decodeRows(resp)
}

type msgpackProbe struct{ sql string }

func (p msgpackProbe) describe() string { return p.sql }
func (p msgpackProbe) run(ctx context.Context, c *client) ([]int64, error) {
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/query/msgpack", jsonHeader, sqlBody(p.sql))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return nil, fmt.Errorf("query/msgpack: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	// Columnar: data[i] is column i.
	var r struct {
		Success bool      `msgpack:"success"`
		Error   string    `msgpack:"error"`
		Columns []string  `msgpack:"columns"`
		Data    [][]int64 `msgpack:"data"`
	}
	if err := msgpack.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}
	if !r.Success {
		return nil, fmt.Errorf("query failed: %s", r.Error)
	}
	if len(r.Data) == 0 {
		return nil, nil
	}
	return r.Data[0], nil
}

type arrowProbe struct{ sql string }

func (p arrowProbe) describe() string { return p.sql }
func (p arrowProbe) run(ctx context.Context, c *client) ([]int64, error) {
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/query/arrow", jsonHeader, sqlBody(p.sql))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return nil, fmt.Errorf("query/arrow: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	rd, err := ipc.NewReader(resp.Body)
	if err != nil {
		return nil, err
	}
	defer rd.Release()
	var out []int64
	for rd.Next() {
		rec := rd.Record()
		if rec.NumCols() == 0 {
			continue
		}
		col, ok := rec.Column(0).(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("marker column is %s, not int64", rec.Column(0).DataType())
		}
		out = append(out, col.Int64Values()...)
	}
	return out, rd.Err()
}
