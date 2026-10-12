package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// recordingCQScheduler records what the handlers tell the scheduler, so a test
// can see the wiring and not only the stored row.
type recordingCQScheduler struct {
	reloaded []int64
	started  []bool // isActive argument of each StartJobDirect call
}

func (s *recordingCQScheduler) ReloadCQ(id int64) error {
	s.reloaded = append(s.reloaded, id)
	return nil
}

func (s *recordingCQScheduler) StartJobDirect(_ int64, _, _ string, isActive bool) error {
	s.started = append(s.started, isActive)
	return nil
}

// The six fields PUT requires, as a JSON object body without the braces.
const cqRequiredFields = `"name":"cq1","database":"db","source_measurement":"src","destination_measurement":"dst",` +
	`"query":"SELECT * FROM src WHERE time >= {start_time} AND time < {end_time}","interval":"1h"`

type cqRow struct {
	description           sql.NullString
	tagColumns            sql.NullString
	retentionDays         sql.NullInt64
	deleteSourceAfterDays sql.NullInt64
	isActive              bool
	name                  string
}

func newPartialUpdateCQHandler(t *testing.T) (*ContinuousQueryHandler, *recordingCQScheduler, *fiber.App) {
	t.Helper()
	h := newSQLiteOnlyCQHandler(t, filepath.Join(t.TempDir(), "cq.db"))
	t.Cleanup(func() { h.sqliteDB.Close() })
	h.config = &config.ContinuousQueryConfig{Enabled: true}
	sched := &recordingCQScheduler{}
	h.scheduler = sched
	app := fiber.New()
	app.Post("/continuous-queries", h.handleCreate)
	app.Put("/continuous-queries/:id", h.handleUpdate)
	return h, sched, app
}

// seedFullCQ stores id 1 with every optional column set to a distinct value.
func seedFullCQ(t *testing.T, h *ContinuousQueryHandler, isActive bool) {
	t.Helper()
	_, err := h.sqliteDB.Exec(`INSERT INTO continuous_queries
		(id, name, description, database, source_measurement, destination_measurement, query, interval,
		 tag_columns, retention_days, delete_source_after_days, is_active)
		VALUES (1, 'cq1', 'stored description', 'db', 'src', 'dst', 'SELECT 1', '1h', '["host"]', 30, 7, ?)`, isActive)
	if err != nil {
		t.Fatalf("seed continuous query: %v", err)
	}
}

func readCQRow(t *testing.T, h *ContinuousQueryHandler, id int64) cqRow {
	t.Helper()
	var r cqRow
	err := h.sqliteDB.QueryRow(`SELECT name, description, tag_columns, retention_days, delete_source_after_days, is_active
		FROM continuous_queries WHERE id = ?`, id).
		Scan(&r.name, &r.description, &r.tagColumns, &r.retentionDays, &r.deleteSourceAfterDays, &r.isActive)
	if err != nil {
		t.Fatalf("read continuous query %d: %v", id, err)
	}
	return r
}

func sendCQ(t *testing.T, app *fiber.App, method, path, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp.StatusCode, string(raw)
}

// The bug in #1023: a PUT carrying only the required fields stored
// is_active=false and stopped the continuous query, and blanked the other four
// optional columns. Each must now keep its stored value, and the scheduler must
// be told to reload so it re-reads the (still active) row.
func TestCQUpdateOmittedOptionalFieldsKeepStoredValues(t *testing.T) {
	h, sched, app := newPartialUpdateCQHandler(t)
	seedFullCQ(t, h, true)

	status, body := sendCQ(t, app, "PUT", "/continuous-queries/1", "{"+cqRequiredFields+"}")
	if status != fiber.StatusOK {
		t.Fatalf("expected 200, got %d: %s", status, body)
	}

	r := readCQRow(t, h, 1)
	if !r.isActive {
		t.Error("is_active was cleared by a PUT that did not carry it")
	}
	if r.description.String != "stored description" {
		t.Errorf("description = %+v, want the stored value", r.description)
	}
	if r.tagColumns.String != `["host"]` {
		t.Errorf("tag_columns = %+v, want the stored value", r.tagColumns)
	}
	if r.retentionDays.Int64 != 30 || !r.retentionDays.Valid {
		t.Errorf("retention_days = %+v, want 30", r.retentionDays)
	}
	if r.deleteSourceAfterDays.Int64 != 7 || !r.deleteSourceAfterDays.Valid {
		t.Errorf("delete_source_after_days = %+v, want 7", r.deleteSourceAfterDays)
	}

	// The response is the stored definition, so a client reading it sees the
	// kept value too.
	var got ContinuousQuery
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !got.IsActive || len(got.TagColumns) != 1 || got.TagColumns[0] != "host" {
		t.Errorf("response = is_active %v, tag_columns %v; want true, [host]", got.IsActive, got.TagColumns)
	}

	if len(sched.reloaded) != 1 || sched.reloaded[0] != 1 {
		t.Errorf("ReloadCQ calls = %v, want [1]", sched.reloaded)
	}
}

// A paused continuous query must stay paused when a PUT leaves is_active out —
// the create-time default (absent = active) must not leak into update.
func TestCQUpdateOmittedIsActiveKeepsPaused(t *testing.T) {
	h, _, app := newPartialUpdateCQHandler(t)
	seedFullCQ(t, h, false)

	if status, body := sendCQ(t, app, "PUT", "/continuous-queries/1", "{"+cqRequiredFields+"}"); status != fiber.StatusOK {
		t.Fatalf("expected 200, got %d: %s", status, body)
	}
	if readCQRow(t, h, 1).isActive {
		t.Error("a paused continuous query was resumed by a PUT that did not carry is_active")
	}
}

// Every optional field written at once with distinct values, so a column and
// its argument swapping places in the UPDATE cannot pass.
func TestCQUpdateWritesEveryOptionalFieldItCarries(t *testing.T) {
	h, _, app := newPartialUpdateCQHandler(t)
	seedFullCQ(t, h, true)

	body := "{" + cqRequiredFields + `,"description":"new description","tag_columns":["region","host"],` +
		`"retention_days":90,"delete_source_after_days":14,"is_active":false}`
	if status, resp := sendCQ(t, app, "PUT", "/continuous-queries/1", body); status != fiber.StatusOK {
		t.Fatalf("expected 200, got %d: %s", status, resp)
	}

	r := readCQRow(t, h, 1)
	if r.isActive {
		t.Error("is_active=false was not written")
	}
	if r.description.String != "new description" {
		t.Errorf("description = %+v", r.description)
	}
	if r.tagColumns.String != `["region","host"]` {
		t.Errorf("tag_columns = %+v", r.tagColumns)
	}
	if r.retentionDays.Int64 != 90 {
		t.Errorf("retention_days = %+v, want 90", r.retentionDays)
	}
	if r.deleteSourceAfterDays.Int64 != 14 {
		t.Errorf("delete_source_after_days = %+v, want 14", r.deleteSourceAfterDays)
	}
}

// Some fields set, some null, some absent in one body.
func TestCQUpdateInterleavedSetNullAbsent(t *testing.T) {
	h, _, app := newPartialUpdateCQHandler(t)
	seedFullCQ(t, h, true)

	body := "{" + cqRequiredFields + `,"description":null,"retention_days":60,"tag_columns":null}`
	if status, resp := sendCQ(t, app, "PUT", "/continuous-queries/1", body); status != fiber.StatusOK {
		t.Fatalf("expected 200, got %d: %s", status, resp)
	}

	r := readCQRow(t, h, 1)
	if r.description.Valid {
		t.Errorf("description = %+v, want NULL (cleared)", r.description)
	}
	if r.tagColumns.Valid {
		t.Errorf("tag_columns = %+v, want NULL (cleared)", r.tagColumns)
	}
	if r.retentionDays.Int64 != 60 {
		t.Errorf("retention_days = %+v, want 60", r.retentionDays)
	}
	if r.deleteSourceAfterDays.Int64 != 7 || !r.deleteSourceAfterDays.Valid {
		t.Errorf("delete_source_after_days = %+v, want the stored 7 (absent)", r.deleteSourceAfterDays)
	}
	if !r.isActive {
		t.Error("is_active changed though absent")
	}
}

// Explicit null clears each clearable field. An empty tag_columns array is a
// value, not "keep": it stores NULL exactly as null does (no tags), which is how
// encodeTagColumns has always stored an empty list.
func TestCQUpdateNullClears(t *testing.T) {
	for _, field := range []string{"description", "tag_columns", "retention_days", "delete_source_after_days"} {
		t.Run(field, func(t *testing.T) {
			h, _, app := newPartialUpdateCQHandler(t)
			seedFullCQ(t, h, true)
			if status, resp := sendCQ(t, app, "PUT", "/continuous-queries/1",
				"{"+cqRequiredFields+`,"`+field+`":null}`); status != fiber.StatusOK {
				t.Fatalf("expected 200, got %d: %s", status, resp)
			}
			var v sql.NullString
			if err := h.sqliteDB.QueryRow("SELECT " + field + " FROM continuous_queries WHERE id = 1").Scan(&v); err != nil {
				t.Fatal(err)
			}
			if v.Valid {
				t.Errorf("%s = %q, want NULL", field, v.String)
			}
		})
	}
	t.Run("tag_columns empty array", func(t *testing.T) {
		h, _, app := newPartialUpdateCQHandler(t)
		seedFullCQ(t, h, true)
		if status, resp := sendCQ(t, app, "PUT", "/continuous-queries/1",
			"{"+cqRequiredFields+`,"tag_columns":[]}`); status != fiber.StatusOK {
			t.Fatalf("expected 200, got %d: %s", status, resp)
		}
		if r := readCQRow(t, h, 1); r.tagColumns.Valid {
			t.Errorf("tag_columns = %+v, want NULL", r.tagColumns)
		}
	})
}

func TestCQIsActiveNullIsRefused(t *testing.T) {
	h, sched, app := newPartialUpdateCQHandler(t)
	seedFullCQ(t, h, true)

	status, body := sendCQ(t, app, "PUT", "/continuous-queries/1", "{"+cqRequiredFields+`,"description":"changed","is_active":null}`)
	if status != fiber.StatusBadRequest || !strings.Contains(body, "is_active may not be null") {
		t.Fatalf("PUT: expected 400 naming is_active, got %d: %s", status, body)
	}
	if r := readCQRow(t, h, 1); r.description.String != "stored description" || !r.isActive {
		t.Errorf("a refused PUT changed the row: %+v", r)
	}

	status, body = sendCQ(t, app, "POST", "/continuous-queries", `{"name":"cq2",`+strings.TrimPrefix(cqRequiredFields, `"name":"cq1",`)+`,"is_active":null}`)
	if status != fiber.StatusBadRequest || !strings.Contains(body, "is_active may not be null") {
		t.Fatalf("POST: expected 400 naming is_active, got %d: %s", status, body)
	}
	if len(sched.reloaded) != 0 || len(sched.started) != 0 {
		t.Errorf("a refused request reached the scheduler: reloaded %v, started %v", sched.reloaded, sched.started)
	}
}

// A create without is_active used to insert false — overriding the column's
// DEFAULT TRUE — so the continuous query never ran. It is now active, and the
// scheduler is started with the same value the row holds.
func TestCQCreateIsActiveDefaultsToTrue(t *testing.T) {
	for _, tc := range []struct {
		extra string
		want  bool
	}{
		{"", true},
		{`,"is_active":true`, true},
		{`,"is_active":false`, false},
	} {
		t.Run(fmt.Sprintf("body%s", tc.extra), func(t *testing.T) {
			h, sched, app := newPartialUpdateCQHandler(t)
			status, body := sendCQ(t, app, "POST", "/continuous-queries", "{"+cqRequiredFields+tc.extra+"}")
			if status != fiber.StatusCreated {
				t.Fatalf("expected 201, got %d: %s", status, body)
			}
			var got ContinuousQuery
			if err := json.Unmarshal([]byte(body), &got); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if r := readCQRow(t, h, got.ID); r.isActive != tc.want {
				t.Errorf("stored is_active = %v, want %v", r.isActive, tc.want)
			}
			if len(sched.started) != 1 || sched.started[0] != tc.want {
				t.Errorf("StartJobDirect isActive = %v, want [%v]", sched.started, tc.want)
			}
		})
	}
}

// Leaving tag_columns out must not re-validate or touch the stored list; a
// present invalid list is still refused.
func TestCQUpdateTagColumnsValidation(t *testing.T) {
	h, _, app := newPartialUpdateCQHandler(t)
	seedFullCQ(t, h, true)

	status, body := sendCQ(t, app, "PUT", "/continuous-queries/1", "{"+cqRequiredFields+`,"tag_columns":["bad name"]}`)
	if status != fiber.StatusBadRequest {
		t.Fatalf("expected 400 for an invalid tag column, got %d: %s", status, body)
	}
	if r := readCQRow(t, h, 1); r.tagColumns.String != `["host"]` {
		t.Errorf("a refused PUT changed tag_columns: %+v", r.tagColumns)
	}
}

func TestCQUpdateMissingIDIs404(t *testing.T) {
	_, sched, app := newPartialUpdateCQHandler(t)
	status, body := sendCQ(t, app, "PUT", "/continuous-queries/42", "{"+cqRequiredFields+"}")
	if status != fiber.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", status, body)
	}
	if len(sched.reloaded) != 0 {
		t.Errorf("ReloadCQ called for a missing id: %v", sched.reloaded)
	}
}

func TestOptionalFieldUnmarshal(t *testing.T) {
	type body struct {
		A optionalField[int]  `json:"a"`
		B optionalField[bool] `json:"b"`
	}
	for _, tc := range []struct {
		in                   string
		set, null, wantError bool
		value                int
	}{
		{in: `{}`},
		{in: `{"a":null}`, set: true, null: true},
		{in: `{"a":5}`, set: true, value: 5},
		{in: `{"a":0}`, set: true, value: 0},
		{in: `{"a":"x"}`, wantError: true},
		// A duplicate key: the last one wins, and null must not keep the
		// earlier value around.
		{in: `{"a":5,"a":null}`, set: true, null: true},
		{in: `{"a":null,"a":5}`, set: true, value: 5},
	} {
		var b body
		err := json.Unmarshal([]byte(tc.in), &b)
		if tc.wantError {
			if err == nil {
				t.Errorf("%s: expected an error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if b.A.Set != tc.set || b.A.Null != tc.null || b.A.Value != tc.value {
			t.Errorf("%s: got %+v, want set=%v null=%v value=%d", tc.in, b.A, tc.set, tc.null, tc.value)
		}
		if b.B.Set {
			t.Errorf("%s: absent key b reported Set", tc.in)
		}
	}
}

// Through the production server's Fiber app and BodyParser, which is what the
// handlers use: if NewServer ever configures a JSON decoder that stops calling
// UnmarshalJSON for a literal null, or calls it for an absent key, this fails
// rather than every optional field silently becoming "absent".
func TestOptionalFieldThroughBodyParser(t *testing.T) {
	app := NewServer(nil, zerolog.Nop()).GetApp()
	var got ContinuousQueryRequest
	app.Post("/", func(c *fiber.Ctx) error { return c.BodyParser(&got) })
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"description":null,"is_active":false}`))
	req.Header.Set("Content-Type", "application/json")
	if _, err := app.Test(req, testRequestTimeoutMS); err != nil {
		t.Fatal(err)
	}
	if !got.Description.Set || !got.Description.Null {
		t.Errorf("description null decoded as %+v", got.Description)
	}
	if !got.IsActive.Set || got.IsActive.Null || got.IsActive.Value {
		t.Errorf("is_active false decoded as %+v", got.IsActive)
	}
	if got.TagColumns.Set || got.RetentionDays.Set {
		t.Errorf("absent keys decoded as set: tag_columns %+v, retention_days %+v", got.TagColumns, got.RetentionDays)
	}
}

// Since #1023 update is fully partial: the body the clients actually want to
// send to pause a continuous query (launchpad#91, arc-client-python#4) is just
// is_active, and every other column keeps its stored value.
func TestCQUpdatePauseBodyAlone(t *testing.T) {
	h, sched, app := newPartialUpdateCQHandler(t)
	seedFullCQ(t, h, true)

	status, body := sendCQ(t, app, "PUT", "/continuous-queries/1", `{"is_active":false}`)
	if status != fiber.StatusOK {
		t.Fatalf("expected 200, got %d: %s", status, body)
	}
	var name, database, src, dst, query, interval string
	if err := h.sqliteDB.QueryRow(`SELECT name, database, source_measurement, destination_measurement, query, interval
		FROM continuous_queries WHERE id = 1`).Scan(&name, &database, &src, &dst, &query, &interval); err != nil {
		t.Fatal(err)
	}
	if name != "cq1" || database != "db" || src != "src" || dst != "dst" || query != "SELECT 1" || interval != "1h" {
		t.Errorf("identity columns changed: %q %q %q %q %q %q", name, database, src, dst, query, interval)
	}
	r := readCQRow(t, h, 1)
	if r.isActive {
		t.Error("is_active=false was not written")
	}
	if r.tagColumns.String != `["host"]` || r.retentionDays.Int64 != 30 || r.description.String != "stored description" {
		t.Errorf("optional columns changed: %+v", r)
	}
	if len(sched.reloaded) != 1 {
		t.Errorf("ReloadCQ calls = %v, want one", sched.reloaded)
	}
}

// Each identity field left out keeps its stored value; a present one is written.
func TestCQUpdateOmittedRequiredFieldsKeepStoredValues(t *testing.T) {
	h, _, app := newPartialUpdateCQHandler(t)
	seedFullCQ(t, h, true)

	status, body := sendCQ(t, app, "PUT", "/continuous-queries/1", `{"interval":"5m","destination_measurement":"dst2"}`)
	if status != fiber.StatusOK {
		t.Fatalf("expected 200, got %d: %s", status, body)
	}
	var name, database, src, dst, query, interval string
	if err := h.sqliteDB.QueryRow(`SELECT name, database, source_measurement, destination_measurement, query, interval
		FROM continuous_queries WHERE id = 1`).Scan(&name, &database, &src, &dst, &query, &interval); err != nil {
		t.Fatal(err)
	}
	if name != "cq1" || database != "db" || src != "src" || query != "SELECT 1" {
		t.Errorf("omitted identity columns changed: %q %q %q %q", name, database, src, query)
	}
	if dst != "dst2" || interval != "5m" {
		t.Errorf("carried columns not written: destination %q interval %q", dst, interval)
	}
}

func TestCQUpdateEmptyBodyIsRefused(t *testing.T) {
	h, sched, app := newPartialUpdateCQHandler(t)
	seedFullCQ(t, h, true)
	status, body := sendCQ(t, app, "PUT", "/continuous-queries/1", `{}`)
	if status != fiber.StatusBadRequest || !strings.Contains(body, "no fields to update") {
		t.Fatalf("expected 400 no fields to update, got %d: %s", status, body)
	}
	if len(sched.reloaded) != 0 {
		t.Errorf("ReloadCQ called for an empty update: %v", sched.reloaded)
	}
}

// A new query in a partial body still gets the shared SQL validator: without
// it, creating a benign continuous query and updating only its query would
// bypass the create-time check.
func TestCQUpdateQueryAloneIsValidated(t *testing.T) {
	h, _, app := newPartialUpdateCQHandler(t)
	seedFullCQ(t, h, true)
	status, body := sendCQ(t, app, "PUT", "/continuous-queries/1",
		`{"query":"SELECT * FROM read_csv('/etc/passwd') WHERE time >= {start_time} AND time < {end_time}"}`)
	if status != fiber.StatusBadRequest {
		t.Fatalf("expected 400 from the SQL validator, got %d: %s", status, body)
	}
	status, body = sendCQ(t, app, "PUT", "/continuous-queries/1", `{"query":"SELECT * FROM src"}`)
	if status != fiber.StatusBadRequest || !strings.Contains(body, "placeholders") {
		t.Fatalf("expected 400 for missing placeholders, got %d: %s", status, body)
	}
}
