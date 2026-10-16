package api

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

type rpRow struct {
	name, database string
	measurement    sql.NullString
	retentionDays  int
	bufferDays     int
	isActive       bool
}

func newPartialUpdateRetentionApp(t *testing.T) (*RetentionHandler, *fiber.App) {
	t.Helper()
	h, _ := setupTestRetentionHandler(t)
	app := fiber.New()
	app.Post("/retention", h.handleCreate)
	app.Put("/retention/:id", h.handleUpdate)
	return h, app
}

// seedPinnedPolicy stores id 1: pinned to measurement cpu, 30 days + 7 buffer.
func seedPinnedPolicy(t *testing.T, h *RetentionHandler, isActive bool) {
	t.Helper()
	if _, err := h.db.Exec(`INSERT INTO retention_policies (id, name, database, measurement, retention_days, buffer_days, is_active)
		VALUES (1, 'rp1', 'db', 'cpu', 30, 7, ?)`, isActive); err != nil {
		t.Fatalf("seed retention policy: %v", err)
	}
}

func readRPRow(t *testing.T, h *RetentionHandler, id int64) rpRow {
	t.Helper()
	var r rpRow
	if err := h.db.QueryRow(`SELECT name, database, measurement, retention_days, buffer_days, is_active
		FROM retention_policies WHERE id = ?`, id).
		Scan(&r.name, &r.database, &r.measurement, &r.retentionDays, &r.bufferDays, &r.isActive); err != nil {
		t.Fatalf("read retention policy %d: %v", id, err)
	}
	return r
}

var seededRP = rpRow{name: "rp1", database: "db", measurement: sql.NullString{String: "cpu", Valid: true},
	retentionDays: 30, bufferDays: 7, isActive: true}

// The bodies the shipped clients send. Before #1023 each of these either
// returned 400 (retention_days 0 <= buffer 0, or an empty database) or, when it
// got through, wrote every column it left out as a zero: measurement NULL
// widened a pinned policy to the whole database, buffer_days 0 moved the
// cutoff in by seven days, and an absent is_active stopped the policy.
func TestRetentionUpdateClientBodies(t *testing.T) {
	cases := []struct {
		name string
		body string
		want func(r rpRow) rpRow
	}{
		{"launchpad pause toggle", `{"is_active":false}`,
			func(r rpRow) rpRow { r.isActive = false; return r }},
		{"python SDK update without database", `{"retention_days":60,"is_active":false}`,
			func(r rpRow) rpRow { r.retentionDays = 60; r.isActive = false; return r }},
		{"name only", `{"name":"renamed"}`,
			func(r rpRow) rpRow { r.name = "renamed"; return r }},
		{"full body without measurement keeps the pin", `{"name":"rp1","database":"db","retention_days":45}`,
			func(r rpRow) rpRow { r.retentionDays = 45; return r }},
		{"null measurement means the whole database", `{"measurement":null}`,
			func(r rpRow) rpRow { r.measurement = sql.NullString{}; return r }},
		{"empty measurement is stored as null", `{"measurement":""}`,
			func(r rpRow) rpRow { r.measurement = sql.NullString{}; return r }},
		{"every field", `{"name":"n","database":"db2","measurement":"mem","retention_days":90,"buffer_days":3,"is_active":false}`,
			func(r rpRow) rpRow {
				return rpRow{name: "n", database: "db2", measurement: sql.NullString{String: "mem", Valid: true},
					retentionDays: 90, bufferDays: 3, isActive: false}
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, app := newPartialUpdateRetentionApp(t)
			seedPinnedPolicy(t, h, true)
			status, body := sendCQ(t, app, "PUT", "/retention/1", tc.body)
			if status != fiber.StatusOK {
				t.Fatalf("expected 200, got %d: %s", status, body)
			}
			if got, want := readRPRow(t, h, 1), tc.want(seededRP); got != want {
				t.Errorf("row = %+v, want %+v", got, want)
			}
		})
	}
}

// A paused policy stays paused when a PUT leaves is_active out.
func TestRetentionUpdateOmittedIsActiveKeepsPaused(t *testing.T) {
	h, app := newPartialUpdateRetentionApp(t)
	seedPinnedPolicy(t, h, false)
	if status, body := sendCQ(t, app, "PUT", "/retention/1", `{"retention_days":40}`); status != fiber.StatusOK {
		t.Fatalf("expected 200, got %d: %s", status, body)
	}
	if readRPRow(t, h, 1).isActive {
		t.Error("a paused policy was resumed by a PUT that did not carry is_active")
	}
}

// retention_days > buffer_days is enforced against the values the row will
// hold, including a stored one the body does not carry.
func TestRetentionUpdateRetentionBufferRuleUsesStoredValues(t *testing.T) {
	refused := []struct{ name, body string }{
		{"buffer raised to the stored retention", `{"buffer_days":30}`},
		{"buffer raised past the stored retention", `{"buffer_days":45,"is_active":false}`},
		{"retention lowered to the stored buffer", `{"retention_days":7}`},
		{"both carried and inverted", `{"retention_days":5,"buffer_days":6}`},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			h, app := newPartialUpdateRetentionApp(t)
			seedPinnedPolicy(t, h, true)
			status, body := sendCQ(t, app, "PUT", "/retention/1", tc.body)
			if status != fiber.StatusBadRequest || !strings.Contains(body, "retention_days must be greater than buffer_days") {
				t.Fatalf("expected 400 naming the rule, got %d: %s", status, body)
			}
			if got := readRPRow(t, h, 1); got != seededRP {
				t.Errorf("a refused PUT changed the row: %+v", got)
			}
		})
	}
	t.Run("buffer raised below the stored retention is accepted", func(t *testing.T) {
		h, app := newPartialUpdateRetentionApp(t)
		seedPinnedPolicy(t, h, true)
		if status, body := sendCQ(t, app, "PUT", "/retention/1", `{"buffer_days":29}`); status != fiber.StatusOK {
			t.Fatalf("expected 200, got %d: %s", status, body)
		}
		if got := readRPRow(t, h, 1).bufferDays; got != 29 {
			t.Errorf("buffer_days = %d, want 29", got)
		}
	})
}

func TestRetentionUpdateRefusals(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"empty body", `{}`, "no fields to update"},
		{"null is_active", `{"is_active":null}`, "is_active may not be null"},
		{"null buffer_days", `{"buffer_days":null}`, "buffer_days may not be null"},
		{"null retention_days", `{"retention_days":null}`, "retention_days may not be null"},
		{"null name", `{"name":null}`, "name may not be null"},
		{"empty name", `{"name":""}`, "name is required"},
		{"null database", `{"database":null}`, "database may not be null"},
		{"empty database", `{"database":""}`, "database is required"},
		{"unsafe database", `{"database":"a/b"}`, "invalid database name"},
		{"unsafe measurement", `{"measurement":".hidden"}`, "invalid measurement name"},
		{"zero retention", `{"retention_days":0}`, "retention_days must be greater than 0"},
		{"negative buffer", `{"buffer_days":-1}`, "buffer_days may not be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, app := newPartialUpdateRetentionApp(t)
			seedPinnedPolicy(t, h, true)
			status, body := sendCQ(t, app, "PUT", "/retention/1", tc.body)
			if status != fiber.StatusBadRequest || !strings.Contains(body, tc.want) {
				t.Fatalf("expected 400 containing %q, got %d: %s", tc.want, status, body)
			}
			if got := readRPRow(t, h, 1); got != seededRP {
				t.Errorf("a refused PUT changed the row: %+v", got)
			}
		})
	}
}

func TestRetentionUpdateMissingIDAndDuplicateName(t *testing.T) {
	h, app := newPartialUpdateRetentionApp(t)
	seedPinnedPolicy(t, h, true)
	if status, body := sendCQ(t, app, "PUT", "/retention/42", `{"is_active":false}`); status != fiber.StatusNotFound {
		t.Fatalf("missing id: expected 404, got %d: %s", status, body)
	}
	// A missing id whose body would also break the retention/buffer rule is
	// still a 404, not a 400 about a row that does not exist.
	if status, body := sendCQ(t, app, "PUT", "/retention/42", `{"buffer_days":100}`); status != fiber.StatusNotFound {
		t.Fatalf("missing id with a bad buffer: expected 404, got %d: %s", status, body)
	}
	if _, err := h.db.Exec(`INSERT INTO retention_policies (id, name, database, retention_days, buffer_days, is_active)
		VALUES (2, 'rp2', 'db', 30, 7, 1)`); err != nil {
		t.Fatal(err)
	}
	status, body := sendCQ(t, app, "PUT", "/retention/2", `{"name":"rp1"}`)
	if status != fiber.StatusBadRequest || !strings.Contains(body, "already exists") {
		t.Fatalf("duplicate name: expected 400, got %d: %s", status, body)
	}
}

// Create keeps its long-standing defaults for the two optional fields — an
// absent is_active is false, so an existing client's new policy does not start
// deleting — and now refuses a negative buffer, which deleted data younger
// than retention_days.
func TestRetentionCreate(t *testing.T) {
	h, app := newPartialUpdateRetentionApp(t)
	status, body := sendCQ(t, app, "POST", "/retention", `{"name":"a","database":"db","retention_days":30,"measurement":""}`)
	if status != fiber.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", status, body)
	}
	var id int64
	if err := h.db.QueryRow("SELECT id FROM retention_policies WHERE name = 'a'").Scan(&id); err != nil {
		t.Fatal(err)
	}
	r := readRPRow(t, h, id)
	if r.isActive || r.bufferDays != 0 || r.measurement.Valid {
		t.Errorf("create defaults changed: %+v (want is_active false, buffer 0, measurement NULL)", r)
	}

	for _, tc := range []struct{ name, body, want string }{
		{"negative buffer", `{"name":"b","database":"db","retention_days":30,"buffer_days":-5}`, "buffer_days may not be negative"},
		{"missing retention_days", `{"name":"b","database":"db"}`, "retention_days is required"},
		{"missing name", `{"database":"db","retention_days":30}`, "name is required"},
		{"missing database", `{"name":"b","retention_days":30}`, "database is required"},
		{"retention not above buffer", `{"name":"b","database":"db","retention_days":7,"buffer_days":7}`, "retention_days must be greater than buffer_days"},
		{"null is_active", `{"name":"b","database":"db","retention_days":30,"is_active":null}`, "is_active may not be null"},
		// Arc's own storage roots, not databases (#1012).
		{"reserved _schema", `{"name":"b","database":"_schema","retention_days":30}`, "is reserved for Arc's own storage"},
		{"reserved _compaction_state", `{"name":"b","database":"_compaction_state","retention_days":30}`, "is reserved for Arc's own storage"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := sendCQ(t, app, "POST", "/retention", tc.body)
			if status != fiber.StatusBadRequest || !strings.Contains(body, tc.want) {
				t.Fatalf("expected 400 containing %q, got %d: %s", tc.want, status, body)
			}
		})
	}
}
