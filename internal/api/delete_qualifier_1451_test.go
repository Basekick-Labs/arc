package api

import (
	"fmt"
	"path/filepath"
	"slices"
	"testing"
)

// #1451: a delete's WHERE qualified by the measurement name failed with
// "Referenced table cpu not found"; the qualifier is now dropped before the
// clause is validated, recorded or run.

func TestUnqualifyMeasurement_Issue1451(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`cpu.host = 'a'`, `host = 'a'`},
		{`CPU.host = 'a'`, `host = 'a'`},
		{`"cpu".host = 'a'`, `host = 'a'`},
		{`cpu."host" = 'a'`, `"host" = 'a'`},
		{`cpu . host = 'a'`, `host = 'a'`},
		{`cpu.host = 'a' AND cpu.time < now()`, `host = 'a' AND time < now()`},
		{`cpu.tags.region = 'eu'`, `tags.region = 'eu'`},
		{`(cpu.v > 1 OR cpu.v < -1)`, `(v > 1 OR v < -1)`},
		// unchanged
		{`host = 'a'`, `host = 'a'`},
		{`cpu = 'a'`, `cpu = 'a'`},             // the bare name: a column
		{`cpu IS NOT NULL`, `cpu IS NOT NULL`}, // never the row
		{`host = 'cpu.host'`, `host = 'cpu.host'`},
		{`x.cpu.host = 1`, `x.cpu.host = 1`}, // a deeper path
		{`cpus.host = 1`, `cpus.host = 1`},
		{`mycpu.host = 1`, `mycpu.host = 1`},
		{`v > 1.5`, `v > 1.5`},
		{`cpu.`, `cpu.`},
		{`cpu.* IS NULL`, `cpu.* IS NULL`},
	} {
		if got := unqualifyMeasurement(tc.in, "cpu"); got != tc.want {
			t.Errorf("unqualifyMeasurement(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if got := unqualifyMeasurement(`"my-m".v = 1`, "my-m"); got != `v = 1` {
		t.Errorf("quoted measurement with a hyphen: %q", got)
	}
}

// Through POST /api/v1/delete on real Parquet: the qualified clause counts,
// deletes and keeps the same rows as the bare one.
func TestDeleteQualifiedWhere_Issue1451(t *testing.T) {
	r := newColdDeleteRig(t)
	key := leftoverHour + "cpu_qualified.parquet"
	r.parquet(key, "a", "b", "c", "a")

	body := func(where string, dryRun bool) string {
		return fmt.Sprintf(`{"database":"db","measurement":"cpu","where":%q,"confirm":true,"dry_run":%v}`, where, dryRun)
	}
	for _, where := range []string{`host = 'a'`, `cpu.host = 'a'`, `"cpu".host = 'a'`, `CPU.host = 'a'`} {
		code, out := r.delete(body(where, true))
		if code != 200 || !out.Success || out.DeletedCount != 2 {
			t.Fatalf("dry run %q: %d %+v, want 2 rows", where, code, out)
		}
	}

	code, out := r.delete(body(`cpu.host = 'a'`, false))
	if code != 200 || !out.Success || out.DeletedCount != 2 {
		t.Fatalf("delete: %d %+v, want 2 rows deleted", code, out)
	}
	var left []string
	rows, err := r.db.DB().Query(fmt.Sprintf("SELECT host FROM read_parquet('%s/db/cpu/**/*.parquet') ORDER BY host", r.root))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			t.Fatal(err)
		}
		left = append(left, h)
	}
	if !slices.Equal(left, []string{"b", "c"}) {
		t.Fatalf("rows left %v, want [b c]", left)
	}
}

// A bare name equal to the measurement stays a column reference: in a file
// without such a column it still fails to bind, and deletes nothing. Naming
// the read after the measurement would have bound it to the whole row, so
// `cpu IS NOT NULL` would have matched every row.
func TestDeleteBareMeasurementNameNeverMeansTheRow_Issue1451(t *testing.T) {
	r := newColdDeleteRig(t)
	key := leftoverHour + "cpu_bare.parquet"
	r.parquet(key, "a", "b")
	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"cpu IS NOT NULL","confirm":true}`)
	if out.DeletedCount != 0 {
		t.Fatalf("got %d %+v: rows deleted by a clause naming no column", code, out)
	}
	if !r.exists(key) {
		t.Fatal("the file was rewritten or removed")
	}
}

// A cold file whose rows all match a qualified clause is deleted, as with
// the bare clause.
func TestDeleteQualifiedWhereColdFile_Issue1451(t *testing.T) {
	r := newColdDeleteRig(t)
	r.coldFile(coldWhole, "h0", "h0")
	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"cpu.host = 'h0'","confirm":true}`)
	if code != 200 || !out.Success || out.DeletedCount != 2 || !slices.Equal(out.ColdDeletedFiles, []string{filepath.Base(coldWhole)}) {
		t.Fatalf("got %d %+v", code, out)
	}
}
