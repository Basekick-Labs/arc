package arcxrouter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The inverse of #1005's sweep: the SQL this package generates must carry NO
// read_parquet option arguments at all.
//
// arcx's recognizer accepts only paths between the parentheses and rejects any
// option (arcx/src/parse.rs, expect_read_parquet_paths; pinned on that side by
// rejects_union_by_name_option). Adding hive_partitioning=false — or
// union_by_name, or anything else — here would make every arcx query decline:
// silently in serve mode, and with a per-query warning plus ArcxShadowDeclined
// in shadow mode, which is the default. The result would be a licensed engine
// turning itself off with no signal.
//
// arcx needs no flag: it reads Parquet itself via arrow-rs and performs no Hive
// partition inference. The DuckDB shadow oracle is built from the normal query
// path, so the oracle does get the flag.
//
// This test exists so the next person sweeping read_parquet call sites fails
// here instead of in production.
func TestArcxGeneratedSQLCarriesNoReadParquetOptions(t *testing.T) {
	data, err := os.ReadFile("router.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Assert on the STRUCTURE of each read_parquet this package spells: there
	// must be no `=` between the opening paren and its match. Checking for
	// option names instead would both false-positive the moment someone
	// documents this invariant in router.go, and miss an option arriving from a
	// variable.
	for i := 0; ; {
		j := strings.Index(src[i:], "read_parquet(")
		if j < 0 {
			break
		}
		start := i + j + len("read_parquet(")
		depth := 1
		k := start
		for ; k < len(src) && depth > 0; k++ {
			switch src[k] {
			case '(':
				depth++
			case ')':
				depth--
			}
		}
		inner := src[start : k-1]
		// The paths are Go expressions here, not literals, so `=` can only come
		// from an option (`:=` inside the span would mean the call is split
		// across statements, which these are not).
		if strings.Contains(inner, "=") {
			t.Errorf("read_parquet near offset %d carries an option argument (%q) — arcx rejects ANY option, so this makes every arcx query decline: silently in serve mode (router.go ModeServe), with a per-query warning and ArcxShadowDeclined in shadow, which is the default. arcx reads Parquet via arrow-rs and performs no Hive inference, so it needs no flag; the DuckDB shadow oracle gets it through the normal query path (#1005)", start, inner)
		}
		i = k
	}

	if !strings.Contains(src, "read_parquet(") {
		t.Fatal("router.go no longer spells read_parquet( — if the SQL moved, move this guard with it")
	}

	// The two halves of the exclusion must agree.
	guard, err := os.ReadFile(filepath.Join("..", "sql", "readparquet_guard_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(guard), "internal/arcxrouter/router.go") {
		t.Error("internal/sql's read_parquet guard no longer exempts arcxrouter; the two halves must agree")
	}
}
