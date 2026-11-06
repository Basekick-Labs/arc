package api

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The transform masks function-body FROMs with generated placeholders and
// restores every occurrence of their text. Caller SQL that spells one is
// refused wherever it appears.
func TestValidateSQLRequestRefusesReservedPlaceholders(t *testing.T) {
	for _, sql := range []string{
		"SELECT v, EXTRACT(YEAR FROM time) AS y __FROM_MASK_0__ cpu",
		"SELECT '__FROM_MASK_0__' FROM cpu",
		`SELECT "__FROM_MASK_0__" FROM cpu`,
		"SELECT v FROM cpu -- __FROM_MASK_0__",
		// Completes a FROM placeholder only once the literal is masked.
		"SELECT v, EXTRACT(YEAR FROM time) AS y, 'a'FROM_MASK_0__ cpu",
	} {
		err := ValidateSQLRequest(sql)
		if err == nil || !strings.Contains(err.Error(), "reserved sequence") {
			t.Errorf("%q: want reserved-sequence refusal, got %v", sql, err)
		}
	}
}

func TestValidateSQLRequestKeepsFunctionBodyFrom(t *testing.T) {
	for _, sql := range []string{
		"SELECT EXTRACT(YEAR FROM time) AS y FROM cpu",
		"SELECT trim(BOTH FROM host), substring(host FROM 1 FOR 2) FROM cpu",
		"SELECT v FROM cpu WHERE host IS DISTINCT FROM 'a'",
		"SELECT my_str_col, from_mask, _ident_ FROM cpu",
		// Exact bytes: the restore never matches another case.
		"SELECT EXTRACT(YEAR FROM time) AS y, __from_mask_0__ FROM cpu",
	} {
		if err := ValidateSQLRequest(sql); err != nil {
			t.Errorf("%q: refused: %v", sql, err)
		}
	}
}

func TestHeaderDatabaseRefusesFromPlaceholder(t *testing.T) {
	for _, name := range []string{"db__FROM_MASK_0__x", "db__STR_0__", "db__IDENT_0__"} {
		if err := validateIdentifier(name); err == nil {
			t.Errorf("validateIdentifier(%q) accepted a reserved sequence", name)
		}
	}
	if err := validateIdentifier("db_from_mask"); err != nil {
		t.Errorf("validateIdentifier refused a plain name: %v", err)
	}
}

// The FROM placeholders are restored before the storage paths go in, so a
// path is never read by the restore. The database here is one the header
// validator refuses; the transform is called directly to pin the order.
func TestFromRestoreDoesNotReadStoragePaths(t *testing.T) {
	h := newParityTestHandler()
	ctx, _ := withStoragePathFailure(context.Background())
	db := "db__FROM_MASK_0__x"
	got := h.convertSQLToStoragePathsWithHeaderDB(ctx, "SELECT EXTRACT(YEAR FROM time) AS y FROM cpu", db)
	if !strings.Contains(got, "./data/"+db+"/cpu/") {
		t.Fatalf("storage path altered by the FROM restore:\n%s", got)
	}
	if !strings.Contains(got, "EXTRACT(YEAR FROM time)") {
		t.Fatalf("function-body FROM not restored:\n%s", got)
	}
}

// Both transforms refuse a statement that reaches them with placeholder
// text, so a caller that skipped ValidateSQLRequest still cannot have it
// restored into the checked statement.
func TestTransformRefusesReservedPlaceholders(t *testing.T) {
	for _, sql := range []string{
		"SELECT EXTRACT(YEAR FROM time) AS y __FROM_MASK_0__ cpu",
		"SELECT EXTRACT(YEAR FROM time) AS y, 'a'FROM_MASK_0__ cpu",
	} {
		for _, headerDB := range []string{"", "mydb"} {
			h := newParityTestHandler()
			ctx, failure := withStoragePathFailure(context.Background())
			if headerDB == "" {
				h.convertSQLToStoragePaths(ctx, sql)
			} else {
				h.convertSQLToStoragePathsWithHeaderDB(ctx, sql, headerDB)
			}
			if failure.err == nil || !strings.Contains(failure.err.Error(), "reserved sequence") {
				t.Errorf("%q header=%q: want refusal, got %v", sql, headerDB, failure.err)
			}
		}
	}
}

// pinDeleteTime restores each literal once, where the masker left it. A
// literal whose content spells another literal's placeholder keeps its
// content; replacing every occurrence spliced the other literal into it.
func TestPinDeleteTimeRestoresEachLiteralInPlace(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	in := "a = 'x' AND b = '__STR_0__' AND time < now()"
	got, err := pinDeleteTime(in, now)
	if err != nil {
		t.Fatal(err)
	}
	want := "a = 'x' AND b = '__STR_0__' AND time < TIMESTAMPTZ '2026-01-02 03:04:05+00'"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestDeleteWhereRefusesReservedPlaceholders(t *testing.T) {
	for _, where := range []string{"b = '__STR_0__'", "__IDENT_0__ = 1", "x __FROM_MASK_0__ y"} {
		if _, err := (&DeleteHandler{}).validateWhereClause(where); err == nil || !strings.Contains(err.Error(), "reserved sequence") {
			t.Errorf("%q: want reserved-sequence refusal, got %v", where, err)
		}
	}
}
