package api

import (
	"context"
	"strings"
	"testing"
)

// A database or measurement name that contains the text of a literal-masking
// placeholder (__STR_n__, __IDENT_n__) is reserved. The transform builds
// storage paths from names while literals are masked and restores every
// placeholder afterwards, so such a name must never reach a path.

func TestValidateIdentifierRefusesPlaceholderText(t *testing.T) {
	for _, name := range []string{"x__STR_0__", "__STR_1__", "a__IDENT_2__b", "db__STR_"} {
		if validateIdentifier(name) == nil {
			t.Errorf("validateIdentifier(%q) accepted a reserved name", name)
		}
		if validateHeaderDatabase(name) == nil {
			t.Errorf("validateHeaderDatabase(%q) accepted a reserved name", name)
		}
	}
	// Lower case is not placeholder text, and double underscores are fine.
	for _, name := range []string{"x__str_0__", "my__table", "readings"} {
		if err := validateIdentifier(name); err != nil {
			t.Errorf("validateIdentifier(%q) = %v, want ok", name, err)
		}
	}
}

func TestIdentResolverRefusesPlaceholderText(t *testing.T) {
	resolve := makeIdentResolver(map[string]string{"__IDENT_0__": `x__STR_1__`})
	for _, name := range []string{"x__STR_0__", "a__IDENT_3__", "__IDENT_0__"} {
		if got, ok := resolve(name); ok || got != arcInvalidIdentifierSentinel {
			t.Errorf("resolve(%q) = %q, %v; want the sentinel", name, got, ok)
		}
	}
	if got, ok := resolve("x__str_0__"); !ok || got != "x__str_0__" {
		t.Errorf("resolve(x__str_0__) = %q, %v", got, ok)
	}
}

func TestTransformNeverBuildsAPathFromPlaceholderText(t *testing.T) {
	h := newCommaJoinTestHandler()
	for _, sql := range []string{
		"SELECT v FROM x__STR_0__ WHERE a = 'zz'",
		"SELECT v FROM db.x__STR_0__ WHERE a = 'zz'",
		"SELECT v FROM x__STR_0__.t WHERE a = 'zz'",
		"SELECT v FROM t, x__STR_0__ WHERE a = 'zz'",
		"SELECT v FROM t JOIN x__STR_0__ ON true WHERE a = 'zz'",
		`SELECT v FROM x__IDENT_0__ WHERE "zz" = 1`,
	} {
		for name, got := range map[string]string{
			"no header": h.convertSQLToStoragePaths(context.Background(), sql),
			"header":    h.convertSQLToStoragePathsWithHeaderDB(context.Background(), sql, "db"),
		} {
			if name == "header" && hasCrossDatabaseSyntax(sql) {
				// Refused before the transform runs under a header
				// database, and this path builds no storage path for it.
				continue
			}
			if !strings.Contains(got, arcInvalidIdentifierSentinel) {
				t.Errorf("%s: %q: reserved name not refused:\n%s", name, sql, got)
			}
			if strings.Count(got, "zz") != 1 {
				t.Errorf("%s: %q: literal restored outside its place:\n%s", name, sql, got)
			}
		}
	}
}

func TestValidateSQLRequestRefusesQuotedPlaceholderTextInTablePosition(t *testing.T) {
	if err := ValidateSQLRequest(`SELECT 1 FROM "x__STR_0__" WHERE a = 'b'`); err == nil {
		t.Fatal("a quoted reserved name in table position was accepted")
	}
	if err := ValidateSQLRequest(`SELECT '__STR_0__' AS s, "x__STR_0__" FROM t`); err != nil {
		t.Fatalf("placeholder text outside table position refused: %v", err)
	}
}
