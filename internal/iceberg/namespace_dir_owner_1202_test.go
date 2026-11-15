package iceberg

import (
	"testing"

	"github.com/apache/iceberg-go/table"
)

// NamespaceDirOwnedBy maps a storage-root segment to every namespace directory
// the exporter writes for it (#1202), built from the exporter's own encoders
// so the test cannot agree with a second copy of the rule.
func TestNamespaceDirOwnedBy(t *testing.T) {
	dir := func(database string) string {
		t.Helper()
		ns, err := namespaceIdentifier("arc", database)
		if err != nil {
			t.Fatal(err)
		}
		return catalogNamespaceKey(ns) + ".db"
	}
	legacy := func(parts ...string) string { return catalogNamespaceKeyLegacy(parts) + ".db" }

	for _, tc := range []struct {
		dir, root string
		want      bool
	}{
		{dir("prod"), "prod", true},
		{dir("prod"), "pro", false},
		{dir("prod"), "prod2", false},
		{dir("rocket01/telemetry"), "rocket01", true},
		{dir("rocket01/telemetry"), "rocket0", false},
		{dir("rocket01/telemetry"), "rocket01.telemetry", true}, // #750 ambiguity: over-report
		{dir("rocket.01/telemetry"), "rocket.01", true},         // encoded
		{dir("rocket.01/telemetry"), "rocket", false},
		{legacy("arc_rocket.01", "telemetry"), "rocket.01", true}, // rekeyed, directory not moved
		{legacy("arc_rocket.01", "telemetry"), "rocket", false},   // inner would carry a dot
		{"arc_rocket01.db", "rocket01", true},                     // pre-#634 spoke namespace
		{"arc_prod", "prod", false},                               // no .db
		{"spark_prod.db", "prod", false},
		{dir("prod"), "", false},
		{`__iceberg_namespace_v1__:["spark_rocket.01","telemetry"].db`, "rocket.01", false},
		{`__iceberg_namespace_v1__:["arc_rocket.01", "telemetry"].db`, "rocket.01", false}, // non-canonical encoding
	} {
		if got := NamespaceDirOwnedBy(tc.dir, "arc", tc.root); got != tc.want {
			t.Errorf("NamespaceDirOwnedBy(%q, arc, %q) = %v, want %v", tc.dir, tc.root, got, tc.want)
		}
	}
	// A non-default namespace prefix: the rule keys on it, not on "arc".
	lake, err := namespaceIdentifier("lake", "rocket.01/telemetry")
	if err != nil {
		t.Fatal(err)
	}
	lakeDir := catalogNamespaceKey(lake) + ".db"
	if !NamespaceDirOwnedBy(lakeDir, "lake", "rocket.01") || NamespaceDirOwnedBy(lakeDir, "arc", "rocket.01") ||
		!NamespaceDirOwnedBy("lake_rocket01.telemetry.db", "lake", "rocket01") || NamespaceDirOwnedBy("arc_rocket01.telemetry.db", "lake", "rocket01") {
		t.Error("NamespaceDirOwnedBy does not key on a non-default namespace prefix")
	}
	if !IsWarehouseNamespaceDir(dir("rocket.01/telemetry"), "arc") || IsWarehouseNamespaceDir("spark_x.db", "arc") {
		t.Error("IsWarehouseNamespaceDir does not follow isWarehouseNamespaceDir")
	}
}

func catalogNamespaceKeyLegacy(parts table.Identifier) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "."
		}
		out += p
	}
	return out
}
