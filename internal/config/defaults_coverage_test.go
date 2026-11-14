package config

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

// Every configuration key Load reads has a v.SetDefault in setDefaults (#1109), so the code
// default and the documented default cannot drift and the key is visible with the rest. The read
// side is taken from the source: every dotted string literal passed to a method of the viper
// (v.GetString("…"), v.InConfig("…")) or to a helper that takes it (parsePositiveDuration(v,
// "…")). Keys built from a prefix are added by prefixedReads: loadEdgeSyncRelay's own fields, and
// every field readObjectTarget reads (taken from its source) under each relay prefix. The default
// side is setDefaults itself, run on an empty viper.
//
// Out of scope: backup.targets.<name>.* — those defaults are set at load time for the target
// names found, not in setDefaults.

// relayPrefixes are the prefixes loadEdgeSyncRelay is called with.
var relayPrefixes = []string{"edge_sync.spoke.relay.", "edge_sync.import.relay."}

// prefixedReads are keys read as prefix+field, which no string literal names whole.
func prefixedReads(t *testing.T, fset *token.FileSet, files []*ast.File) []string {
	t.Helper()
	out := []string{
		"edge_sync.spoke.relay.enabled", "edge_sync.spoke.relay.interval", "edge_sync.spoke.relay.ack_timeout",
		"edge_sync.import.relay.enabled", "edge_sync.import.relay.interval", "edge_sync.import.relay.staging_dir",
	}
	fields := funcFieldLiterals(files, "readObjectTarget")
	if len(fields) < 20 {
		t.Fatalf("found %d readObjectTarget fields: the scan is not seeing them", len(fields))
	}
	for _, p := range relayPrefixes {
		for _, f := range fields {
			out = append(out, p+f)
		}
	}
	return out
}

var fieldName = regexp.MustCompile(`^[a-z0-9_]+$`)

// funcFieldLiterals returns the field-name string literals in the body of the named function.
func funcFieldLiterals(files []*ast.File, name string) []string {
	var out []string
	for _, f := range files {
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Name.Name != name {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
					if s, err := strconv.Unquote(lit.Value); err == nil && fieldName.MatchString(s) {
						out = append(out, s)
					}
				}
				return true
			})
		}
	}
	return out
}

// notDefaulted are keys read on purpose without a default, and why. The test fails when one of
// them gains a default or is no longer read, so the list cannot go stale.
var notDefaulted = map[string]string{
	"backup.targets":                "a map read with GetStringMap: a non-empty default would be a phantom target when the file has none (an empty-map default does not show in AllKeys, so this list cannot see one)",
	"edge_sync.spoke.secret":        "env-only secret (ARC_EDGE_SYNC_SPOKE_SECRET, os.Getenv); viper only checks it is NOT in the config file (InConfig), and a default would advertise a key Load refuses",
	"edge_sync.spoke.shared_secret": "a spelling Load refuses in the config file (InConfig); the secret comes from ARC_EDGE_SYNC_SPOKE_SECRET",
	"edge_sync.spoke.hub_token":     "env-only secret read from ARC_EDGE_SYNC_HUB_TOKEN; viper only refuses it in the config file",
}

var configKey = regexp.MustCompile(`^[a-z0-9_]+(\.[a-z0-9_]+)+$`)

// readKeys returns the dotted keys passed as string literals to a viper method on v or to a call
// that takes v as an argument, in the package's non-test files.
func readKeys(t *testing.T) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]string{}
	var parsed []*ast.File
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		parsed = append(parsed, f)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			viperCall := false
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "v" && sel.Sel.Name != "SetDefault" {
					viperCall = true
				}
			}
			for _, a := range call.Args {
				if id, ok := a.(*ast.Ident); ok && id.Name == "v" {
					viperCall = true
				}
			}
			if !viperCall {
				return true
			}
			for _, a := range call.Args {
				lit, ok := a.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				s, err := strconv.Unquote(lit.Value)
				if err == nil && configKey.MatchString(s) {
					keys[s] = fset.Position(lit.Pos()).String()
				}
			}
			return true
		})
	}
	for _, k := range prefixedReads(t, fset, parsed) {
		keys[k] = "prefixedReads"
	}
	return keys
}

func TestEveryReadKeyHasADefault(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	defaulted := map[string]bool{}
	for _, k := range v.AllKeys() {
		defaulted[k] = true
	}
	reads := readKeys(t)
	if len(reads) < 100 {
		t.Fatalf("found only %d read keys: the source scan is not seeing the reads", len(reads))
	}
	var missing []string
	for k, where := range reads {
		if defaulted[k] {
			continue
		}
		if _, ok := notDefaulted[k]; ok {
			continue
		}
		missing = append(missing, k+" (read at "+where+")")
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("config keys read without a v.SetDefault in setDefaults:\n  %s", strings.Join(missing, "\n  "))
	}
	for k, why := range notDefaulted {
		if defaulted[k] {
			t.Errorf("%s is in notDefaulted (%s) but now has a default: remove it from the list", k, why)
		}
		if _, ok := reads[k]; !ok {
			t.Errorf("%s is in notDefaulted but no longer read: remove it from the list", k)
		}
	}
}
