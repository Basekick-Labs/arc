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
// "…")). Keys built from a prefix are added by prefixedReads: loadEdgeSyncRelay's own fields (per
// side, checked against its source), and every field readObjectTarget reads (taken from its source
// and checked against setObjectTargetDefaults) under each relay prefix. The default
// side is setDefaults itself, run on an empty viper.
//
// Out of scope: backup.targets.<name>.* — those defaults are set at load time for the target
// names found, not in setDefaults.

// relayPrefixes are the prefixes loadEdgeSyncRelay is called with.
var relayPrefixes = []string{"edge_sync.spoke.relay.", "edge_sync.import.relay."}

// The fields loadEdgeSyncRelay reads itself, per side. Its source is checked against these
// lists (relayFieldsCovered), so a new field fails the test until it is assigned to a side; a
// side cannot be derived from the source, so which side reads what is stated here.
var (
	spokeRelayFields = []string{"enabled", "interval", "ack_timeout"}
	hubRelayFields   = []string{"enabled", "interval", "staging_dir"}
)

// prefixedReads are keys read as prefix+field, which no string literal names whole.
func prefixedReads(t *testing.T, files []*ast.File) []string {
	t.Helper()
	var out []string
	for _, f := range spokeRelayFields {
		out = append(out, "edge_sync.spoke.relay."+f)
	}
	for _, f := range hubRelayFields {
		out = append(out, "edge_sync.import.relay."+f)
	}
	relayFieldsCovered(t, files)
	fields := objectTargetFields(t, files)
	for _, p := range relayPrefixes {
		for _, f := range fields {
			out = append(out, p+f)
		}
	}
	return out
}

// relayFieldsCovered fails when loadEdgeSyncRelay reads a field neither side list names.
func relayFieldsCovered(t *testing.T, files []*ast.File) {
	t.Helper()
	known := map[string]bool{"relay": true} // the target name passed to readObjectTarget, not a key
	for _, f := range append(append([]string(nil), spokeRelayFields...), hubRelayFields...) {
		known[f] = true
	}
	found := funcFieldLiterals(files, "loadEdgeSyncRelay")
	if len(found) == 0 {
		t.Fatal("found no fields in loadEdgeSyncRelay: the scan is not seeing it")
	}
	for _, f := range found {
		if !known[f] {
			t.Errorf("loadEdgeSyncRelay reads %q, which neither spokeRelayFields nor hubRelayFields lists: add it to the side(s) that read it, with a default", f)
		}
	}
}

// objectTargetFields returns the fields readObjectTarget reads (from its source), and fails
// unless they are exactly the fields setObjectTargetDefaults defaults.
func objectTargetFields(t *testing.T, files []*ast.File) []string {
	t.Helper()
	read := map[string]bool{}
	for _, f := range funcFieldLiterals(files, "readObjectTarget") {
		read[f] = true
	}
	v := viper.New()
	setObjectTargetDefaults(v, "x.")
	defaulted := map[string]bool{}
	for _, k := range v.AllKeys() {
		defaulted[strings.TrimPrefix(k, "x.")] = true
	}
	for f := range read {
		if !defaulted[f] {
			t.Errorf("readObjectTarget reads %q, which setObjectTargetDefaults does not default", f)
		}
	}
	for f := range defaulted {
		if !read[f] {
			t.Errorf("setObjectTargetDefaults defaults %q, which readObjectTarget does not read (or reads outside its own body)", f)
		}
	}
	out := make([]string, 0, len(read))
	for f := range read {
		out = append(out, f)
	}
	sort.Strings(out)
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
	for _, k := range prefixedReads(t, parsed) {
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
