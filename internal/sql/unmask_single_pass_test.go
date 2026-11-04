package sql

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// unmaskByMask is the previous UnmaskStringLiterals, one strings.Replace per
// mask, kept as the reference the single-pass version must agree with.
func unmaskByMask(sql string, masks []StringMask) string {
	result := sql
	for _, mask := range masks {
		if mask.Identifier {
			result = strings.ReplaceAll(result, mask.Placeholder, mask.Original)
			continue
		}
		result = strings.Replace(result, mask.Placeholder, mask.Original, 1)
	}
	return result
}

// On everything the masker produces from SQL that carries no placeholder
// text, the single pass restores exactly what the per-mask version did, and
// both give back the input.
func TestUnmaskSinglePassMatchesPerMask(t *testing.T) {
	corpus := []string{
		`SELECT 1`,
		`SELECT 'a', 'b' FROM t WHERE x = 'c'`,
		`SELECT "Host", "Host", "my col" FROM "my-table" WHERE "Host" = 'a'`,
		`SELECT E'it\'s', $$dollar$$, $tag$x$tag$ FROM t`,
		`SELECT 'a''b', "q""q" FROM t`,
		`SELECT '' , "" FROM t`,
		`SELECT 'x'||'y'||'z'`,
		`SELECT 'a' -- 'comment'
FROM t /* 'block' */ WHERE y = 'b'`,
		`SELECT '` + strings.Repeat("x", 100) + `'`,
	}
	ids := make([]string, 12)
	for i := range ids {
		ids[i] = fmt.Sprintf("'ch%d'", i) // placeholders __STR_1__ .. __STR_11__ next to __STR_10__
	}
	corpus = append(corpus, "SELECT * FROM t WHERE c IN ("+strings.Join(ids, ",")+")")

	// Random statements over an alphabet with every quoting form and no '_',
	// so no input carries placeholder text.
	r := rand.New(rand.NewSource(1448))
	parts := []string{"'a'", "'b''c'", `"Id"`, `"x y"`, "E'q\\'r'", "$$d$$", "$t$e$t$", " ", ",", "(", ")", "x", "1", "-- c\n", "/* k */", "'", `"`}
	for i := 0; i < 20000; i++ {
		var b strings.Builder
		for n := r.Intn(14); n >= 0; n-- {
			b.WriteString(parts[r.Intn(len(parts))])
		}
		corpus = append(corpus, b.String())
	}

	for _, in := range corpus {
		masked, masks := MaskStringLiterals(in, true)
		got, want := UnmaskStringLiterals(masked, masks), unmaskByMask(masked, masks)
		if got != want {
			t.Fatalf("input %q\n single pass: %q\n per mask:    %q", in, got, want)
		}
		if got != in {
			t.Fatalf("input %q did not round-trip: %q", in, got)
		}
	}
}

// A string placeholder is restored at its first occurrence only and an
// identifier placeholder at every one, as the per-mask version did.
func TestUnmaskOccurrenceRules(t *testing.T) {
	masks := []StringMask{
		{Placeholder: "__STR_0__", Original: "'a'"},
		{Placeholder: "__IDENT_1__", Original: `"B"`, Identifier: true},
	}
	in := "__STR_0__ __IDENT_1__ __STR_0__ __IDENT_1__ __STR_01__ __STR_0_ x__STR_0__"
	got, want := UnmaskStringLiterals(in, masks), unmaskByMask(in, masks)
	if got != want || got != `'a' "B" __STR_0__ "B" __STR_01__ __STR_0_ x__STR_0__` {
		t.Fatalf("single pass %q, per mask %q", got, want)
	}

	// Placeholders that share their underscores: the scan resumes after the
	// one it restored, so the second is never matched across the first.
	in = "__STR_0__IDENT_1__ end"
	got, want = UnmaskStringLiterals(in, masks), unmaskByMask(in, masks)
	if got != want || got != `'a'IDENT_1__ end` {
		t.Fatalf("overlap: single pass %q, per mask %q", got, want)
	}
}

// Restored text is never scanned again: a literal whose content looks like a
// placeholder stays as written, and every literal stays inside its quotes.
func TestUnmaskDoesNotRescanRestoredText(t *testing.T) {
	for _, in := range []string{
		`SELECT '__STR_1__' AS a, 'x' AS b`,
		`SELECT "__STR_1__" AS a, 'x' AS b`,
		`SELECT '__IDENT_1__' AS a, "x" AS b`,
		`SELECT 'a__STR_2__b', 'y', 'z'`,
	} {
		masked, masks := MaskStringLiterals(in, true)
		if got := UnmaskStringLiterals(masked, masks); got != in {
			t.Errorf("input %q restored as %q", in, got)
		}
	}
}

// Restoring is linear in the statement: 100,000 literals took about 34 s
// when every literal copied the whole statement.
func TestUnmaskManyLiteralsIsLinear(t *testing.T) {
	ids := make([]string, 100000)
	for i := range ids {
		ids[i] = fmt.Sprintf("'site1/line3/ch%06d'", i)
	}
	in := "SELECT * FROM t WHERE c IN (" + strings.Join(ids, ",") + ")"
	masked, masks := MaskStringLiterals(in, true)
	start := time.Now()
	got := UnmaskStringLiterals(masked, masks)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("unmasking %d bytes took %v", len(in), d)
	}
	if got != in {
		t.Fatal("did not round-trip")
	}
}

func BenchmarkUnmaskStringLiterals(b *testing.B) {
	for _, n := range []int{100, 10000} {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = fmt.Sprintf("'ch%06d'", i)
		}
		masked, masks := MaskStringLiterals("SELECT * FROM t WHERE c IN ("+strings.Join(ids, ",")+")", true)
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			b.SetBytes(int64(len(masked)))
			for i := 0; i < b.N; i++ {
				UnmaskStringLiterals(masked, masks)
			}
		})
	}
}
