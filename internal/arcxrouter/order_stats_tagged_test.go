// agg-7 companion: re-validation and the shadow tolerance set live in the tagged
// router.go / compare_agg.go.

//go:build cgo && arcx_engine

package arcxrouter

import "testing"

func TestIsAggItemOrderStats(t *testing.T) {
	for _, item := range []string{"median(v)", "mad(v)", "mode(code)", "quantile_cont(v, 0.99)", "quantile_disc(v, 0)", "quantile(v, 1.00)"} {
		if !isAggItem(item) {
			t.Fatalf("isAggItem(%q) = false", item)
		}
	}
	for _, item := range []string{"quantile_cont(v, 1.5)", "quantile_cont(v)", "quantile_cont(v, 0.5) FROM x", "quantile_cont(v,0.5)", "quantile_cont(v, .5)", "quantile_cont(a.b, 0.5)"} {
		if isAggItem(item) {
			t.Fatalf("isAggItem(%q) = true", item)
		}
	}
}

func TestAggItemTolerantOrderStats(t *testing.T) {
	for _, item := range []string{"median(v)", "quantile_cont(v, 0.5)", "mad(v)"} {
		if !aggItemTolerant(item) {
			t.Fatalf("aggItemTolerant(%q) = false", item)
		}
	}
	for _, item := range []string{"quantile_disc(v, 0.5)", "quantile(v, 0.5)", "mode(v)"} {
		if aggItemTolerant(item) {
			t.Fatalf("aggItemTolerant(%q) = true", item)
		}
	}
}
