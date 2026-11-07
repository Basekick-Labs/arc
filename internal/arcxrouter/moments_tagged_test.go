// agg-6a companion: re-validation and the shadow tolerance set live in the
// tagged router.go / compare_agg.go.

//go:build cgo && arcx_engine

package arcxrouter

import "testing"

func TestIsAggItemMoments(t *testing.T) {
	for _, item := range []string{"stddev(v)", "var_pop(cpu_user)", "variance(X)", "sem(v)", "mean(v)", "stddev_samp(v)", "stddev_pop(v)", "var_samp(v)"} {
		if !isAggItem(item) {
			t.Fatalf("isAggItem(%q) = false", item)
		}
	}
	for _, item := range []string{"skewness(v)", "stddev(a, b)", "stddev(m['k'])", "stddev(v) FROM x", "stddev()"} {
		if isAggItem(item) {
			t.Fatalf("isAggItem(%q) = true", item)
		}
	}
}

func TestAggItemTolerantMoments(t *testing.T) {
	for _, item := range []string{"sum(v)", "avg(v)", "mean(v)", "stddev(v)", "var_pop(v)", "var_samp(v)", "variance(v)", "stddev_samp(v)", "stddev_pop(v)", "sem(v)"} {
		if !aggItemTolerant(item) {
			t.Fatalf("aggItemTolerant(%q) = false", item)
		}
	}
	for _, item := range []string{"count(*)", "count(v)", "min(v)", "max(v)", "arg_max(v, time)", "summary(v)"} {
		if aggItemTolerant(item) {
			t.Fatalf("aggItemTolerant(%q) = true", item)
		}
	}
}
