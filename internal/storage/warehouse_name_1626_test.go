package storage

import "testing"

func TestWarehouseRootName_Issue1626(t *testing.T) {
	t.Cleanup(func() { SetWarehouseRootName("") })
	if IsWarehouseRootName("lv_wh") || IsWarehouseRootName("") {
		t.Fatal("unset: a name matched")
	}
	SetWarehouseRootName("lv_wh")
	if !IsWarehouseRootName("lv_wh") {
		t.Fatal("set: lv_wh did not match")
	}
	for _, n := range []string{"lv_wh-other", "lv_w", "LV_WH", ""} {
		if IsWarehouseRootName(n) {
			t.Errorf("set: %q matched", n)
		}
	}
	SetWarehouseRootName("")
	if IsWarehouseRootName("lv_wh") || IsWarehouseRootName("") {
		t.Fatal("cleared: a name matched")
	}
}

// The per-request cost every writer pays (#1626): one atomic load, and a string compare when
// a subdirectory warehouse is configured. No allocation.
func BenchmarkIsWarehouseRootName(b *testing.B) {
	for _, tc := range []struct{ name, set string }{{"unset", ""}, {"set", "lv_wh"}} {
		b.Run(tc.name, func(b *testing.B) {
			SetWarehouseRootName(tc.set)
			defer SetWarehouseRootName("")
			b.ReportAllocs()
			var hit bool
			for i := 0; i < b.N; i++ {
				hit = IsWarehouseRootName("production")
			}
			if hit {
				b.Fatal("production matched")
			}
		})
	}
}
