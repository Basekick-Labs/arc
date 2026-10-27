package storage

import "testing"

func TestRetiredPathValid(t *testing.T) {
	for p, want := range map[string]bool{
		"db/cpu/2026/x.parquet":               true,
		"db/x.parquet":                        false,
		"_delete_retired/x.parquet/y.parquet": false,
		"db/cpu/../x.parquet":                 false,
		"/db/cpu/x.parquet":                   false,
		"db/cpu/x.json":                       false,
		"db//x.parquet":                       false,
	} {
		if got := retiredPathValid(p); got != want {
			t.Errorf("retiredPathValid(%q) = %v, want %v", p, got, want)
		}
	}
}

func TestParseRetired(t *testing.T) {
	p := "db/cpu/2026/x.parquet"
	data := []byte(`{"v":1,"path":"` + p + `"}`)
	if got, ok := ParseRetired(RetiredKey(p), data); !ok || got != p {
		t.Fatalf("own key: %q %v", got, ok)
	}
	if _, ok := ParseRetired(DeleteRetiredRoot+"/other.json", data); ok {
		t.Fatal("trusted a record under another key")
	}
}
