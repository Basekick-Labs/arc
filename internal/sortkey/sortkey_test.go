package sortkey

import "testing"

func TestParse(t *testing.T) {
	tests := []struct {
		spec string
		want Key
	}{
		{spec: "time", want: Key{Column: "time"}},
		{spec: " time:desc ", want: Key{Column: "time", Desc: true}},
		{spec: "host:ASC", want: Key{Column: "host"}},
		{spec: "field:with:colon:desc", want: Key{Column: "field:with:colon", Desc: true}},
		{spec: "field:with:colon", want: Key{Column: "field:with:colon"}},
	}
	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			if got := Parse(tt.spec); got != tt.want {
				t.Errorf("Parse(%q) = %#v, want %#v", tt.spec, got, tt.want)
			}
		})
	}
}

func TestTimeKey(t *testing.T) {
	if got := TimeKey([]string{"host"}); got != "time:desc" {
		t.Fatalf("TimeKey without explicit time = %q, want time:desc", got)
	}
	if got := TimeKey([]string{"host", "time"}); got != "time" {
		t.Fatalf("TimeKey with legacy explicit time = %q, want time", got)
	}
}
