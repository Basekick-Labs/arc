package api

import "testing"

func TestParseExecutionLimit(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int
		wantErr bool
	}{
		{name: "default", raw: "", want: 50},
		{name: "zero is bounded", raw: "0", want: 0},
		{name: "maximum", raw: "1000", want: 1000},
		{name: "negative", raw: "-1", wantErr: true},
		{name: "above maximum", raw: "1001", wantErr: true},
		{name: "non numeric", raw: "abc", wantErr: true},
		{name: "fractional", raw: "1.5", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseExecutionLimit(tt.raw)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseExecutionLimit(%q) error = %v, wantErr %v", tt.raw, err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Fatalf("parseExecutionLimit(%q) = %d, want %d", tt.raw, got, tt.want)
			}
		})
	}
}
