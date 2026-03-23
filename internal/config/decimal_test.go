package config

import (
	"reflect"
	"testing"
)

func TestParseDecimalColumns(t *testing.T) {
	tests := []struct {
		name            string
		config          IngestConfig
		wantDecimalCols map[string]map[string]DecimalSpec
		wantDefaults    map[string]DecimalSpec
		wantErr         bool
	}{
		{
			name: "empty config",
			config: IngestConfig{
				DecimalColumns:        []string{},
				DefaultDecimalColumns: "",
			},
			wantDecimalCols: map[string]map[string]DecimalSpec{},
			wantDefaults:    nil,
			wantErr:         false,
		},
		{
			name: "single measurement single column",
			config: IngestConfig{
				DecimalColumns: []string{"trades:price=18,8"},
			},
			wantDecimalCols: map[string]map[string]DecimalSpec{
				"trades": {"price": {Precision: 18, Scale: 8}},
			},
			wantDefaults: nil,
			wantErr:      false,
		},
		{
			name: "single measurement multiple columns",
			config: IngestConfig{
				DecimalColumns: []string{"trades:price=18,8;amount=18,8"},
			},
			wantDecimalCols: map[string]map[string]DecimalSpec{
				"trades": {
					"price":  {Precision: 18, Scale: 8},
					"amount": {Precision: 18, Scale: 8},
				},
			},
			wantDefaults: nil,
			wantErr:      false,
		},
		{
			name: "multiple measurements",
			config: IngestConfig{
				DecimalColumns: []string{
					"trades:price=18,8",
					"balances:amount=38,18",
				},
			},
			wantDecimalCols: map[string]map[string]DecimalSpec{
				"trades":   {"price": {Precision: 18, Scale: 8}},
				"balances": {"amount": {Precision: 38, Scale: 18}},
			},
			wantDefaults: nil,
			wantErr:      false,
		},
		{
			name: "with defaults",
			config: IngestConfig{
				DecimalColumns:        []string{},
				DefaultDecimalColumns: "value=18,6",
			},
			wantDecimalCols: map[string]map[string]DecimalSpec{},
			wantDefaults:    map[string]DecimalSpec{"value": {Precision: 18, Scale: 6}},
			wantErr:         false,
		},
		{
			name: "spaces trimmed",
			config: IngestConfig{
				DecimalColumns: []string{" trades : price = 18 , 8 ; amount = 10 , 2 "},
			},
			wantDecimalCols: map[string]map[string]DecimalSpec{
				"trades": {
					"price":  {Precision: 18, Scale: 8},
					"amount": {Precision: 10, Scale: 2},
				},
			},
			wantDefaults: nil,
			wantErr:      false,
		},
		{
			name: "precision 1 and scale 0",
			config: IngestConfig{
				DecimalColumns: []string{"m:col=1,0"},
			},
			wantDecimalCols: map[string]map[string]DecimalSpec{
				"m": {"col": {Precision: 1, Scale: 0}},
			},
			wantDefaults: nil,
			wantErr:      false,
		},
		{
			name: "max precision 38",
			config: IngestConfig{
				DecimalColumns: []string{"m:col=38,18"},
			},
			wantDecimalCols: map[string]map[string]DecimalSpec{
				"m": {"col": {Precision: 38, Scale: 18}},
			},
			wantDefaults: nil,
			wantErr:      false,
		},
		// Error cases
		{
			name: "invalid - no colon separator",
			config: IngestConfig{
				DecimalColumns: []string{"trades_price=18,8"},
			},
			wantErr: true,
		},
		{
			name: "invalid - empty measurement",
			config: IngestConfig{
				DecimalColumns: []string{":price=18,8"},
			},
			wantErr: true,
		},
		{
			name: "invalid - no equals sign",
			config: IngestConfig{
				DecimalColumns: []string{"trades:price18,8"},
			},
			wantErr: true,
		},
		{
			name: "invalid - empty column name",
			config: IngestConfig{
				DecimalColumns: []string{"trades:=18,8"},
			},
			wantErr: true,
		},
		{
			name: "invalid - precision 0",
			config: IngestConfig{
				DecimalColumns: []string{"trades:price=0,0"},
			},
			wantErr: true,
		},
		{
			name: "invalid - precision 39",
			config: IngestConfig{
				DecimalColumns: []string{"trades:price=39,8"},
			},
			wantErr: true,
		},
		{
			name: "invalid - scale exceeds precision",
			config: IngestConfig{
				DecimalColumns: []string{"trades:price=10,11"},
			},
			wantErr: true,
		},
		{
			name: "invalid - negative scale",
			config: IngestConfig{
				DecimalColumns: []string{"trades:price=10,-1"},
			},
			wantErr: true,
		},
		{
			name: "invalid - missing precision,scale",
			config: IngestConfig{
				DecimalColumns: []string{"trades:price=18"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCols, gotDefaults, err := ParseDecimalColumns(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDecimalColumns() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if !reflect.DeepEqual(gotCols, tt.wantDecimalCols) {
					t.Errorf("ParseDecimalColumns() gotCols = %v, want %v", gotCols, tt.wantDecimalCols)
				}
				if !reflect.DeepEqual(gotDefaults, tt.wantDefaults) {
					t.Errorf("ParseDecimalColumns() gotDefaults = %v, want %v", gotDefaults, tt.wantDefaults)
				}
			}
		})
	}
}
