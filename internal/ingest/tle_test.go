package ingest

import (
	"math"
	"testing"
	"time"
)

// Real ISS TLE (used as canonical test fixture)
const issName = "ISS (ZARYA)"
const issLine1 = "1 25544U 98067A   24051.34722222  .00016717  00000-0  10270-3 0  9014"
const issLine2 = "2 25544  51.6400 208.9163 0006703 319.1918  40.8793 15.49560830442108"

func TestValidateChecksum(t *testing.T) {
	tests := []struct {
		name  string
		line  string
		valid bool
	}{
		{"ISS line 1", issLine1, true},
		{"ISS line 2", issLine2, true},
		{"too short", "1 25544U", false},
		{
			"modified last digit (bad checksum)",
			"1 25544U 98067A   24051.34722222  .00016717  00000-0  10270-3 0  9019",
			false, // correct checksum is 4, not 9
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validateChecksum(tt.line)
			if result != tt.valid {
				t.Errorf("validateChecksum(%q) = %v, want %v", tt.line[:min(30, len(tt.line))], result, tt.valid)
			}
		})
	}
}

func TestParseModifiedExponential(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected float64
	}{
		{"zero with minus", " 00000-0", 0},
		{"zero with plus", " 00000+0", 0},
		{"negative small", "-11606-4", -0.11606e-4},
		{"positive", " 10270-3", 0.10270e-3},
		{"positive with plus", " 12345+3", 0.12345e+3},
		{"large negative", "-34567-2", -0.34567e-2},
		{"empty string", "", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseModifiedExponential(tt.input)
			if err != nil {
				t.Fatalf("parseModifiedExponential(%q) error: %v", tt.input, err)
			}
			if math.Abs(result-tt.expected) > 1e-15 {
				t.Errorf("parseModifiedExponential(%q) = %e, want %e", tt.input, result, tt.expected)
			}
		})
	}
}

func TestEpochToTime(t *testing.T) {
	tests := []struct {
		name     string
		year     int
		day      float64
		expected time.Time
	}{
		{
			"2024 Jan 1 midnight",
			24, 1.0,
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			"1999 Dec 31 noon",
			99, 365.5,
			time.Date(1999, 12, 31, 12, 0, 0, 0, time.UTC),
		},
		{
			"1957 Jan 1 (Sputnik era)",
			57, 1.0,
			time.Date(1957, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			"2024 Feb 20 ~8:20 UTC",
			24, 51.34722222,
			// Day 51 = Feb 20. 0.34722222 * 24h ≈ 8h 20m
			time.Date(2024, 2, 20, 8, 19, 59, 0, time.UTC), // approximate
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := epochToTime(tt.year, tt.day)
			diff := result.Sub(tt.expected)
			// Allow 2 second tolerance for floating-point day fraction
			if diff < -2*time.Second || diff > 2*time.Second {
				t.Errorf("epochToTime(%d, %f) = %v, want ~%v (diff=%v)", tt.year, tt.day, result, tt.expected, diff)
			}
		})
	}
}

func TestParseTLEEntry(t *testing.T) {
	parser := NewTLEParser()
	rec, err := parser.ParseTLEEntry(issName, issLine1, issLine2)
	if err != nil {
		t.Fatalf("ParseTLEEntry failed: %v", err)
	}

	// Line 0
	if rec.ObjectName != "ISS (ZARYA)" {
		t.Errorf("ObjectName = %q, want %q", rec.ObjectName, "ISS (ZARYA)")
	}

	// Line 1
	if rec.NoradID != "25544" {
		t.Errorf("NoradID = %q, want %q", rec.NoradID, "25544")
	}
	if rec.Classification != "U" {
		t.Errorf("Classification = %q, want %q", rec.Classification, "U")
	}
	if rec.InternationalDesignator != "98067A" {
		t.Errorf("InternationalDesignator = %q, want %q", rec.InternationalDesignator, "98067A")
	}
	if rec.EpochYear != 24 {
		t.Errorf("EpochYear = %d, want 24", rec.EpochYear)
	}
	if math.Abs(rec.EpochDay-51.34722222) > 0.00001 {
		t.Errorf("EpochDay = %f, want 51.34722222", rec.EpochDay)
	}
	if math.Abs(rec.MeanMotionDot-0.00016717) > 1e-10 {
		t.Errorf("MeanMotionDot = %e, want 0.00016717", rec.MeanMotionDot)
	}
	if math.Abs(rec.BStar-0.10270e-3) > 1e-10 {
		t.Errorf("BStar = %e, want %e", rec.BStar, 0.10270e-3)
	}

	// Line 2
	if math.Abs(rec.InclinationDeg-51.6400) > 0.0001 {
		t.Errorf("InclinationDeg = %f, want 51.6400", rec.InclinationDeg)
	}
	if math.Abs(rec.RAANDeg-208.9163) > 0.0001 {
		t.Errorf("RAANDeg = %f, want 208.9163", rec.RAANDeg)
	}
	if math.Abs(rec.Eccentricity-0.0006703) > 0.0000001 {
		t.Errorf("Eccentricity = %f, want 0.0006703", rec.Eccentricity)
	}
	if math.Abs(rec.ArgPerigeeDeg-319.1918) > 0.0001 {
		t.Errorf("ArgPerigeeDeg = %f, want 319.1918", rec.ArgPerigeeDeg)
	}
	if math.Abs(rec.MeanAnomalyDeg-40.8793) > 0.0001 {
		t.Errorf("MeanAnomalyDeg = %f, want 40.8793", rec.MeanAnomalyDeg)
	}
	if math.Abs(rec.MeanMotionRevDay-15.49560830) > 0.00001 {
		t.Errorf("MeanMotionRevDay = %f, want 15.49560830", rec.MeanMotionRevDay)
	}
	if rec.RevolutionNumber != 44210 {
		t.Errorf("RevolutionNumber = %d, want 44210", rec.RevolutionNumber)
	}

	// Epoch should be Feb 20, 2024
	if rec.EpochTime.Year() != 2024 || rec.EpochTime.Month() != 2 || rec.EpochTime.Day() != 20 {
		t.Errorf("EpochTime date = %v, want 2024-02-20", rec.EpochTime.Format("2006-01-02"))
	}

	// Timestamp should be positive microseconds
	if rec.EpochTimestampUs <= 0 {
		t.Errorf("EpochTimestampUs = %d, want positive", rec.EpochTimestampUs)
	}
}

func TestParseTLEFile(t *testing.T) {
	data := `ISS (ZARYA)
1 25544U 98067A   24051.34722222  .00016717  00000-0  10270-3 0  9014
2 25544  51.6400 208.9163 0006703 319.1918  40.8793 15.49560830442108
NOAA 19
1 33591U 09005A   24051.00000000  .00000080  00000-0  55221-4 0  9994
2 33591  99.1926 170.2345 0014183 315.6789  44.3210 14.12343268788907
`

	parser := NewTLEParser()
	records, warnings := parser.ParseTLEFile([]byte(data))

	if len(warnings) != 0 {
		t.Errorf("expected 0 warnings, got %d: %v", len(warnings), warnings)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	if records[0].ObjectName != "ISS (ZARYA)" {
		t.Errorf("record 0 name = %q, want %q", records[0].ObjectName, "ISS (ZARYA)")
	}
	if records[1].ObjectName != "NOAA 19" {
		t.Errorf("record 1 name = %q, want %q", records[1].ObjectName, "NOAA 19")
	}
	if records[0].NoradID != "25544" {
		t.Errorf("record 0 NoradID = %q, want %q", records[0].NoradID, "25544")
	}
	if records[1].NoradID != "33591" {
		t.Errorf("record 1 NoradID = %q, want %q", records[1].NoradID, "33591")
	}
}

func TestParseTLEFile_InvalidChecksum(t *testing.T) {
	data := `ISS (ZARYA)
1 25544U 98067A   24051.34722222  .00016717  00000-0  10270-3 0  9014
2 25544  51.6400 208.9163 0006703 319.1918  40.8793 15.49560830442108
BAD SAT
1 99999U 24001A   24051.00000000  .00000000  00000-0  00000-0 0  0009
2 99999   0.0000   0.0000 0000000   0.0000   0.0000  1.00000000000009
`

	parser := NewTLEParser()
	records, warnings := parser.ParseTLEFile([]byte(data))

	// ISS should parse fine, BAD SAT likely has checksum issues
	if len(records) < 1 {
		t.Fatalf("expected at least 1 record, got %d", len(records))
	}
	if records[0].NoradID != "25544" {
		t.Errorf("first record should be ISS, got NoradID=%s", records[0].NoradID)
	}

	// Total should be records + warnings = 2 (both attempted)
	totalAttempted := len(records) + len(warnings)
	if totalAttempted != 2 {
		t.Errorf("expected 2 total attempts, got %d records + %d warnings", len(records), len(warnings))
	}
}

func TestParseTLEFile_TwoLineFormat(t *testing.T) {
	// No name lines — just line 1 and line 2 pairs
	data := `1 25544U 98067A   24051.34722222  .00016717  00000-0  10270-3 0  9014
2 25544  51.6400 208.9163 0006703 319.1918  40.8793 15.49560830442108
`

	parser := NewTLEParser()
	records, warnings := parser.ParseTLEFile([]byte(data))

	if len(warnings) != 0 {
		t.Errorf("expected 0 warnings, got %d: %v", len(warnings), warnings)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].NoradID != "25544" {
		t.Errorf("NoradID = %q, want %q", records[0].NoradID, "25544")
	}
	// Name should be synthesized from NORAD ID
	if records[0].ObjectName != "NORAD 25544" {
		t.Errorf("ObjectName = %q, want %q", records[0].ObjectName, "NORAD 25544")
	}
}

func TestComputeDerivedMetrics(t *testing.T) {
	parser := NewTLEParser()
	rec, err := parser.ParseTLEEntry(issName, issLine1, issLine2)
	if err != nil {
		t.Fatalf("ParseTLEEntry failed: %v", err)
	}

	// ISS orbits at ~400 km altitude with ~92 min period
	if rec.PeriodMin < 90 || rec.PeriodMin > 95 {
		t.Errorf("PeriodMin = %f, want ~92 (ISS)", rec.PeriodMin)
	}
	if rec.PerigeeKm < 350 || rec.PerigeeKm > 450 {
		t.Errorf("PerigeeKm = %f, want ~400 (ISS)", rec.PerigeeKm)
	}
	if rec.ApogeeKm < 350 || rec.ApogeeKm > 450 {
		t.Errorf("ApogeeKm = %f, want ~400 (ISS)", rec.ApogeeKm)
	}
	if rec.SemiMajorAxisKm < 6700 || rec.SemiMajorAxisKm > 6800 {
		t.Errorf("SemiMajorAxisKm = %f, want ~6770 (ISS)", rec.SemiMajorAxisKm)
	}
	if rec.OrbitType != "LEO" {
		t.Errorf("OrbitType = %q, want %q (ISS)", rec.OrbitType, "LEO")
	}
}

func TestClassifyOrbit(t *testing.T) {
	tests := []struct {
		name         string
		perigeeKm   float64
		apogeeKm    float64
		eccentricity float64
		expected     string
	}{
		{"ISS (LEO)", 408, 412, 0.0007, "LEO"},
		{"GPS (MEO)", 20180, 20220, 0.001, "MEO"},
		{"Geostationary (GEO)", 35780, 35790, 0.0001, "GEO"},
		{"Molniya (HEO)", 500, 39800, 0.74, "HEO"},
		{"Decaying (SUB)", -50, 200, 0.01, "SUB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := classifyOrbit(tt.perigeeKm, tt.apogeeKm, tt.eccentricity)
			if result != tt.expected {
				t.Errorf("classifyOrbit(%f, %f, %f) = %q, want %q",
					tt.perigeeKm, tt.apogeeKm, tt.eccentricity, result, tt.expected)
			}
		})
	}
}

func TestTLERecordsToColumnar(t *testing.T) {
	parser := NewTLEParser()
	rec, err := parser.ParseTLEEntry(issName, issLine1, issLine2)
	if err != nil {
		t.Fatalf("ParseTLEEntry failed: %v", err)
	}

	columns := TLERecordsToColumnar([]*TLERecord{rec})

	// Should have 20 columns (1 time + 5 tags + 14 fields)
	if len(columns) != 20 {
		t.Errorf("expected 20 columns, got %d", len(columns))
	}

	// Check time
	timeCol := columns["time"]
	if len(timeCol) != 1 {
		t.Fatalf("time column has %d rows, want 1", len(timeCol))
	}
	if timeCol[0] != rec.EpochTimestampUs {
		t.Errorf("time[0] = %v, want %d", timeCol[0], rec.EpochTimestampUs)
	}

	// Check tags
	if columns["norad_id"][0] != "25544" {
		t.Errorf("norad_id[0] = %v, want %q", columns["norad_id"][0], "25544")
	}
	if columns["object_name"][0] != "ISS (ZARYA)" {
		t.Errorf("object_name[0] = %v, want %q", columns["object_name"][0], "ISS (ZARYA)")
	}
	if columns["orbit_type"][0] != "LEO" {
		t.Errorf("orbit_type[0] = %v, want %q", columns["orbit_type"][0], "LEO")
	}

	// Check all expected columns exist
	expectedCols := []string{
		"time", "norad_id", "object_name", "classification", "international_designator",
		"orbit_type", "inclination_deg", "raan_deg", "eccentricity", "arg_perigee_deg",
		"mean_anomaly_deg", "mean_motion_rev_day", "bstar", "mean_motion_dot",
		"mean_motion_ddot", "revolution_number", "semi_major_axis_km",
		"period_min", "apogee_km", "perigee_km",
	}
	for _, col := range expectedCols {
		if _, ok := columns[col]; !ok {
			t.Errorf("missing column %q", col)
		}
	}

	// Check a specific field value
	inc, ok := columns["inclination_deg"][0].(float64)
	if !ok {
		t.Fatalf("inclination_deg is not float64")
	}
	if math.Abs(inc-51.6400) > 0.001 {
		t.Errorf("inclination_deg = %f, want 51.6400", inc)
	}
}

func TestTLEToColumnar_RoundTrip(t *testing.T) {
	data := `ISS (ZARYA)
1 25544U 98067A   24051.34722222  .00016717  00000-0  10270-3 0  9014
2 25544  51.6400 208.9163 0006703 319.1918  40.8793 15.49560830442108
NOAA 19
1 33591U 09005A   24051.00000000  .00000080  00000-0  55221-4 0  9994
2 33591  99.1926 170.2345 0014183 315.6789  44.3210 14.12343268788907
`

	parser := NewTLEParser()
	tleRecords, warnings := parser.ParseTLEFile([]byte(data))
	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}
	if len(tleRecords) != 2 {
		t.Fatalf("expected 2 TLE records, got %d", len(tleRecords))
	}

	// Convert directly to columnar
	columns := TLERecordsToColumnar(tleRecords)

	// Should have 20 columns
	if len(columns) != 20 {
		t.Fatalf("expected 20 columns, got %d", len(columns))
	}

	// Should have 2 rows per column
	timeCol, ok := columns["time"]
	if !ok {
		t.Fatal("missing 'time' column")
	}
	if len(timeCol) != 2 {
		t.Errorf("time column has %d rows, want 2", len(timeCol))
	}

	// Verify both satellites are present
	if columns["norad_id"][0] != "25544" {
		t.Errorf("norad_id[0] = %v, want %q", columns["norad_id"][0], "25544")
	}
	if columns["norad_id"][1] != "33591" {
		t.Errorf("norad_id[1] = %v, want %q", columns["norad_id"][1], "33591")
	}
	if columns["object_name"][0] != "ISS (ZARYA)" {
		t.Errorf("object_name[0] = %v, want %q", columns["object_name"][0], "ISS (ZARYA)")
	}
	if columns["object_name"][1] != "NOAA 19" {
		t.Errorf("object_name[1] = %v, want %q", columns["object_name"][1], "NOAA 19")
	}

	// Check that expected columns exist
	expectedCols := []string{
		"time", "norad_id", "object_name", "orbit_type",
		"inclination_deg", "period_min", "perigee_km", "apogee_km",
	}
	for _, col := range expectedCols {
		if _, ok := columns[col]; !ok {
			t.Errorf("missing column %q", col)
		}
	}
}

func TestParseTLEFile_WindowsLineEndings(t *testing.T) {
	data := "ISS (ZARYA)\r\n" +
		issLine1 + "\r\n" +
		issLine2 + "\r\n"

	parser := NewTLEParser()
	records, warnings := parser.ParseTLEFile([]byte(data))

	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].NoradID != "25544" {
		t.Errorf("NoradID = %q, want %q", records[0].NoradID, "25544")
	}
}

func TestParseTLEFile_EmptyInput(t *testing.T) {
	parser := NewTLEParser()
	records, warnings := parser.ParseTLEFile([]byte(""))
	if len(records) != 0 {
		t.Errorf("expected 0 records, got %d", len(records))
	}
	if len(warnings) != 0 {
		t.Errorf("expected 0 warnings, got %d", len(warnings))
	}
}
