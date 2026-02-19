// Package ingest provides data ingestion functionality for Arc.
// This file implements a Two-Line Element (TLE) parser for satellite orbital data.
//
// TLE Format:
//
//	ISS (ZARYA)
//	1 25544U 98067A   26048.50000000  .00016717  00000-0  10270-3 0  9006
//	2 25544  51.6416 247.4627 0006703 130.5360 325.0288 15.72125391563537
//
// Line 0: Satellite name (up to 24 chars, optional)
// Line 1: NORAD ID, classification, designator, epoch, mean motion derivatives, BSTAR, checksum
// Line 2: Inclination, RAAN, eccentricity, arg perigee, mean anomaly, mean motion, rev number, checksum
package ingest

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	muEarth     = 3.986004418e14 // m³/s² (WGS-84 standard gravitational parameter)
	radiusEarth = 6371.0         // km (mean Earth radius)
	twoPi       = 2 * math.Pi
)

// TLERecord holds all parsed fields from a single TLE entry.
type TLERecord struct {
	// Line 0
	ObjectName string

	// Line 1
	NoradID                 string
	Classification          string
	InternationalDesignator string
	EpochYear               int
	EpochDay                float64
	EpochTime               time.Time
	EpochTimestampUs        int64
	MeanMotionDot           float64 // rev/day²
	MeanMotionDDot          float64 // rev/day³ (modified exponential)
	BStar                   float64 // 1/earth radii (modified exponential)
	EphemerisType           int
	ElementSetNumber        int

	// Line 2
	InclinationDeg   float64
	RAANDeg          float64
	Eccentricity     float64
	ArgPerigeeDeg    float64
	MeanAnomalyDeg  float64
	MeanMotionRevDay float64
	RevolutionNumber int

	// Derived
	SemiMajorAxisKm float64
	PeriodMin       float64
	PerigeeKm       float64
	ApogeeKm        float64
	OrbitType       string // LEO, MEO, GEO, HEO
}

// TLEParser parses Two-Line Element set files.
type TLEParser struct{}

// NewTLEParser creates a new TLE parser.
func NewTLEParser() *TLEParser {
	return &TLEParser{}
}

// ParseTLEFile parses a complete TLE file containing one or more satellites.
// Returns parsed records and any warnings (e.g., checksum failures).
// Entries with invalid checksums are skipped with a warning, not a fatal error.
// Uses bytes-based parsing to avoid full-payload string copies.
func (p *TLEParser) ParseTLEFile(data []byte) ([]TLERecord, []string) {
	// Split on \n directly — no full-payload string conversion
	rawLines := bytes.Split(data, []byte{'\n'})

	// Filter blank lines and trim \r in a single pass
	lines := make([]string, 0, len(rawLines))
	for _, raw := range rawLines {
		// Trim trailing \r, spaces, tabs (handles CRLF, CR-only, trailing whitespace)
		trimmed := trimRightBytes(raw)
		if len(trimmed) > 0 {
			lines = append(lines, string(trimmed))
		}
	}

	if len(lines) == 0 {
		return nil, nil
	}

	// Pre-allocate contiguous record slice (single alloc for all records)
	records := make([]TLERecord, 0, len(lines)/2)
	var warnings []string
	const maxWarnings = 100
	warningCount := 0

	i := 0
	entryNum := 0
	for i < len(lines) {
		entryNum++
		var name, line1, line2 string

		// Per-entry format detection: check if current line starts with "1 " (2-line)
		// or is a name line (3-line). This handles mixed-format files correctly.
		if len(lines[i]) >= 2 && lines[i][0] == '1' && lines[i][1] == ' ' {
			// 2-line format: current line is line 1
			if i+1 >= len(lines) {
				break
			}
			line1 = lines[i]
			line2 = lines[i+1]
			name = "NORAD " + trimSpaces(line1[2:7])
			i += 2
		} else {
			// 3-line format: current line is name
			if i+2 >= len(lines) {
				break
			}
			name = lines[i]
			line1 = lines[i+1]
			line2 = lines[i+2]
			i += 3
		}

		// Grow slice by one and fill in-place (avoids per-record heap alloc)
		records = append(records, TLERecord{})
		rec := &records[len(records)-1]
		if err := p.parseTLEEntry(name, line1, line2, rec); err != nil {
			records = records[:len(records)-1] // undo append
			warningCount++
			if len(warnings) < maxWarnings {
				warnings = append(warnings, fmt.Sprintf("entry %d (%s): %v", entryNum, trimSpaces(name), err))
			} else if len(warnings) == maxWarnings {
				warnings = append(warnings, fmt.Sprintf("... and %d more warnings suppressed", warningCount-maxWarnings))
			}
			continue
		}
	}

	// Update the suppressed count if we exceeded max
	if warningCount > maxWarnings && len(warnings) > maxWarnings {
		warnings[maxWarnings] = fmt.Sprintf("... and %d more warnings suppressed", warningCount-maxWarnings)
	}

	return records, warnings
}

// ParseTLEEntry parses a single 3-line TLE entry. Public API for individual entries.
func (p *TLEParser) ParseTLEEntry(name, line1, line2 string) (*TLERecord, error) {
	rec := &TLERecord{}
	if err := p.parseTLEEntry(name, line1, line2, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// parseTLEEntry fills a TLERecord in-place from a 3-line TLE entry.
func (p *TLEParser) parseTLEEntry(name, line1, line2 string, rec *TLERecord) error {
	// Validate line lengths
	if len(line1) < 69 {
		return fmt.Errorf("line 1 too short (%d chars, need 69)", len(line1))
	}
	if len(line2) < 69 {
		return fmt.Errorf("line 2 too short (%d chars, need 69)", len(line2))
	}
	if line1[0] != '1' {
		return fmt.Errorf("line 1 does not start with '1'")
	}
	if line2[0] != '2' {
		return fmt.Errorf("line 2 does not start with '2'")
	}

	// Validate checksums
	if !validateChecksum(line1) {
		return fmt.Errorf("line 1 checksum mismatch")
	}
	if !validateChecksum(line2) {
		return fmt.Errorf("line 2 checksum mismatch")
	}

	rec.ObjectName = trimSpaces(name)

	if err := p.parseLine1(line1, rec); err != nil {
		return fmt.Errorf("line 1: %w", err)
	}
	if err := p.parseLine2(line2, rec); err != nil {
		return fmt.Errorf("line 2: %w", err)
	}

	// Compute epoch timestamp
	rec.EpochTime = epochToTime(rec.EpochYear, rec.EpochDay)
	rec.EpochTimestampUs = rec.EpochTime.UnixMicro()

	// Compute derived orbital mechanics
	computeDerivedMetrics(rec)

	return nil
}

// parseLine1 extracts fields from TLE line 1 (fixed-width columns).
// Col positions are 1-indexed per the TLE spec.
func (p *TLEParser) parseLine1(line string, rec *TLERecord) error {
	// Cols 3-7: Satellite number
	rec.NoradID = trimSpaces(line[2:7])

	// Col 8: Classification
	rec.Classification = string(line[7])

	// Cols 10-17: International designator
	rec.InternationalDesignator = trimSpaces(line[9:17])

	// Cols 19-20: Epoch year (2-digit)
	epochYr, err := strconv.Atoi(trimSpaces(line[18:20]))
	if err != nil {
		return fmt.Errorf("epoch year: %w", err)
	}
	rec.EpochYear = epochYr

	// Cols 21-32: Epoch day (fractional day of year)
	epochDay, err := strconv.ParseFloat(trimSpaces(line[20:32]), 64)
	if err != nil {
		return fmt.Errorf("epoch day: %w", err)
	}
	rec.EpochDay = epochDay

	// Cols 34-43: 1st derivative of mean motion (rev/day²)
	mmDot, err := strconv.ParseFloat(trimSpaces(line[33:43]), 64)
	if err != nil {
		return fmt.Errorf("mean motion dot: %w", err)
	}
	rec.MeanMotionDot = mmDot

	// Cols 45-52: 2nd derivative of mean motion (modified exponential)
	mmDDot, err := parseModifiedExponential(line[44:52])
	if err != nil {
		return fmt.Errorf("mean motion ddot: %w", err)
	}
	rec.MeanMotionDDot = mmDDot

	// Cols 54-61: BSTAR drag term (modified exponential)
	bstar, err := parseModifiedExponential(line[53:61])
	if err != nil {
		return fmt.Errorf("bstar: %w", err)
	}
	rec.BStar = bstar

	// Col 63: Ephemeris type
	ephType := trimSpaces(string(line[62]))
	if ephType != "" {
		rec.EphemerisType, _ = strconv.Atoi(ephType)
	}

	// Cols 65-68: Element set number
	elSetStr := trimSpaces(line[64:68])
	if elSetStr != "" {
		rec.ElementSetNumber, _ = strconv.Atoi(elSetStr)
	}

	return nil
}

// parseLine2 extracts fields from TLE line 2 (fixed-width columns).
func (p *TLEParser) parseLine2(line string, rec *TLERecord) error {
	// Verify satellite numbers match (cols 3-7 of both lines)
	noradID2 := trimSpaces(line[2:7])
	if noradID2 != rec.NoradID {
		return fmt.Errorf("satellite number mismatch: line1=%s line2=%s", rec.NoradID, noradID2)
	}

	// Cols 9-16: Inclination (degrees)
	inc, err := strconv.ParseFloat(trimSpaces(line[8:16]), 64)
	if err != nil {
		return fmt.Errorf("inclination: %w", err)
	}
	rec.InclinationDeg = inc

	// Cols 18-25: RAAN (degrees)
	raan, err := strconv.ParseFloat(trimSpaces(line[17:25]), 64)
	if err != nil {
		return fmt.Errorf("raan: %w", err)
	}
	rec.RAANDeg = raan

	// Cols 27-33: Eccentricity (implied leading "0.")
	eccStr := trimSpaces(line[26:33])
	ecc, err := strconv.ParseFloat("0."+eccStr, 64)
	if err != nil {
		return fmt.Errorf("eccentricity: %w", err)
	}
	rec.Eccentricity = ecc

	// Cols 35-42: Argument of perigee (degrees)
	argP, err := strconv.ParseFloat(trimSpaces(line[34:42]), 64)
	if err != nil {
		return fmt.Errorf("arg perigee: %w", err)
	}
	rec.ArgPerigeeDeg = argP

	// Cols 44-51: Mean anomaly (degrees)
	ma, err := strconv.ParseFloat(trimSpaces(line[43:51]), 64)
	if err != nil {
		return fmt.Errorf("mean anomaly: %w", err)
	}
	rec.MeanAnomalyDeg = ma

	// Cols 53-63: Mean motion (rev/day)
	mm, err := strconv.ParseFloat(trimSpaces(line[52:63]), 64)
	if err != nil {
		return fmt.Errorf("mean motion: %w", err)
	}
	rec.MeanMotionRevDay = mm

	// Cols 64-68: Revolution number at epoch
	revStr := trimSpaces(line[63:68])
	if revStr != "" {
		rec.RevolutionNumber, _ = strconv.Atoi(revStr)
	}

	return nil
}

// validateChecksum verifies the mod-10 checksum of a TLE line.
// Digits add their face value, '-' adds 1, everything else adds 0.
func validateChecksum(line string) bool {
	if len(line) < 69 {
		return false
	}
	sum := 0
	for i := 0; i < 68; i++ {
		ch := line[i]
		if ch >= '0' && ch <= '9' {
			sum += int(ch - '0')
		} else if ch == '-' {
			sum++
		}
	}
	expected := int(line[68] - '0')
	return (sum % 10) == expected
}

// trimRightBytes trims trailing \r, spaces, and tabs from a byte slice.
// Faster than bytes.TrimRight for the ASCII-only TLE format.
func trimRightBytes(b []byte) []byte {
	i := len(b)
	for i > 0 && (b[i-1] == '\r' || b[i-1] == ' ' || b[i-1] == '\t') {
		i--
	}
	return b[:i]
}

// trimSpaces trims leading and trailing spaces from a string.
// For TLE fixed-width fields, this is equivalent to strings.TrimSpace
// but avoids the unicode-aware path since TLE is always ASCII.
func trimSpaces(s string) string {
	start := 0
	for start < len(s) && s[start] == ' ' {
		start++
	}
	end := len(s)
	for end > start && s[end-1] == ' ' {
		end--
	}
	return s[start:end]
}

// parseModifiedExponential converts TLE's modified exponential notation.
// Format: "SMMMMM±E" where S is sign/space, MMMMM is mantissa digits,
// ± is exponent sign, E is exponent digit(s).
// Examples: " 00000-0" → 0, "-11606-4" → -0.11606e-4, " 12345+3" → 0.12345e+3
func parseModifiedExponential(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}

	// All zeros → 0
	allZero := true
	for _, ch := range s {
		if ch != '0' && ch != ' ' && ch != '+' && ch != '-' {
			allZero = false
			break
		}
	}
	// Check for common zero patterns like "00000-0" or "00000+0"
	if allZero || s == "00000-0" || s == "00000+0" {
		return 0, nil
	}

	// Determine sign from first character
	sign := 1.0
	start := 0
	if s[0] == '-' {
		sign = -1.0
		start = 1
	} else if s[0] == '+' || s[0] == ' ' {
		start = 1
	}

	// Find the exponent delimiter (last '+' or '-' not at position 0)
	expIdx := -1
	for i := len(s) - 1; i > 0; i-- {
		if s[i] == '+' || s[i] == '-' {
			expIdx = i
			break
		}
	}
	if expIdx < 0 {
		return 0, fmt.Errorf("no exponent in modified exponential %q", s)
	}

	mantissaStr := strings.TrimSpace(s[start:expIdx])
	exponentStr := s[expIdx:]

	if mantissaStr == "" {
		return 0, nil
	}

	mantissa, err := strconv.ParseFloat("0."+mantissaStr, 64)
	if err != nil {
		return 0, fmt.Errorf("mantissa parse error in %q: %w", s, err)
	}

	exponent, err := strconv.ParseInt(exponentStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("exponent parse error in %q: %w", s, err)
	}

	return sign * mantissa * math.Pow(10, float64(exponent)), nil
}

// epochToTime converts a TLE epoch (2-digit year + fractional day-of-year) to time.Time.
// Year rule: 57-99 → 1957-1999, 00-56 → 2000-2056.
func epochToTime(year int, dayFraction float64) time.Time {
	fullYear := year
	if year >= 57 {
		fullYear = 1900 + year
	} else {
		fullYear = 2000 + year
	}

	// Day 1 = January 1, so subtract 1 to get offset from Jan 1 00:00:00
	base := time.Date(fullYear, 1, 1, 0, 0, 0, 0, time.UTC)
	offsetNs := int64((dayFraction - 1) * 24 * float64(time.Hour))
	return base.Add(time.Duration(offsetNs))
}

// computeDerivedMetrics calculates orbital mechanics from TLE elements.
func computeDerivedMetrics(rec *TLERecord) {
	if rec.MeanMotionRevDay <= 0 {
		return
	}

	// Mean motion in radians/second
	n := rec.MeanMotionRevDay * twoPi / 86400.0

	// Semi-major axis: a = (μ / n²)^(1/3), in meters → km
	rec.SemiMajorAxisKm = math.Pow(muEarth/(n*n), 1.0/3.0) / 1000.0

	// Orbital period in minutes
	rec.PeriodMin = 86400.0 / rec.MeanMotionRevDay / 60.0

	// Perigee and apogee altitude (km above Earth surface)
	rec.PerigeeKm = rec.SemiMajorAxisKm*(1-rec.Eccentricity) - radiusEarth
	rec.ApogeeKm = rec.SemiMajorAxisKm*(1+rec.Eccentricity) - radiusEarth

	// Orbit classification
	rec.OrbitType = classifyOrbit(rec.PerigeeKm, rec.ApogeeKm, rec.Eccentricity)
}

// classifyOrbit returns orbit type based on altitude and eccentricity.
func classifyOrbit(perigeeKm, apogeeKm, eccentricity float64) string {
	if perigeeKm < 0 {
		return "SUB" // sub-orbital / decaying
	}

	// Highly elliptical: eccentricity > 0.25 with high apogee
	if eccentricity > 0.25 && apogeeKm > 35786 {
		return "HEO"
	}

	// GEO: ~35,786 km ± 200 km
	avgAlt := (perigeeKm + apogeeKm) / 2
	if avgAlt > 35586 && avgAlt < 35986 {
		return "GEO"
	}

	// LEO: below 2000 km
	if apogeeKm < 2000 {
		return "LEO"
	}

	// MEO: between LEO and GEO
	if perigeeKm >= 2000 && apogeeKm <= 35786 {
		return "MEO"
	}

	return "HEO"
}

// TLERecordsToTypedColumnar converts parsed TLE records directly to a TypedColumnBatch,
// bypassing the []interface{} intermediary and convertColumnsToTyped entirely.
// All typed slices are pre-allocated at exact size and filled in a single pass.
// Returns the batch and the number of records.
func TLERecordsToTypedColumnar(records []TLERecord) (*TypedColumnBatch, int) {
	n := len(records)
	if n == 0 {
		return &TypedColumnBatch{
			Data: make(map[string]interface{}),
		}, 0
	}

	// Pre-allocate all typed slices
	timeCol := make([]int64, n)
	noradID := make([]string, n)
	objectName := make([]string, n)
	classification := make([]string, n)
	intlDesignator := make([]string, n)
	orbitType := make([]string, n)
	inclinationDeg := make([]float64, n)
	raanDeg := make([]float64, n)
	eccentricity := make([]float64, n)
	argPerigeeDeg := make([]float64, n)
	meanAnomalyDeg := make([]float64, n)
	meanMotionRevDay := make([]float64, n)
	bstar := make([]float64, n)
	meanMotionDot := make([]float64, n)
	meanMotionDDot := make([]float64, n)
	revolutionNumber := make([]float64, n) // float64 for Parquet schema compat
	semiMajorAxisKm := make([]float64, n)
	periodMin := make([]float64, n)
	apogeeKm := make([]float64, n)
	perigeeKm := make([]float64, n)

	// Single pass — fill all columns
	for i := range records {
		tle := &records[i]
		timeCol[i] = tle.EpochTimestampUs
		noradID[i] = tle.NoradID
		objectName[i] = tle.ObjectName
		classification[i] = tle.Classification
		intlDesignator[i] = tle.InternationalDesignator
		orbitType[i] = tle.OrbitType
		inclinationDeg[i] = tle.InclinationDeg
		raanDeg[i] = tle.RAANDeg
		eccentricity[i] = tle.Eccentricity
		argPerigeeDeg[i] = tle.ArgPerigeeDeg
		meanAnomalyDeg[i] = tle.MeanAnomalyDeg
		meanMotionRevDay[i] = tle.MeanMotionRevDay
		bstar[i] = tle.BStar
		meanMotionDot[i] = tle.MeanMotionDot
		meanMotionDDot[i] = tle.MeanMotionDDot
		revolutionNumber[i] = float64(tle.RevolutionNumber)
		semiMajorAxisKm[i] = tle.SemiMajorAxisKm
		periodMin[i] = tle.PeriodMin
		apogeeKm[i] = tle.ApogeeKm
		perigeeKm[i] = tle.PerigeeKm
	}

	batch := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time":                     timeCol,
			"norad_id":                 noradID,
			"object_name":              objectName,
			"classification":           classification,
			"international_designator": intlDesignator,
			"orbit_type":               orbitType,
			"inclination_deg":          inclinationDeg,
			"raan_deg":                 raanDeg,
			"eccentricity":             eccentricity,
			"arg_perigee_deg":          argPerigeeDeg,
			"mean_anomaly_deg":         meanAnomalyDeg,
			"mean_motion_rev_day":      meanMotionRevDay,
			"bstar":                    bstar,
			"mean_motion_dot":          meanMotionDot,
			"mean_motion_ddot":         meanMotionDDot,
			"revolution_number":        revolutionNumber,
			"semi_major_axis_km":       semiMajorAxisKm,
			"period_min":               periodMin,
			"apogee_km":                apogeeKm,
			"perigee_km":               perigeeKm,
		},
		// No validity bitmaps — TLE records never have null values
	}

	return batch, n
}
