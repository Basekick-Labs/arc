package ingest

// Sizes of WAL row entries, for a writer that reserves WAL space before it
// builds its batches (AdmitWholeRequest): a format whose rows repeat values
// its body sends once, such as Prometheus Remote Write's labels, appends far
// more to the WAL than its body. The typed path (encodeTypedWALRows) and the
// columnar-record path (columnarToWALRecords then msgpack.Marshal, which
// remote write takes) write the same bytes: numbers at full width (9 bytes),
// NULL cells left out. Pinned against encodeTypedWALRows by
// TestWALRowSizesMatchEncoder and against the remote-write path by
// TestPrometheusRemoteWriteReservesItsWALBytes (internal/api).

// WALNumberBytes is the size of an int64 or float64 cell: encodeTypedWALRows
// writes both at full width (msgpack int64/float64, 9 bytes).
const WALNumberBytes = 9

// WALStringBytes is the size of s as a msgpack string (header and bytes).
func WALStringBytes(s string) int { return msgpackStrHeader(len(s)) + len(s) }

// WALRowBytes is the size of one row entry with cells present columns (not
// counting _database and _measurement) whose names and values take
// cellBytes.
func WALRowBytes(database, measurement string, cells, cellBytes int) int {
	return msgpackLenHeader(cells+2) + cellBytes +
		WALStringBytes("_database") + WALStringBytes(database) +
		WALStringBytes("_measurement") + WALStringBytes(measurement)
}

// WALRowsHeaderBytes is the size of the array header of an entry of rows rows.
func WALRowsHeaderBytes(rows int) int { return msgpackLenHeader(rows) }

func msgpackStrHeader(n int) int {
	switch {
	case n < 32:
		return 1
	case n < 256:
		return 2
	case n <= 0xffff:
		return 3
	default:
		return 5
	}
}

// msgpackLenHeader is the header size of an array or map of n elements.
func msgpackLenHeader(n int) int {
	switch {
	case n < 16:
		return 1
	case n <= 0xffff:
		return 3
	default:
		return 5
	}
}
