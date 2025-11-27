package models

import "time"

// Record represents a single time-series data point
// This is the internal format used throughout Arc
type Record struct {
	Measurement string                 `json:"measurement"`
	Time        time.Time              `json:"time"`
	Timestamp   int64                  `json:"timestamp"` // Microseconds since epoch (for optimized path)
	Fields      map[string]interface{} `json:"fields"`
	Tags        map[string]string      `json:"tags"`
}

// ColumnarRecord represents data in columnar format (zero-copy, fastest)
// This format avoids row-to-column conversion and is passed directly to Arrow
type ColumnarRecord struct {
	Measurement string                   `json:"measurement"`
	Columnar    bool                     `json:"_columnar"` // Marker for columnar format
	Columns     map[string][]interface{} `json:"columns"`   // Column name -> array of values
	TimeUnit    string                   `json:"_time_unit,omitempty"`
	RawPayload  []byte                   `json:"-"`         // Original msgpack bytes for zero-copy WAL
}

// MsgPackPayload represents the top-level MessagePack payload structure
type MsgPackPayload struct {
	// Single measurement (row format)
	M      interface{}            `msgpack:"m,omitempty"`      // measurement (string or int)
	T      interface{}            `msgpack:"t,omitempty"`      // timestamp
	H      interface{}            `msgpack:"h,omitempty"`      // host
	Fields map[string]interface{} `msgpack:"fields,omitempty"` // fields
	F      interface{}            `msgpack:"f,omitempty"`      // compact fields (array)
	Tags   map[string]string      `msgpack:"tags,omitempty"`   // tags

	// Columnar format
	Columns map[string][]interface{} `msgpack:"columns,omitempty"`

	// Batch format
	Batch []interface{} `msgpack:"batch,omitempty"`
}
