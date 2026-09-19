package fieldschema

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// ArrowTypeFromDuckDB maps a DuckDB type name as printed by DESCRIBE to the
// Arrow type an anchor stores. The mapping is closed on purpose: a type that
// is not listed returns ok=false and bootstrap leaves that field out of the
// anchor. Leaving a field out is safe (it binds wherever files carry it,
// exactly as today), while guessing a type is not.
func ArrowTypeFromDuckDB(name string) (arrow.DataType, bool) {
	n := strings.ToUpper(strings.TrimSpace(name))
	switch n {
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean, true
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8, true
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16, true
	case "INTEGER":
		return arrow.PrimitiveTypes.Int32, true
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, true
	case "FLOAT", "REAL":
		return arrow.PrimitiveTypes.Float32, true
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64, true
	case "VARCHAR", "TEXT", "STRING":
		return arrow.BinaryTypes.String, true
	case "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ":
		return arrow.FixedWidthTypes.Timestamp_us, true
	case "TIMESTAMP":
		return &arrow.TimestampType{Unit: arrow.Microsecond}, true
	case "DATE":
		return arrow.FixedWidthTypes.Date32, true
	case "BLOB":
		return arrow.BinaryTypes.Binary, true
	}
	if strings.HasPrefix(n, "DECIMAL(") && strings.HasSuffix(n, ")") {
		parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(n, "DECIMAL("), ")"), ",")
		if len(parts) == 2 {
			p, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
			s, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
			if err1 == nil && err2 == nil && p > 0 && p <= 38 && s >= 0 && s <= p {
				return &arrow.Decimal128Type{Precision: int32(p), Scale: int32(s)}, true
			}
		}
	}
	return nil, false
}

// DuckDBTypeName renders an anchor field's type the way DuckDB names it, for
// the schema API. Unknown Arrow types fall back to their Arrow name.
func DuckDBTypeName(t arrow.DataType) string {
	switch t.ID() {
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.INT8:
		return "TINYINT"
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INTEGER"
	case arrow.INT64:
		return "BIGINT"
	case arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.STRING, arrow.LARGE_STRING:
		return "VARCHAR"
	case arrow.TIMESTAMP:
		if t.(*arrow.TimestampType).TimeZone != "" {
			return "TIMESTAMP WITH TIME ZONE"
		}
		return "TIMESTAMP"
	case arrow.DATE32, arrow.DATE64:
		return "DATE"
	case arrow.BINARY, arrow.LARGE_BINARY:
		return "BLOB"
	case arrow.DECIMAL128, arrow.DECIMAL256:
		d := t.(arrow.DecimalType)
		return fmt.Sprintf("DECIMAL(%d,%d)", d.GetPrecision(), d.GetScale())
	}
	return t.String()
}
