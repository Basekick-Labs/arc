package ingest

import (
	"context"
	"fmt"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/wal"
)

func (b *ArrowBuffer) HasReplicationPublisher() bool { return b.replicationPublisher != nil }

// ApplyReplicatedWAL is shared by the authenticated live stream and received
// WAL recovery. One originating ingest entry covers exactly one measurement;
// grouping a heterogeneous WAL entry and checkpointing just one group would
// incorrectly declare the remaining rows durable.
func (b *ArrowBuffer) ApplyReplicatedWAL(ctx context.Context, payload []byte) error {
	identity, logical, err := wal.TrackedPayload(payload)
	if err != nil {
		return err
	}
	enveloped := len(logical) > 0 && logical[0] == wal.WALEnvelopeMarker
	database, body := wal.ParseEnvelope(logical, "default")
	var columnar map[string]interface{}
	if err := msgpack.Unmarshal(body, &columnar); err == nil {
		decoder := &MessagePackDecoder{}
		measurement, err := decoder.extractMeasurement(columnar["m"])
		if err != nil || measurement == "" {
			return fmt.Errorf("replicated columnar entry lacks a measurement")
		}
		rawColumns, ok := columnar["columns"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("replicated entry lacks columns")
		}
		columns := make(map[string][]interface{}, len(rawColumns))
		for key, value := range rawColumns {
			if values, ok := value.([]interface{}); ok {
				columns[key] = values
			}
		}
		return b.WriteReplicatedColumnar(ctx, database, measurement, columns, identity)
	}
	var rows []map[string]interface{}
	if err := msgpack.Unmarshal(body, &rows); err != nil || len(rows) == 0 {
		return fmt.Errorf("unsupported replicated WAL payload")
	}
	measurement := ""
	columns := make(map[string][]interface{})
	for index, row := range rows {
		name, _ := row["_measurement"].(string)
		if name == "" {
			name, _ = row["measurement"].(string)
		}
		if name == "" {
			name, _ = row["m"].(string)
		}
		if name == "" {
			return fmt.Errorf("replicated row lacks a measurement")
		}
		rowDB := database
		if !enveloped {
			rowDB = "default"
			if value, ok := row["_database"].(string); ok && value != "" {
				rowDB = value
			} else if value, ok := row["database"].(string); ok && value != "" {
				rowDB = value
			}
		}
		if index == 0 {
			measurement = name
			database = rowDB
		} else if name != measurement || rowDB != database {
			return fmt.Errorf("replicated WAL entry spans multiple measurements or databases")
		}
		for key, value := range row {
			switch key {
			case "_measurement", "measurement", "m", "_database", "database":
				continue
			}
			if columns[key] == nil {
				columns[key] = make([]interface{}, len(rows))
			}
			columns[key][index] = value
		}
	}
	return b.WriteReplicatedColumnar(ctx, database, measurement, columns, identity)
}
