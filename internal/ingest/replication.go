package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/basekick-labs/arc/internal/replicaview"
)

// The sentinel belongs only to the in-memory buffer key. It never forms part
// of a database, measurement or storage path.
const replicaBufferPrefix = "\x00replica\x00"

// ReplicationPublisher atomically publishes a successfully persisted file to
// the query view. A replica file must not enter the ordinary file manifest.
type ReplicationPublisher interface {
	PublishReplicationFile(context.Context, string, replicaview.FileMetadata, int64, string) error
}

func (b *ArrowBuffer) SetReplicationPublisher(publisher ReplicationPublisher) {
	b.replicationPublisher = publisher
}

// WriteReplicatedColumnar accepts one originating WAL entry for one
// measurement. The identity covers all its rows, including all hour partitions.
func (b *ArrowBuffer) WriteReplicatedColumnar(ctx context.Context, database, measurement string, columns map[string][]interface{}, identity string) error {
	if len(columns["time"]) == 0 {
		return fmt.Errorf("replicated WAL entry must preserve originating timestamps")
	}
	if b.replicationPublisher == nil {
		return fmt.Errorf("replica query view is not configured")
	}
	if _, _, err := replicaview.ParseIdentity(identity); err != nil {
		return err
	}
	if known, ok := b.replicationPublisher.(interface {
		HasEntry(string, string, string) bool
	}); ok && known.HasEntry(database, measurement, identity) {
		return nil
	}
	key := database + "\x00" + measurement + "\x00" + identity
	if _, exists := b.replicaInFlight.LoadOrStore(key, struct{}{}); exists {
		return nil
	}
	if err := b.writeColumnarDirect(ctx, database, measurement, columns, identity, true); err != nil {
		b.replicaInFlight.Delete(key)
		return err
	}
	return nil
}

func (b *ArrowBuffer) writeReplicationParquet(ctx context.Context, database, measurement string, batch *TypedColumnBatch, hour time.Time, sortKeys []string) ([]byte, *arrow.Schema, replicaview.FileMetadata, error) {
	if b.replicationPublisher == nil && b.fileRegistrar == nil {
		sorted := sortTypedColumnBatchByKeys(batch, sortKeys)
		data, schema, err := b.writer.writeParquetColumnarWithSchema(ctx, measurement, sorted.Data, sorted.Validity, sorted.TagColumns, sorted.DedupTime, b.getDecimalColumns(measurement))
		return data, schema, replicaview.FileMetadata{Database: database, Measurement: measurement, Hour: hour.Unix() / 3600}, err
	}

	coverage, err := replicaview.FromIdentities(batch.WALHashes)
	if err != nil {
		return nil, nil, replicaview.FileMetadata{}, err
	}
	metadata := replicaview.FileMetadata{Database: database, Measurement: measurement, Hour: hour.Unix() / 3600, Coverage: coverage, Segments: batch.ReplicaSegments}
	output := batch
	if !metadata.IsReplica() {
		output = sortTypedColumnBatchByKeys(batch, sortKeys)
	}
	var fileMetadata []replicaview.FileMetadata
	if b.replicationPublisher != nil || b.fileRegistrar != nil {
		fileMetadata = []replicaview.FileMetadata{metadata}
	}
	data, schema, err := b.writer.writeParquetColumnarWithSchema(ctx, measurement, output.Data, output.Validity, output.TagColumns, output.DedupTime, b.getDecimalColumns(measurement), fileMetadata...)
	if schema != nil {
		metadata.Columns = make([]string, len(schema.Fields()))
		for i, field := range schema.Fields() {
			metadata.Columns[i] = field.Name
		}
	}
	return data, schema, metadata, err
}

func (b *ArrowBuffer) publishReplicationFile(ctx context.Context, path string, metadata replicaview.FileMetadata, size int64, hash string) error {
	if b.replicationPublisher != nil {
		if err := b.replicationPublisher.PublishReplicationFile(ctx, path, metadata, size, hash); err != nil {
			return err
		}
	}
	if metadata.IsReplica() {
		return nil
	}
	b.registerFileInTiering(ctx, metadata.Database, metadata.Measurement, path, metadata.PartitionTime(), size, hash, metadata.PartitionCoverages())
	return nil
}

func (b *ArrowBuffer) releaseReplicaInFlight(database, measurement string, identities []string) {
	for _, identity := range identities {
		b.replicaInFlight.Delete(database + "\x00" + measurement + "\x00" + identity)
	}
}
