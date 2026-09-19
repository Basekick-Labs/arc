package fieldschema

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// AnchorBasePath is the reserved root directory holding stored anchors,
// beside compaction's _compaction_state. Root-level listings in Arc skip
// directories that start with "_" or "."; see storage.IsReservedRootDir.
const AnchorBasePath = "_schema"

// AnchorKey returns the storage key of a measurement's stored anchor:
// _schema/{database}/{measurement}.parquet. database may carry a slash for a
// spoke pseudo-database ("spoke/child"), which keeps the anchor under the
// same layout the hub uses for that namespace's data.
func AnchorKey(database, measurement string) string {
	return AnchorBasePath + "/" + database + "/" + measurement + ".parquet"
}

// AnchorPrefixForDatabase returns the prefix under which every anchor of a
// database lives, for deletion and listing.
func AnchorPrefixForDatabase(database string) string {
	return AnchorBasePath + "/" + database + "/"
}

// isAnchorPath reports whether a storage key lives under the anchor root.
func isAnchorPath(key string) bool {
	return strings.HasPrefix(key, AnchorBasePath+"/")
}

// completeKey is the Parquet key-value metadata entry recording that an
// anchor is known to carry every field the measurement's files hold (#928):
// set when ingest created it for a measurement that had no files yet, or
// when a bootstrap sampled every file with every type mapped. A stored
// anchor without it is treated as incomplete.
const completeKey = "arc:schema_complete"

// EncodeAnchor writes a zero-row Parquet file carrying schema. The Parquet
// footer is all that matters: DuckDB binds the columns and types from it and
// union_by_name supplies NULLs for every other file.
func EncodeAnchor(schema *arrow.Schema) ([]byte, error) {
	return EncodeAnchorComplete(schema, false)
}

// EncodeAnchorComplete is EncodeAnchor with the completeness flag recorded.
func EncodeAnchorComplete(schema *arrow.Schema, complete bool) ([]byte, error) {
	if schema == nil || schema.NumFields() == 0 {
		return nil, fmt.Errorf("fieldschema: refusing to encode an empty anchor")
	}
	if complete {
		md := arrow.NewMetadata([]string{completeKey}, []string{"true"})
		schema = arrow.NewSchema(schema.Fields(), &md)
	}
	mem := memory.DefaultAllocator
	arrays := make([]arrow.Array, schema.NumFields())
	for i, f := range schema.Fields() {
		b := array.NewBuilder(mem, f.Type)
		arrays[i] = b.NewArray()
		b.Release()
	}
	defer func() {
		for _, a := range arrays {
			a.Release()
		}
	}()
	rec := array.NewRecord(schema, arrays, 0)
	defer rec.Release()

	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(schema, &buf,
		parquet.NewWriterProperties(parquet.WithCompression(compressionNone())),
		pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		return nil, fmt.Errorf("fieldschema: create anchor writer: %w", err)
	}
	if err := w.Write(rec); err != nil {
		w.Close()
		return nil, fmt.Errorf("fieldschema: write anchor: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("fieldschema: close anchor: %w", err)
	}
	return buf.Bytes(), nil
}

// DecodeAnchor reads the Arrow schema back out of an anchor (or any Parquet
// file). Metadata is dropped: the anchor carries columns and types only.
func DecodeAnchor(data []byte) (*arrow.Schema, error) {
	s, _, err := DecodeAnchorComplete(data)
	return s, err
}

// DecodeAnchorComplete is DecodeAnchor also returning the completeness flag.
func DecodeAnchorComplete(data []byte) (*arrow.Schema, bool, error) {
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return nil, false, fmt.Errorf("fieldschema: open anchor: %w", err)
	}
	defer pf.Close()
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, false, fmt.Errorf("fieldschema: read anchor schema: %w", err)
	}
	s, err := fr.Schema()
	if err != nil {
		return nil, false, fmt.Errorf("fieldschema: decode anchor schema: %w", err)
	}
	complete := false
	if v, ok := s.Metadata().GetValue(completeKey); ok && v == "true" {
		complete = true
	}
	return stripMetadata(s), complete, nil
}
