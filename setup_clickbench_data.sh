#!/bin/bash
# Setup ClickBench data by copying parquet file directly to Arc storage

set -e

PARQUET_FILE="${PARQUET_FILE:-/Users/nacho/dev/exydata.ventures/historian_product/benchmarks/clickbench/data/hits.parquet}"
DATABASE="clickbench"
TABLE="hits"
STORAGE_BASE="data/arc"

echo "=" 
echo "Setting up ClickBench data in Arc storage"
echo "="
echo "Source: $PARQUET_FILE"
echo "Target: $STORAGE_BASE/$DATABASE/$TABLE/"
echo "="

# Check if parquet file exists
if [ ! -f "$PARQUET_FILE" ]; then
    echo "Error: Parquet file not found: $PARQUET_FILE"
    exit 1
fi

# Create directory structure
# Arc expects: database/table/[partitions]/file.parquet
TARGET_DIR="$STORAGE_BASE/$DATABASE/$TABLE"
mkdir -p "$TARGET_DIR"

echo "Creating target directory: $TARGET_DIR"

# Option 1: Copy file directly (simplest)
echo "Copying parquet file..."
cp "$PARQUET_FILE" "$TARGET_DIR/hits.parquet"

FILE_SIZE=$(ls -lh "$TARGET_DIR/hits.parquet" | awk '{print $5}')
echo "✓ Copied: $FILE_SIZE"

echo ""
echo "=" 
echo "Setup complete!"
echo "="
echo ""
echo "The data is now available at: $DATABASE.$TABLE"
echo "You can query it with:"
echo "  SELECT COUNT(*) FROM $DATABASE.$TABLE"
echo ""
