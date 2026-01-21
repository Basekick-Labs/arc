#!/bin/bash
# Script to reproduce Issue #144: S3 partition pruning failure
# This demonstrates the bug where queries fail when daily compaction hasn't run

set -e

echo "=== Issue #144 Reproduction Script ==="
echo ""
echo "This script will:"
echo "1. Configure Arc with S3 storage"
echo "2. Ingest test data (creates hourly partitions)"
echo "3. Query before daily compaction (should fail on old versions)"
echo ""
echo "Prerequisites:"
echo "- MinIO running locally (or S3 bucket configured)"
echo "- Arc built from source and configured for S3/MinIO"
echo ""
echo "Quick MinIO setup:"
echo "  docker run -p 9000:9000 -p 9001:9001 \\"
echo "    -e MINIO_ROOT_USER=minioadmin \\"
echo "    -e MINIO_ROOT_PASSWORD=minioadmin \\"
echo "    quay.io/minio/minio server /data --console-address :9001"
echo ""
echo "Arc configuration (arc.toml):"
echo "  [storage]"
echo "  backend = \"s3\""
echo "  s3_bucket = \"arc-test\""
echo "  s3_region = \"us-east-1\""
echo "  s3_endpoint = \"http://localhost:9000\""
echo "  s3_access_key = \"minioadmin\""
echo "  s3_secret_key = \"minioadmin\""
echo "  s3_use_ssl = false"
echo "  s3_path_style = true"
echo ""

# Configuration
DB_NAME="test_issue_144"
MEASUREMENT="cpu"
TIMESTAMP=$(date -u +"%Y-%m-%d %H:%M:%S")
YEAR=$(date -u +"%Y")
MONTH=$(date -u +"%m")
DAY=$(date -u +"%d")

# Check if Arc is running
if ! pgrep -x "arc" > /dev/null; then
    echo "ERROR: Arc is not running"
    echo "Please start Arc with S3 storage configured"
    exit 1
fi

# Get Arc endpoint
ARC_URL="${ARC_URL:-http://localhost:8000}"
echo "Using Arc at: $ARC_URL"
echo ""

# Step 1: Create database
echo "Step 1: Creating database '$DB_NAME'..."
curl -s -X POST "$ARC_URL/api/v1/databases" \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"$DB_NAME\"}" | jq .
echo ""

# Step 2: Ingest test data
echo "Step 2: Ingesting test data..."
echo "This will create hourly partitions at: s3://bucket/$DB_NAME/$MEASUREMENT/$YEAR/$MONTH/$DAY/HH/*.parquet"
echo ""

# Write 100 data points (creates hourly partition)
for i in {1..100}; do
    curl -s -X POST "$ARC_URL/write?db=$DB_NAME" \
        -H "Content-Type: text/plain" \
        -d "$MEASUREMENT,host=server$i value=$i $(($(date +%s%N) / 1000000))" > /dev/null
done

echo "✓ Ingested 100 data points"
echo ""

# Wait a moment for flush
echo "Waiting 10 seconds for buffer flush..."
sleep 10
echo ""

# Step 3: Verify data exists
echo "Step 3: Verifying data was ingested..."
TOTAL_ROWS=$(curl -s "$ARC_URL/api/v1/query?db=$DB_NAME" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"SELECT COUNT(*) as count FROM $MEASUREMENT\"}" | jq -r '.results[0].series[0].values[0][1]')

echo "✓ Total rows in database: $TOTAL_ROWS"
echo ""

# Step 4: Check partition structure
echo "Step 4: Checking partition structure on S3..."
echo "Expected hourly partition: s3://bucket/$DB_NAME/$MEASUREMENT/$YEAR/$MONTH/$DAY/HH/*.parquet"
echo "Expected day-level (after compaction): s3://bucket/$DB_NAME/$MEASUREMENT/$YEAR/$MONTH/$DAY/*.parquet"
echo ""
echo "NOTE: Day-level files won't exist because:"
echo "  - daily_min_age_hours = 24 (default)"
echo "  - Data is < 24 hours old"
echo "  - Daily compaction hasn't run yet"
echo ""

# Step 5: Run the problematic query
echo "Step 5: Running query with time filter (this triggers the bug)..."
echo ""
echo "Query: SELECT * FROM $MEASUREMENT WHERE time >= '$TIMESTAMP'"
echo ""

RESPONSE=$(curl -s "$ARC_URL/api/v1/query?db=$DB_NAME" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"SELECT * FROM $MEASUREMENT WHERE time >= '$TIMESTAMP' LIMIT 10\"}")

# Check if query succeeded
if echo "$RESPONSE" | jq -e '.error' > /dev/null; then
    ERROR_MSG=$(echo "$RESPONSE" | jq -r '.error')
    echo "❌ QUERY FAILED (Bug reproduced!)"
    echo ""
    echo "Error message:"
    echo "$ERROR_MSG"
    echo ""
    echo "This is Issue #144: Partition pruner generates day-level paths that don't exist"
    echo ""
    echo "The partition pruner looked for:"
    echo "  - s3://bucket/$DB_NAME/$MEASUREMENT/$YEAR/$MONTH/$DAY/*.parquet (DOESN'T EXIST)"
    echo "  - s3://bucket/$DB_NAME/$MEASUREMENT/$YEAR/$MONTH/$DAY/HH/*.parquet (EXISTS)"
    echo ""
    echo "But the directory check passed because hourly subdirs exist beneath the day-level directory,"
    echo "so DuckDB tried to read non-existent day-level files and failed."
    echo ""
    echo "Fix: PR #127 (commit a9d0774) adds file-level verification for S3/Azure paths"
else
    RESULT_COUNT=$(echo "$RESPONSE" | jq -r '.results[0].series[0].values | length')
    echo "✓ QUERY SUCCEEDED (Fix is working!)"
    echo ""
    echo "Returned $RESULT_COUNT rows"
    echo ""
    echo "This means PR #127 fix is working correctly:"
    echo "  - filterExistingRemotePaths() verified which paths have actual files"
    echo "  - Only hourly partition paths were included in query"
    echo "  - Day-level paths were filtered out"
fi

# Cleanup
echo ""
echo "Step 6: Cleanup..."
echo "Deleting database '$DB_NAME'..."
curl -s -X DELETE "$ARC_URL/api/v1/databases/$DB_NAME?confirm=true" | jq .
echo ""
echo "=== Reproduction script complete ==="
