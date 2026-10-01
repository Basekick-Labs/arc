#!/bin/bash
# Debug script to inspect the S3 partition layout Arc has written.
#
# Works against any S3-compatible endpoint (the bundled SeaweedFS from the
# compose stacks, external MinIO, AWS S3, ...). Override via environment:
#   ARC_S3_ENDPOINT   (default http://localhost:8333 — bundled SeaweedFS)
#   ARC_S3_BUCKET     (default arc-test)
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (default arcadmin/arcadmin123,
#   the compose stacks' dev credentials)

set -u

ENDPOINT="${ARC_S3_ENDPOINT:-http://localhost:8333}"
BUCKET="${ARC_S3_BUCKET:-arc-test}"
PREFIX="${ARC_S3_PREFIX:-production/cpu}"
DAY="${ARC_S3_DAY:-2026/01/21}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-arcadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-arcadmin123}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

if ! command -v aws &> /dev/null; then
    echo "Please install the AWS CLI: https://docs.aws.amazon.com/cli/"
    exit 1
fi

s3() { aws --endpoint-url "$ENDPOINT" s3 "$@"; }

echo "=== S3 File Structure Debug ($ENDPOINT / $BUCKET) ==="
echo ""

echo "=== Listing files under $PREFIX/ ==="
s3 ls "s3://$BUCKET/$PREFIX/" --recursive | head -50

echo ""
echo "=== Directory structure ==="
s3 ls "s3://$BUCKET/$PREFIX/"

echo ""
echo "=== Checking for day-level files ==="
echo "Looking for: $PREFIX/$DAY/*.parquet"
DAY_FILES=$(s3 ls "s3://$BUCKET/$PREFIX/$DAY/" 2>/dev/null | grep -E '\.parquet$' | grep -v ' PRE ')
if [ -z "$DAY_FILES" ]; then
    echo "❌ NO day-level .parquet files found at $PREFIX/$DAY/"
else
    echo "✓ Found day-level files:"
    echo "$DAY_FILES"
fi

echo ""
echo "=== Checking for hourly subdirectories ==="
echo "Looking for: $PREFIX/$DAY/HH/"
HOURLY_DIRS=$(s3 ls "s3://$BUCKET/$PREFIX/$DAY/" 2>/dev/null | grep ' PRE ')
if [ -z "$HOURLY_DIRS" ]; then
    echo "❌ NO hourly subdirectories found"
else
    echo "✓ Found hourly subdirectories:"
    echo "$HOURLY_DIRS"
    echo ""
    echo "Files in first hourly directory:"
    FIRST_HOUR=$(echo "$HOURLY_DIRS" | head -1 | awk '{print $NF}' | tr -d '/')
    s3 ls "s3://$BUCKET/$PREFIX/$DAY/$FIRST_HOUR/" | head -5
fi

echo ""
echo "=== Storage Backend ListDirectories() output ==="
echo "This is what Arc sees when it calls storage.ListDirectories():"
echo ""
echo "For prefix '$PREFIX/$DAY/':"
s3 ls "s3://$BUCKET/$PREFIX/$DAY/" | awk '{print $NF}'

echo ""
echo "=== Analysis ==="
echo "Bug occurs when:"
echo "1. Directory exists: $PREFIX/$DAY/ ✓"
echo "2. Has hourly subdirs: $PREFIX/$DAY/HH/ ✓"
echo "3. NO day-level files: $PREFIX/$DAY/*.parquet ✓"
echo "4. FilterExistingRemotePaths includes day-level path incorrectly ✗"
