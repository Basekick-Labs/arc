#!/bin/bash
# Debug script to see actual S3/MinIO structure

echo "=== S3/MinIO File Structure Debug ==="
echo ""

# Check if mc (MinIO client) is available
if ! command -v mc &> /dev/null; then
    echo "Installing MinIO client..."
    brew install minio/stable/mc 2>/dev/null || {
        echo "Please install mc manually: https://min.io/docs/minio/linux/reference/minio-mc.html"
        exit 1
    }
fi

# Configure MinIO client
echo "Configuring MinIO client..."
mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null

echo ""
echo "=== Listing all files in arc-test bucket ==="
mc ls -r local/arc-test/production/cpu/ | head -50

echo ""
echo "=== Directory structure ==="
mc ls local/arc-test/production/cpu/

echo ""
echo "=== Checking for day-level files ==="
echo "Looking for: production/cpu/2026/01/21/*.parquet"
DAY_FILES=$(mc ls local/arc-test/production/cpu/2026/01/21/ 2>/dev/null | grep -E '\.parquet$' | grep -v '/')
if [ -z "$DAY_FILES" ]; then
    echo "❌ NO day-level .parquet files found at production/cpu/2026/01/21/"
else
    echo "✓ Found day-level files:"
    echo "$DAY_FILES"
fi

echo ""
echo "=== Checking for hourly subdirectories ==="
echo "Looking for: production/cpu/2026/01/21/HH/"
HOURLY_DIRS=$(mc ls local/arc-test/production/cpu/2026/01/21/ 2>/dev/null | grep '/$')
if [ -z "$HOURLY_DIRS" ]; then
    echo "❌ NO hourly subdirectories found"
else
    echo "✓ Found hourly subdirectories:"
    echo "$HOURLY_DIRS"
    echo ""
    echo "Files in first hourly directory:"
    FIRST_HOUR=$(echo "$HOURLY_DIRS" | head -1 | awk '{print $NF}' | tr -d '/')
    mc ls local/arc-test/production/cpu/2026/01/21/$FIRST_HOUR/ | head -5
fi

echo ""
echo "=== Storage Backend ListDirectories() output ==="
echo "This is what Arc sees when it calls storage.ListDirectories():"
echo ""

# Simulate what Arc's ListDirectories would return
echo "For prefix 'production/cpu/2026/01/21/':"
mc ls local/arc-test/production/cpu/2026/01/21/ | awk '{print $NF}'

echo ""
echo "=== Analysis ==="
echo "Bug occurs when:"
echo "1. Directory exists: production/cpu/2026/01/21/ ✓"
echo "2. Has hourly subdirs: production/cpu/2026/01/21/HH/ ✓"
echo "3. NO day-level files: production/cpu/2026/01/21/*.parquet ✓"
echo "4. FilterExistingRemotePaths includes day-level path incorrectly ✗"
