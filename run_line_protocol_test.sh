#!/bin/bash

#
# Line Protocol Load Test Runner
#
# Quick script to run Line Protocol load tests with sensible defaults
#

# Default values
URL="${ARC_URL:-http://localhost:8000}"
TOKEN="${ARC_TOKEN:-fuLrsJiVG_NluAS-VT5cLYanJt7dfI2KFRx6qkYkKYY}"
DATABASE="${ARC_DATABASE:-telegraf}"
RPS="${RPS:-100000}"
DURATION="${DURATION:-30}"
PREGENERATE="${PREGENERATE:-1000}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
HOSTS="${HOSTS:-1000}"
WORKERS="${WORKERS}"

echo "=================================="
echo "Arc Line Protocol Load Test"
echo "=================================="
echo "URL:         $URL"
echo "Database:    $DATABASE"
echo "Target RPS:  $RPS"
echo "Duration:    ${DURATION}s"
echo "=================================="
echo ""

# Build command
CMD="python3 scripts/line_protocol_load_test.py \
  --url $URL \
  --token $TOKEN \
  --database $DATABASE \
  --rps $RPS \
  --duration $DURATION \
  --pregenerate $PREGENERATE \
  --batch-size $BATCH_SIZE \
  --hosts $HOSTS"

# Add workers if specified
if [ -n "$WORKERS" ]; then
  CMD="$CMD --workers $WORKERS"
fi

# Run the test
eval $CMD
