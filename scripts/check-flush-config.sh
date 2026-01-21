#!/bin/bash
# Diagnostic script to check max_buffer_age_ms configuration

echo "=== Arc Buffer Flush Configuration Diagnostic ==="
echo ""

# Check arc.toml
echo "1. Checking arc.toml configuration:"
if [ -f "arc.toml" ]; then
    grep -n "max_buffer_age_ms" arc.toml || echo "   max_buffer_age_ms not found in arc.toml (will use default: 5000)"
else
    echo "   arc.toml not found"
fi
echo ""

# Check environment variable
echo "2. Checking environment variable:"
if [ -z "$ARC_INGEST_MAX_BUFFER_AGE_MS" ]; then
    echo "   ARC_INGEST_MAX_BUFFER_AGE_MS is not set"
else
    echo "   ARC_INGEST_MAX_BUFFER_AGE_MS=$ARC_INGEST_MAX_BUFFER_AGE_MS"
fi
echo ""

# Check running process
echo "3. Checking running Arc process environment:"
ARC_PID=$(pgrep -f "arc" | head -1)
if [ -n "$ARC_PID" ]; then
    echo "   Found Arc process: PID $ARC_PID"
    if [ -f "/proc/$ARC_PID/environ" ]; then
        cat /proc/$ARC_PID/environ | tr '\0' '\n' | grep "ARC_INGEST_MAX_BUFFER_AGE_MS" || echo "   No ARC_INGEST_MAX_BUFFER_AGE_MS in process environment"
    elif command -v lsof >/dev/null 2>&1; then
        echo "   (Process environment check not available on this system)"
    fi
else
    echo "   No running Arc process found"
fi
echo ""

# Check recent logs for actual flush timing
echo "4. Analyzing recent flush logs (if available):"
if [ -f "arc.log" ]; then
    echo "   Recent buffer flushes (showing 'age' field):"
    grep "Flushing aged buffer" arc.log | tail -5 | grep -oP '"age":\K[0-9.]+' | while read age; do
        echo "   - Buffer flushed at age: ${age}ms"
    done
else
    echo "   No arc.log file found"
    echo "   Check your logs for lines containing 'Flushing aged buffer' and look for the 'age' field"
fi
echo ""

echo "=== Analysis ==="
echo "If you configured max_buffer_age_ms=2000 and see flushes at age ~4000ms,"
echo "this confirms the ticker phase misalignment bug."
echo ""
echo "Expected behavior:"
echo "  - max_buffer_age_ms=2000 → flush at ~2000-2100ms"
echo "  - max_buffer_age_ms=5000 → flush at ~5000-5100ms"
echo ""
echo "Bug behavior (2x timing):"
echo "  - max_buffer_age_ms=2000 → flush at ~4000ms"
echo "  - max_buffer_age_ms=5000 → flush at ~10000ms"
