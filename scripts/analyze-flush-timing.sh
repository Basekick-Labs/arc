#!/bin/bash
# Script to analyze buffer flush timing from Arc logs
# Usage: ./scripts/analyze-flush-timing.sh arc.log

if [ $# -eq 0 ]; then
    echo "Usage: $0 <log-file>"
    echo "Example: $0 arc.log"
    exit 1
fi

LOG_FILE="$1"

if [ ! -f "$LOG_FILE" ]; then
    echo "Error: Log file '$LOG_FILE' not found"
    exit 1
fi

echo "=== Arc Flush Timing Analysis ==="
echo ""

# Extract age-based flushes
echo "1. Age-Based Flush Timing (last 20):"
echo "   Format: age | excess_age | lock_wait | buffer_key | shard"
echo "   ---------------------------------------------------------------"
grep "Flushing aged buffer" "$LOG_FILE" | tail -20 | while read -r line; do
    age=$(echo "$line" | grep -oP 'age=\K[0-9.]+' || echo "N/A")
    excess=$(echo "$line" | grep -oP 'excess_age=\K[0-9.]+' || echo "N/A")
    lock_wait=$(echo "$line" | grep -oP 'lock_wait=\K[0-9.]+' || echo "N/A")
    buffer=$(echo "$line" | grep -oP 'buffer_key=\K[^ ]+' || echo "N/A")
    shard=$(echo "$line" | grep -oP 'shard=\K[0-9]+' || echo "N/A")

    printf "   %-8s | %-11s | %-10s | %-20s | %s\n" "$age" "$excess" "$lock_wait" "$buffer" "$shard"
done
echo ""

# Check for slow age checks
echo "2. Slow Age Checks (> 100ms):"
slow_checks=$(grep "Slow age check detected" "$LOG_FILE" | wc -l)
if [ "$slow_checks" -gt 0 ]; then
    echo "   Found $slow_checks slow age checks:"
    grep "Slow age check detected" "$LOG_FILE" | tail -10 | while read -r line; do
        shard=$(echo "$line" | grep -oP 'shard=\K[0-9]+' || echo "N/A")
        duration=$(echo "$line" | grep -oP 'check_duration=\K[0-9.]+' || echo "N/A")
        echo "   - Shard $shard took ${duration}ms"
    done
else
    echo "   None found (good!)"
fi
echo ""

# Check for lock wait times
echo "3. Lock Contention Analysis:"
echo "   High lock wait times (> 10ms):"
high_lock_waits=$(grep "Age check waited for shard lock" "$LOG_FILE" | wc -l)
if [ "$high_lock_waits" -gt 0 ]; then
    echo "   Found $high_lock_waits instances:"
    grep "Age check waited for shard lock" "$LOG_FILE" | tail -10 | while read -r line; do
        shard=$(echo "$line" | grep -oP 'shard=\K[0-9]+' || echo "N/A")
        wait=$(echo "$line" | grep -oP 'lock_wait=\K[0-9.]+' || echo "N/A")
        echo "   - Shard $shard waited ${wait}ms for lock"
    done
else
    echo "   None found (excellent lock performance!)"
fi
echo ""

# Statistics
echo "4. Summary Statistics:"
ages=$(grep "Flushing aged buffer" "$LOG_FILE" | grep -oP 'age=\K[0-9.]+')
if [ -n "$ages" ]; then
    avg=$(echo "$ages" | awk '{s+=$1; c++} END {printf "%.2f", s/c}')
    min=$(echo "$ages" | sort -n | head -1)
    max=$(echo "$ages" | sort -n | tail -1)
    count=$(echo "$ages" | wc -l)

    echo "   Age-based flushes: $count"
    echo "   Average age: ${avg}ms"
    echo "   Min age: ${min}ms"
    echo "   Max age: ${max}ms"

    # Calculate excess ages (age - 5000)
    excess_ages=$(grep "Flushing aged buffer" "$LOG_FILE" | grep -oP 'excess_age=\K[0-9.]+')
    if [ -n "$excess_ages" ]; then
        avg_excess=$(echo "$excess_ages" | awk '{s+=$1; c++} END {printf "%.2f", s/c}')
        max_excess=$(echo "$excess_ages" | sort -n | tail -1)
        echo "   Average excess: ${avg_excess}ms (how much beyond threshold)"
        echo "   Max excess: ${max_excess}ms"
    fi
else
    echo "   No age-based flushes found in log"
fi
echo ""

echo "=== Recommendations ==="
if [ -n "$ages" ]; then
    avg_num=$(echo "$ages" | awk '{s+=$1; c++} END {print s/c}')
    if (( $(echo "$avg_num > 6000" | bc -l) )); then
        echo "⚠️  Average flush age (${avg}ms) significantly exceeds 5000ms threshold"
        echo "   Possible causes:"
        echo "   - Heavy lock contention (check lock wait times above)"
        echo "   - Go scheduler delays under extreme load"
        echo "   - GC pauses (check for slow age check warnings)"
        echo "   Consider: Increase flush_workers, reduce max_buffer_size, or enable GC tuning"
    elif (( $(echo "$avg_num > 5500" | bc -l) )); then
        echo "ℹ️  Average flush age (${avg}ms) is moderately above threshold"
        echo "   This is expected under high load with ticker firing every 2500ms"
    else
        echo "✅ Average flush age (${avg}ms) is close to ideal (5000-5500ms range)"
    fi
fi
echo ""
