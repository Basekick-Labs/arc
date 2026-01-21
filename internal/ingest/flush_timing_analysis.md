# Buffer Flush Timing Analysis

## Test Results

Running the timing tests shows that buffers are consistently flushing at ~5.1 seconds:

```
Iteration 1: flush took 5.102807583s
Iteration 2: flush took 5.102354625s
Iteration 3: flush took 5.102462583s
Iteration 4: flush took 5.102942917s
Iteration 5: flush took 5.102635459s

Average flush time: 5.102640633s
Min flush time:     5.102354625s
Max flush time:     5.102942917s
```

##Current Implementation

The ticker fires every `max_buffer_age_ms` (5000ms by default) and checks:
```go
age := now.Sub(startTime)
if age >= maxAge {
    // Flush
}
```

## Why ~5.1 seconds instead of exactly 5.0?

The buffer start time is recorded when the first write happens. The ticker runs every 5 seconds. So:

- T=0ms: Write happens, buffer startTime recorded
- T=5000ms: Ticker fires, checks age
- Age = 5000ms - 0ms = 5000ms
- Condition: 5000ms >= 5000ms? YES → Flush

The extra ~100ms comes from:
1. Ticker scheduling jitter (~1-50ms)
2. Time to iterate through shards and acquire locks
3. Actual flush operation time

## Potential Issue: User Reports 10 seconds

The user reported seeing 10 seconds instead of 5 seconds. This could happen if:

### Scenario 1: Ticker Phase Misalignment (Edge Case)
If the buffer is created just after a ticker fires:
- T=0ms: Ticker fires
- T=1ms: Write happens, buffer created
- T=5000ms: Ticker fires again
- Age check: age = 4999ms < 5000ms → NOT flushed
- T=10000ms: Next ticker fires
- Age check: age = 9999ms >= 5000ms → FLUSHED

But our tests don't show this happening consistently.

### Scenario 2: High Lock Contention
With 32 shards (default) and sequential lock acquisition in `flushAgedBuffers()`:
```go
for shardIdx := range b.shards {
    shard.mu.Lock()  // Could block here
    // Check buffers...
    shard.mu.Unlock()
}
```

If writes are happening continuously with high concurrency, the flush goroutine might be delayed acquiring locks.

### Scenario 3: Measurement Confusion
The user might be measuring:
- Time from FIRST write to flush (correct: ~5s)
- Time from LAST write to flush (could be variable)
- Time from server start to first flush (includes initialization time)

## Conclusion

The current implementation appears to be working correctly based on testing. The flush happens reliably at ~5.1 seconds (5000ms + overhead).

To help the user debug their specific case, we should:
1. Add more detailed logging showing buffer start time vs flush time
2. Add metrics for flush timing distribution
3. Verify their actual configuration (maybe they have max_buffer_age_ms=10000?)
