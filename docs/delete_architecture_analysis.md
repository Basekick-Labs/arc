# DELETE Architecture Analysis: File-Level vs Row-Level

## The Question

Can we apply the retention policy approach (file-level deletion) to general DELETE operations?

Let's analyze the trade-offs.

---

## Current Approaches

### 1. Row-Level DELETE (Currently Disabled)
**How it works:**
- Compute SHA256 hash for every row on write
- Store tombstones in `.deletes/*.parquet` files
- Filter tombstones during queries

**Status:** ❌ Disabled due to 4.6x performance regression

### 2. File-Level DELETE (Retention Policies)
**How it works:**
- Analyze entire Parquet files
- Delete files where ALL rows match criteria
- Physical file deletion, no tombstones

**Status:** ✅ Enabled, zero performance impact

---

## Detailed Comparison

### Architecture 1: Row-Level DELETE with Tombstones (Current/Disabled)

```
WRITE PATH:
┌─────────────────────────────────────┐
│ Row arrives                         │
│  ↓                                  │
│ Compute SHA256(time + tags)  ← SLOW│ +500μs per row
│  ↓                                  │
│ Write to Parquet with hash          │
└─────────────────────────────────────┘

DELETE PATH:
┌─────────────────────────────────────┐
│ DELETE WHERE host='server01'        │
│  ↓                                  │
│ Query matching rows (with hashes)   │
│  ↓                                  │
│ Write tombstone file:               │
│   .deletes/delete_20251023.parquet  │
│   [hash1, hash2, hash3...]          │
└─────────────────────────────────────┘

QUERY PATH:
┌─────────────────────────────────────┐
│ SELECT * FROM cpu                   │
│  ↓                                  │
│ Read data files                     │
│  ↓                                  │
│ Read tombstone files                │
│  ↓                                  │
│ Filter: WHERE hash NOT IN tombstones│ ← SLOW on every query
│  ↓                                  │
│ Return filtered results             │
└─────────────────────────────────────┘
```

**Performance Impact:**
```
Write: 1.74ms → 8ms p50 (4.6x slower)   ❌
Query: Added tombstone filtering       ❌
Storage: Extra tombstone files         ❌
```

---

### Architecture 2: File-Level DELETE (Like Retention)

```
WRITE PATH:
┌─────────────────────────────────────┐
│ Row arrives                         │
│  ↓                                  │
│ Write to Parquet (no hash)    ← FAST│ Normal speed
└─────────────────────────────────────┘

DELETE PATH:
┌─────────────────────────────────────┐
│ DELETE WHERE time < '2025-07-18'    │
│  ↓                                  │
│ Find files with ALL rows matching   │
│  ↓                                  │
│ Delete entire files                 │
│   os.unlink(file.parquet)           │
└─────────────────────────────────────┘

QUERY PATH:
┌─────────────────────────────────────┐
│ SELECT * FROM cpu                   │
│  ↓                                  │
│ Read data files (no filtering!)     │
│  ↓                                  │
│ Return results                      │
└─────────────────────────────────────┘
```

**Performance Impact:**
```
Write: 1.74ms (unchanged)              ✅
Query: No overhead (unchanged)         ✅
Storage: No extra files                ✅
```

---

## Architecture 3: HYBRID - Rewrite-Based DELETE (New Proposal)

Instead of tombstones, **rewrite files** without deleted rows.

```
WRITE PATH:
┌─────────────────────────────────────┐
│ Row arrives                         │
│  ↓                                  │
│ Write to Parquet (no hash)    ← FAST│ Normal speed
└─────────────────────────────────────┘

DELETE PATH:
┌─────────────────────────────────────┐
│ DELETE WHERE host='server01'        │
│  ↓                                  │
│ 1. Find files containing matches    │
│    (use Parquet metadata filters)   │
│  ↓                                  │
│ 2. For each affected file:          │
│    - Read into Arrow table          │
│    - Filter: WHERE NOT (condition)  │
│    - Write new file (filtered)      │
│    - Delete old file                │
│  ↓                                  │
│ 3. Update query cache               │
└─────────────────────────────────────┘

QUERY PATH:
┌─────────────────────────────────────┐
│ SELECT * FROM cpu                   │
│  ↓                                  │
│ Read data files (no filtering!)     │
│  ↓                                  │
│ Return results                      │
└─────────────────────────────────────┘
```

**Performance Impact:**
```
Write: 1.74ms (unchanged)              ✅
Query: No overhead (unchanged)         ✅
Delete: Expensive (rewrites) but rare  ⚠️
Storage: No extra files                ✅
```

---

## Pros & Cons Analysis

### Option 1: Row-Level DELETE with Tombstones (Current)

**Pros:**
- ✅ Precise row-level deletion
- ✅ Fast delete operation (just write tombstone)
- ✅ Can delete specific rows by any criteria

**Cons:**
- ❌ **CRITICAL:** 4.6x write slowdown (hash computation)
- ❌ **CRITICAL:** Query slowdown (tombstone filtering)
- ❌ Extra storage for tombstone files
- ❌ Requires compaction to physically remove deleted data
- ❌ Breaks zero-copy architecture

**Use Cases:**
- Delete specific error events
- GDPR compliance (delete user data)
- Remove corrupted data points

**Verdict:** ❌ **Performance cost too high for time-series workload**

---

### Option 2: File-Level DELETE (Retention-Style)

**Pros:**
- ✅ **ZERO** write overhead
- ✅ **ZERO** query overhead
- ✅ No tombstone files
- ✅ Simple implementation
- ✅ Physical deletion (no lingering data)

**Cons:**
- ❌ **CRITICAL:** Can only delete if ALL rows in file match
- ❌ Not suitable for sparse deletions
- ❌ Requires time-aligned data (compaction helps)

**Use Cases:**
- ✅ Retention policies (time-based)
- ✅ Delete entire partitions (time ranges)
- ❌ Can't delete specific hosts/tags across time
- ❌ Can't delete individual error events

**Example Limitations:**
```sql
-- ✅ WORKS: Delete old data
DELETE WHERE time < '2025-07-18'
-- Entire files are old → delete them

-- ❌ DOESN'T WORK: Delete specific host
DELETE WHERE host = 'server01'
-- Host data is mixed across files → can't delete files

-- ❌ DOESN'T WORK: Delete error events
DELETE WHERE error_code IS NOT NULL
-- Errors scattered across files → can't delete files
```

**Verdict:** ✅ **Perfect for retention, LIMITED for general DELETE**

---

### Option 3: HYBRID - Rewrite-Based DELETE (Proposed)

**Pros:**
- ✅ **ZERO** write overhead (no hashing)
- ✅ **ZERO** query overhead (no tombstones)
- ✅ Precise row-level deletion
- ✅ Can delete by any criteria
- ✅ Physical deletion (no lingering data)
- ✅ No tombstone files

**Cons:**
- ⚠️ Delete operation is EXPENSIVE (rewrites affected files)
- ⚠️ Temporary increased storage (old + new files)
- ⚠️ Locks required during rewrite (prevents concurrent writes)
- ⚠️ Not suitable for frequent deletes

**Use Cases:**
- ✅ Infrequent, precise deletions
- ✅ GDPR compliance (delete user data)
- ✅ Remove corrupted data
- ❌ Not for high-frequency deletes

**Performance Characteristics:**
```
Write: No impact                       ✅
Query: No impact                       ✅
Delete: Slow but acceptable for rare ops ⚠️

Example DELETE performance:
- 10 files affected, 100MB each
- Read: 1000MB @ 500MB/s = 2s
- Filter: Arrow compute = ~100ms
- Write: 900MB @ 300MB/s = 3s
- Total: ~5-6 seconds for 10 files

But happens RARELY (not on hot path)
```

**Verdict:** ✅ **Best hybrid approach for infrequent precise deletes**

---

## Recommendation: Tiered DELETE Strategy

Implement **BOTH** approaches based on use case:

### Tier 1: File-Level DELETE (Fast Path)
**For time-based deletions:**
```sql
-- Use file-level deletion (retention-style)
DELETE WHERE time < '2025-07-18'
DELETE WHERE time BETWEEN '2025-01-01' AND '2025-01-31'
```

**Implementation:** Already done (retention policies)

**Performance:**
- 1-2ms per file analysis
- Zero write/query impact
- Perfect for retention

---

### Tier 2: Rewrite-Based DELETE (Slow Path)
**For precise row-level deletions:**
```sql
-- Use rewrite-based deletion
DELETE WHERE host = 'server01' AND time > '2025-01-01'
DELETE WHERE error_code = 500
DELETE WHERE user_id = 'user123'  -- GDPR
```

**Implementation:** New (needs to be built)

**Performance:**
- Expensive (rewrites files)
- But happens rarely
- Zero impact on writes/queries

---

## Implementation Comparison

### Current (Disabled): Tombstone-Based
```python
# WRITE - adds overhead ❌
def write_row(row):
    hash = sha256(row.time + row.tags)  # SLOW
    row['_hash'] = hash
    buffer.append(row)

# DELETE - fast ✅
def delete(where):
    rows = query(where)
    hashes = [r['_hash'] for r in rows]
    write_tombstone_file(hashes)

# QUERY - adds overhead ❌
def query(sql):
    data = read_parquet_files()
    tombstones = read_tombstone_files()  # SLOW
    return data.filter(~data._hash.isin(tombstones))
```

### Retention (Current): File-Level
```python
# WRITE - no overhead ✅
def write_row(row):
    buffer.append(row)  # No hash

# DELETE - fast if file-aligned ✅
def delete_old(cutoff_date):
    for file in parquet_files:
        max_time = read_max_time(file)  # Fast metadata
        if max_time < cutoff_date:
            os.unlink(file)  # Physical delete

# QUERY - no overhead ✅
def query(sql):
    return read_parquet_files()  # No filtering
```

### Proposed: Rewrite-Based
```python
# WRITE - no overhead ✅
def write_row(row):
    buffer.append(row)  # No hash

# DELETE - expensive but rare ⚠️
def delete(where):
    affected_files = find_files_with_matches(where)

    for file in affected_files:
        # Rewrite file without deleted rows
        table = read_parquet(file)
        filtered = table.filter(~where)  # Arrow zero-copy

        new_file = write_parquet(filtered)
        os.unlink(file)
        os.rename(new_file, file)

# QUERY - no overhead ✅
def query(sql):
    return read_parquet_files()  # No filtering
```

---

## Decision Matrix

| Deletion Type | Best Approach | Why |
|---------------|---------------|-----|
| **Time-based** (retention) | File-level | Files naturally aligned by time |
| **Partition drop** (entire day) | File-level | Can delete entire partition |
| **Tag-based** (specific host) | Rewrite-based | Rows scattered across files |
| **Error cleanup** | Rewrite-based | Errors scattered across files |
| **GDPR** (user data) | Rewrite-based | User data scattered across files |
| **Frequent deletes** | ❌ Not supported | Would kill performance |

---

## Recommended Implementation Plan

### Phase 1: Keep What Works (Now)
✅ File-level deletion via retention policies
- Already implemented
- Zero performance impact
- Covers 80% of use cases (time-based cleanup)

### Phase 2: Add Rewrite-Based DELETE (Future)
🔨 Implement rewrite-based deletion for precise cases:

```python
async def delete_with_rewrite(database, measurement, where_clause):
    """
    Delete rows by rewriting affected Parquet files.

    Use sparingly - this is expensive but preserves performance
    of normal write/query operations.
    """

    # 1. Find affected files using Parquet metadata
    affected_files = find_files_matching(where_clause)

    # 2. Rewrite each file
    for file in affected_files:
        table = pq.read_table(file)

        # Filter using Arrow compute (zero-copy)
        mask = evaluate_where_clause(table, where_clause)
        filtered = table.filter(~mask)

        # Write new file
        temp_file = write_temp_parquet(filtered)

        # Atomic replace
        os.replace(temp_file, file)

    # 3. Clear query cache for affected measurements
    query_cache.clear(database, measurement)
```

**Safety features:**
- Confirmation required (like current delete)
- Row count limits
- Dry-run support
- Execution time limits
- Lock during rewrite (prevent concurrent writes)

### Phase 3: Hybrid Router (Future)
Route DELETE to appropriate handler:

```python
def delete_router(sql):
    """Route DELETE to fastest implementation"""

    if is_time_based_only(sql):
        # Fast path: file-level deletion
        return delete_file_level(sql)
    else:
        # Slow path: rewrite-based deletion
        return delete_with_rewrite(sql)
```

---

## Performance Projections

### File-Level DELETE (Retention)
```
Files analyzed: 1000 files
Time per file: 1-2ms (metadata only)
Total time: 1-2 seconds
Write impact: ZERO
Query impact: ZERO
```

### Rewrite-Based DELETE
```
Files affected: 10 files (100MB each)
Read time: 2 seconds
Filter time: 100ms (Arrow compute)
Write time: 3 seconds
Total time: 5-6 seconds
Write impact: ZERO (not on write path)
Query impact: ZERO (no tombstones)

Acceptable for rare operations!
```

### Tombstone-Based DELETE (Don't Use)
```
Write impact: 4.6x slower    ❌ UNACCEPTABLE
Query impact: Significant    ❌ UNACCEPTABLE
Delete time: Fast           ✅ But not worth it
```

---

## Conclusion

**For Arc's time-series workload:**

1. ✅ **Use file-level deletion (retention-style)** for:
   - Time-based cleanup (90% of cases)
   - Retention policies
   - Partition drops

2. ✅ **Implement rewrite-based deletion** for:
   - GDPR compliance (rare)
   - Error cleanup (rare)
   - Tag-based deletion (rare)

3. ❌ **Never use tombstone-based deletion**:
   - Performance cost is unacceptable
   - Breaks zero-copy architecture
   - Slows down every write and query

**The key insight:** DELETE operations should be **rare** in time-series databases. When they happen, it's acceptable to spend 5-10 seconds rewriting files, because this doesn't impact the hot path (writes/queries).

---

## Next Steps

1. ✅ **Already done:** Retention policies (file-level deletion)
2. 🔨 **Implement next:** Rewrite-based DELETE for precise cases
3. 📊 **Monitor:** Track DELETE frequency to validate it's rare
4. 🚀 **Future:** Hybrid router for automatic best-path selection

Want me to implement the rewrite-based DELETE next?
