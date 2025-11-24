# Arc v25.11.2 - Critical Memory Leak Fix

**Release Date**: November 24, 2025
**Type**: Patch Release

---

## Critical Fix

### Memory Leak in Query Result Handling

**Problem**: Arc was experiencing a memory leak that caused gradual memory growth over time under query load. Memory consumption could increase by 20-40% over several hours, eventually leading to system resource exhaustion.

**Root Cause**: Query results were not being properly garbage collected after being returned to clients, causing Python objects to accumulate in memory.

**Solution**: Implemented aggressive garbage collection strategy:
- **Immediate GC** for large query results (>1,000 rows)
- **Periodic GC** for small queries (every 100 queries or every 60 seconds)
- **Explicit cleanup** with `del result` to break references before GC

**Impact**: Eliminates memory leak, ensures stable memory usage under sustained query load.

**Evidence**: Monitoring showed memory growth from 37% → 60% in just 1 hour before fix. This release resolves that issue.

---

## Files Changed

- `api/main.py` - Added garbage collection logic in query endpoints

---

## Upgrade Instructions

### For Existing Deployments

1. **Pull latest code**:
   ```bash
   cd /path/to/arc
   git fetch origin
   git checkout v25.11.2
   ```

2. **Restart Arc** (no configuration changes required):
   ```bash
   ./stop.sh
   ./start.sh native
   ```

3. **Monitor memory usage** over next 24 hours to verify fix

### For Docker Deployments

```bash
docker pull basekick/arc:v25.11.2
docker-compose restart
```

---

## Compatibility

- **No breaking changes**
- **No configuration changes required**
- **No data migration needed**
- Compatible with all v25.11.x deployments

---

## Recommendation

**Immediate upgrade recommended** for all production deployments experiencing:
- Gradual memory growth over time
- Need to restart Arc periodically due to high memory usage
- Memory pressure alerts from monitoring systems

---

## Next Release

**Arc v25.12.1** (December 2025) will include:
- Performance improvements
- New features
- Enhanced documentation
- Additional optimizations

Stay tuned for the December feature release!

---

## Support

- **Issues**: https://github.com/Basekick-Labs/arc/issues
- **Discussions**: https://github.com/Basekick-Labs/arc/discussions
- **Documentation**: https://github.com/Basekick-Labs/arc/tree/main/docs
