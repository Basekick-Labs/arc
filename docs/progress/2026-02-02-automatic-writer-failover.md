# Automatic Writer Failover (Phase 3)

**Date**: February 2, 2026
**Branch**: `feat/writer-automatic-failover`
**Status**: Implemented

## Summary

Automatic single-writer failover with < 30s RTO. When the primary writer node fails, a healthy standby writer is automatically promoted via Raft consensus.

## Files Created

- `internal/cluster/writer_failover.go` — WriterFailoverManager
- `internal/cluster/writer_failover_test.go` — 9 tests

## Files Modified

- `internal/cluster/node.go` — Added WriterState (primary/standby), getter/setter, IsPrimaryWriter, Clone updated
- `internal/cluster/registry.go` — Added GetPrimaryWriter, GetStandbyWriters
- `internal/cluster/router.go` — RouteWrite prefers primary writer
- `internal/cluster/raft/fsm.go` — CommandPromoteWriter, CommandDemoteWriter, primaryWriterID tracking, snapshot/restore
- `internal/cluster/raft/node.go` — PromoteWriter, DemoteWriter, LeaderCh methods
- `internal/config/config.go` — FailoverEnabled, FailoverTimeoutSeconds, FailoverCooldownSeconds
- `docs/progress/2026-01-08-arc-enterprise-product-definition.md` — Updated checklist

## Architecture

```
Health Checker detects writer unhealthy (15s)
    → Registry callback fires
    → WriterFailoverManager.HandleWriterUnhealthy()
    → Counts consecutive failures against threshold
    → selectNewPrimary() picks healthiest standby
    → Raft.PromoteWriter() applies via consensus
    → FSM updates all nodes' writer state
    → Router sends writes to new primary

Total RTO: ~10-22s
```

## Configuration

```toml
[cluster]
failover_enabled = true
failover_timeout = 30       # seconds
failover_cooldown = 60      # seconds between failovers
```
