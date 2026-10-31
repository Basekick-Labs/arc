package metrics

import (
	"database/sql"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// Metrics holds all Arc metrics for Prometheus export
type Metrics struct {
	startTime time.Time

	// HTTP request metrics
	httpRequestsTotal   atomic.Int64
	httpRequestsSuccess atomic.Int64
	httpRequestsError   atomic.Int64

	// HTTP latency histogram buckets (microseconds)
	// Buckets: 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, +Inf
	httpLatencyBuckets [10]atomic.Int64
	httpLatencySum     atomic.Int64
	httpLatencyCount   atomic.Int64

	// Ingestion metrics
	ingestRecordsTotal  atomic.Int64
	ingestBytesTotal    atomic.Int64
	ingestBatchesTotal  atomic.Int64
	ingestErrorsTotal   atomic.Int64
	ingestFlushDeferred atomic.Int64
	// bufferDeferredBuffers is a GAUGE: how many buffers are currently holding
	// records that could not be handed to a flush worker. It is what the flush
	// drainer acts on, so a value that stays non-zero while the queue has room
	// means the drainer is not keeping up (or is wedged).
	//
	// Exported as arc_buffer_deferred_buffers, NOT arc_ingest_*: in this file
	// arc_ingest_* is uniformly counters and arc_buffer_* holds the buffer-state
	// gauges (arc_buffer_queue_depth, arc_buffer_records_buffered,
	// arc_buffer_records_queued, arc_buffer_records_inflight), which this sits
	// beside and is published alongside.
	bufferDeferredBuffers atomic.Int64

	// MessagePack specific
	msgpackRequestsTotal atomic.Int64
	msgpackRecordsTotal  atomic.Int64
	msgpackBytesTotal    atomic.Int64

	// Line Protocol specific
	lineprotocolRequestsTotal atomic.Int64
	lineprotocolRecordsTotal  atomic.Int64
	lineprotocolBytesTotal    atomic.Int64

	// Query metrics
	queryRequestsTotal atomic.Int64
	querySuccessTotal  atomic.Int64
	queryErrorsTotal   atomic.Int64
	queryTimeoutsTotal atomic.Int64
	queryRowsTotal     atomic.Int64
	querySlowTotal     atomic.Int64
	queryLatencySum    atomic.Int64 // microseconds
	queryLatencyCount  atomic.Int64
	queryExecutions    [2][2]atomic.Int64 // engine (DuckDB, arcx) by outcome (success, error)
	queryEngineLatency [2]queryLatencyHistogram

	// Client-disconnect mid-stream counters, broken out by streaming
	// handler so operators can disambiguate "Grafana panels giving up"
	// (arrow_json — the duckdb_arrow code path /api/v1/query takes) from
	// "scripts piping into curl that get killed" (sql_json) from
	// "downstream Arrow consumers like grafana-arrow-datasource that
	// disconnect on tab close" (arrow_ipc). Incremented at every site
	// that today only logs a Warn with rows_sent (see #426).
	queryDisconnectsArrowIPC  atomic.Int64
	queryDisconnectsArrowJSON atomic.Int64
	queryDisconnectsSQLJSON   atomic.Int64

	// Arrow buffer metrics
	bufferRecordsBuffered atomic.Int64
	bufferBytesBuffered   atomic.Int64
	bufferOldestAgeNS     atomic.Int64
	bufferMemoryLimit     atomic.Int64
	bufferMemoryPressure  atomic.Int64
	bufferRecordsWritten  atomic.Int64
	bufferFlushesTotal    atomic.Int64
	bufferErrorsTotal     atomic.Int64
	bufferFlushFailures   atomic.Int64
	bufferQueueDepth      atomic.Int64
	bufferRecordsQueued   atomic.Int64
	bufferRecordsInflight atomic.Int64

	// Storage metrics
	storageWritesTotal     atomic.Int64
	storageWriteBytesTotal atomic.Int64
	storageReadsTotal      atomic.Int64
	storageReadBytesTotal  atomic.Int64
	storageErrorsTotal     atomic.Int64

	// Compaction metrics
	compactionJobsTotal   atomic.Int64
	compactionJobsSuccess atomic.Int64
	compactionJobsFailed  atomic.Int64
	// compactionJobsInterrupted: attempts cut short by shutdown or a
	// cancelled cycle (#1343).
	compactionJobsInterrupted atomic.Int64
	// compactionPartitionsSkippedTooWide: candidates a cycle left alone
	// because they are remembered as too wide to compact; the gauge is how
	// many partitions are remembered now.
	compactionPartitionsSkippedTooWide atomic.Int64
	compactionPartitionsTooWide        atomic.Int64
	compactionFilesCompacted           atomic.Int64
	compactionBytesRead                atomic.Int64
	compactionBytesWritten             atomic.Int64
	compactionManifestsRecovered       atomic.Int64

	// Scheduled spoke metrics are absent from exports until the network
	// scheduler starts. A bundle-only or disabled spoke must not look healthy.
	edgeSyncSpokeSchedulerEnabled atomic.Bool
	edgeSyncSpokeLastSuccessUnix  atomic.Int64
	edgeSyncSpokePassFailures     atomic.Int64

	// Auth metrics
	authRequestsTotal atomic.Int64
	authCacheHits     atomic.Int64
	authCacheMisses   atomic.Int64
	authFailuresTotal atomic.Int64

	// DuckDB connection pool.
	//
	// The gauges are sampled from sql.DBStats when metrics are read, not
	// pushed on state change — see SetDBPoolStats. The saturation signal is
	// dbWaitCount/dbWaitSeconds, NOT the in-use/open ratio: a pool sitting at
	// 4-of-4 with zero waits is healthy, while any sustained wait growth means
	// queries are actually blocking on a connection (#809).
	dbConnectionsMax   atomic.Int64
	dbConnectionsOpen  atomic.Int64
	dbConnectionsInUse atomic.Int64
	dbConnectionsIdle  atomic.Int64
	dbWaitCount        atomic.Int64 // Cumulative connections waited for
	dbWaitMicros       atomic.Int64 // Cumulative time blocked waiting, microseconds
	// dbQueriesTotal and dbQueryErrorsTotal are NOT wired, deliberately.
	//
	// Counting them at the DuckDB wrapper (DuckDB.Query/QueryContext/Exec)
	// misses the hot path: shipped builds answer queries through
	// DuckDB.ArrowQueryContext (and the arcx engine, when built), which never
	// pass through those wrappers. A counter named "total" that silently
	// omits most queries is the failure mode #801 was filed for, so it is
	// better absent than partial. Use arc_query_requests_total / arc_query_errors_total,
	// which are counted at every API entry point (#809).
	dbQueriesTotal     atomic.Int64
	dbQueryErrorsTotal atomic.Int64

	// MQTT metrics
	mqttMessagesReceived atomic.Int64
	mqttMessagesFailed   atomic.Int64
	mqttBytesReceived    atomic.Int64
	mqttDecodeSuccess    atomic.Int64
	mqttDecodeErrors     atomic.Int64
	mqttConnected        atomic.Int64 // 1 = connected, 0 = disconnected
	mqttReconnects       atomic.Int64

	// Audit metrics
	auditEventsTotal   atomic.Int64
	auditWriteErrors   atomic.Int64
	auditEventsDropped atomic.Int64 // Events discarded before queueing (channel full)

	// WAL metrics
	walRecordsPreserved   atomic.Int64 // Records preserved in WAL for recovery (flush failures)
	walRegisteredFloorLag atomic.Int64 // committed batches above the registered floor (#1207)
	walHeldFiles          atomic.Int64 // WAL files kept for batches whose files wait to register (#1207 S4c)
	walParkedFiles        atomic.Int64 // refused registrations parked out of the query path (#1207 S4b)
	walParkedBytes        atomic.Int64
	walCoverageLost       atomic.Int64 // batches that settled without a registered file (#1207)

	// Origin stream and standby store (#1207 S3).
	originStreamSessions     atomic.Int64 // standby sessions this primary is sending to
	originStreamDropped      atomic.Int64 // batches not sent to a standby (its queue was full)
	standbyStreamConnected   atomic.Int64 // 1 while this standby receives from its primary
	standbyStreamEntries     atomic.Int64 // batches (or chunks) stored
	standbyStreamGaps        atomic.Int64 // times a gap was found in what this standby holds
	standbyStoreBytes        atomic.Int64
	standbyStoreIncomplete   atomic.Int64 // keys with a known gap above the floor
	standbyStoreTrimmedBytes atomic.Int64
	standbyStoreDropped      atomic.Int64 // batches not stored: the store was at its cap
	standbyStoreEvicted      atomic.Int64 // bytes of replaced primary instances evicted at the cap
	standbyStoreLost         atomic.Int64 // batches held that the primary reported lost
	standbyStoreBlocked      atomic.Int64 // keys whose trim waits on files not yet here
	standbyStoreReplaced     atomic.Int64 // bytes held for replaced primary instances
	standbyStorePinned       atomic.Int64 // bytes the store may not evict (stream takeover)
	standbyStoreCapped       atomic.Int64 // 1 while the store refuses batches at its cap (#1266)
	standbyStoreLowSpace     atomic.Int64 // 1 while the store's volume is below its free-space reserve (#1266)
	standbyStoreMaxBytes     atomic.Int64 // the store's effective cap, 0 = none (#1266)

	streamClaimsEnabled  atomic.Int64 // 1 once stream claims are applied (#1207 S4a)
	streamClaims         atomic.Int64 // stream claims held in the FSM
	departedWriters      atomic.Int64 // writers that left gracefully and have not re-joined (#1207)
	oldestClaimAge       atomic.Int64 // seconds the oldest unfinished stream claim has been open (#1207)
	unpulledFiles        atomic.Int64 // files of claimed instances this claimant does not hold yet (#1207)
	walCoverageRefused   atomic.Int64 // batches whose file was refused: another writer claimed them
	walRecoveryTotal     atomic.Int64 // Successful WAL recovery operations
	walRecoveryRecords   atomic.Int64 // Total records recovered from WAL
	walDroppedEntries    atomic.Int64 // Entries dropped due to full WAL buffer
	walFailedWrites      atomic.Int64 // WAL entries lost to a failed write (#676)
	walDiskFullWrites    atomic.Int64 // Of those, lost to ENOSPC or EDQUOT
	walRecoveredWrites   atomic.Int64 // Failed WAL writes the retry on a new file recovered
	walFailedSyncs       atomic.Int64 // WAL fsync/fdatasync failures
	walOversizedPayloads atomic.Int64 // Payloads rejected for exceeding the single-entry cap even after chunking (#677)
	walDirectoryBytes    atomic.Int64 // Current bytes occupied by every file in the WAL directory
	// WAL volume disk pressure (#676)
	walDiskPressure           atomic.Int64 // 1 while live writes are refused for the WAL volume's free space
	walDiskFreeBytes          atomic.Int64 // Free bytes on the WAL volume at the last sample
	walDiskPressureRejections atomic.Int64 // Live writes refused for WAL volume disk pressure
	walDiskStatErrors         atomic.Int64 // Failed reads of the WAL volume's free space (the gate fails open)

	// Cluster file registration outbox (#1209)
	fileRegistrationsPending      atomic.Int64 // files flushed and not yet confirmed in the cluster manifest
	fileRegistrationOldestPending atomic.Int64 // unix nanos the oldest pending file was flushed; 0 when none
	fileRegistrationsConfirmed    atomic.Int64 // files registered by the outbox
	fileRegistrationJournalErrs   atomic.Int64 // registration journal write or fsync failures

	// Reconciliation (#1214): 1 while the last run held its orphan-storage
	// sweep because what other nodes have pending could not be established.
	reconciliationStorageSweepHeld atomic.Int64

	// Ingest fencing (#1207)
	ingestFenced          atomic.Int64                           // 1 while this writer refuses writes
	ingestFencedWrites    [len(IngestFencedReasons)]atomic.Int64 // live writes refused, by reason (#1268)
	writesForwarded       [len(WriteForwardResults)]atomic.Int64 // writes forwarded to a writer, by result (#1270)
	walQuarantinedFiles   atomic.Int64                           // WAL files isolated after repeated recovery failures
	walQuarantineReleased atomic.Int64                           // pending WAL entries released because their file was quarantined (#1225)
	// 1 while the last recovery pass left a file on disk for which at least one
	// row range of a parent entry was handed off — the state an older binary
	// would replay in full.
	walPartialRowRecovery atomic.Int64

	// NOTE: no decompression-pool discard counter here (#817). The pooled
	// codecs it belonged to were replaced by decompressGzipPooled /
	// decompressZstdPooled; internal/api's decompressBufferPool is never
	// Get() from at runtime, so no buffer can be discarded. If a pooled
	// decompression path returns, add the guard and its counter together.

	// Governance metrics
	governanceRateLimited    atomic.Int64 // Queries rejected by rate limiting
	governanceQuotaExhausted atomic.Int64 // Queries rejected by quota exhaustion
	governanceQueriesCapped  atomic.Int64 // Queries whose results reached a policy row cap (#724)
	governancePoliciesActive atomic.Int64 // Number of active governance policies

	// Query Management metrics
	queryMgmtActiveQueries  atomic.Int64 // Currently running tracked queries (gauge)
	queryMgmtCancelledTotal atomic.Int64 // Total queries cancelled via API
	queryMgmtHistorySize    atomic.Int64 // Completed queries in history buffer (gauge)

	// Cluster FSM security metrics (Enterprise only — only mutated when
	// the Raft FSM is constructed, which is gated by cluster.enabled +
	// Enterprise license. See internal/cluster/raft/fsm.go and
	// GHSA-f85q-mvg8-qf37). A non-zero growth rate is the load-bearing
	// operator signal that somebody (a peer, a stored snapshot, or a
	// pre-validation Raft log entry) proposed a path the FSM refused.
	// Alert on this.
	clusterManifestRejectedPathsTotal atomic.Int64

	// storageInvalidPathQuarantinedTotal counts entries a cleanup,
	// reconciliation or replication loop refused to keep retrying because a
	// storage call returned storage.ErrInvalidPath, which is permanent (#747).
	// Same operator semantics as clusterManifestRejectedPathsTotal above: the
	// value should sit at zero, and any growth means a stored key (a compaction
	// manifest, a Raft manifest entry, an edge-sync ledger row) names something
	// no backend can address. Those entries are dropped from their work set, so
	// this counter is the only place the condition is aggregated.
	storageInvalidPathQuarantinedTotal atomic.Int64

	// File-level time pruning (query.file_time_pruning, #659). A pass is a
	// query whose range starts in the current hour on a local backend; the
	// rest say what the pass did. nothingDropped and capFallback are the
	// evidence for the margin and cap defaults.
	fileTimePruningPasses         atomic.Int64
	fileTimePruningExpanded       atomic.Int64
	fileTimePruningFilesListed    atomic.Int64
	fileTimePruningFilesDropped   atomic.Int64
	fileTimePruningNothingDropped atomic.Int64
	fileTimePruningCapFallback    atomic.Int64
	fileTimePruningZeroSurvivors  atomic.Int64
	fileTimePruningAnomalousKept  atomic.Int64
	// ingestFutureStampedRows: rows stamped more than the file-time pruning
	// margin past their flush, in its hour.
	ingestFutureStampedRows atomic.Int64
	// compactionCopyFallbackTotal counts compaction outputs an object store
	// refused to copy server-side, copied through the node instead;
	// compactionFinishedTotal, committed object-store compactions their
	// finisher ended; compactionWedgedTotal, those marked wedged for an
	// operator (#1207 d2a2b).
	compactionCopyFallbackTotal atomic.Int64
	compactionFinishedTotal     atomic.Int64
	compactionWedgedTotal       atomic.Int64
	// manifestRepairDroppedTotal / manifestRepairAdoptedTotal: entries the
	// shared-object-store manifest repair dropped (object gone) and objects
	// it registered (compaction outputs the manifest never listed) (#1207).
	manifestRepairDroppedTotal atomic.Int64
	manifestRepairAdoptedTotal atomic.Int64
	manifestRepairPending      atomic.Int64
	// staleLocal*: the stale-local sweep's removals, tracked files and
	// whether its breaker refused the last pass (#1071).
	staleLocalUnlinkedTotal atomic.Int64
	staleLocalTracked       atomic.Int64
	staleLocalBlocked       atomic.Int64
	staleLocalTrackingFail  atomic.Int64

	// clusterLocalDeletePending is the number of manifest deletes a
	// per-node-storage cluster node has been told about and has not yet
	// unlinked locally. A gauge: it should return to zero within a grace
	// period of every burst; a value that keeps climbing means the delete
	// workers cannot keep up with retention or a compaction backlog.
	clusterLocalDeletePending atomic.Int64
	// clusterLocalDeleteDeferred is the number of local unlinks of a file a
	// row DELETE rewrote that wait for the rewrite to arrive on this node
	// (normal while pulls are slow; a value that never falls means the
	// rewrites are not arriving).
	clusterLocalDeleteDeferred atomic.Int64

	// compactionManifestsParkedUnparseableTotal counts crash-recovery
	// manifests recovery parked because their body did not decode (#926): a
	// zero-length file left by a crash before the rename was durable, or
	// garbage. Parking removes the manifest from the recovery work set so it
	// stops holding back every candidate on the node, which means compaction
	// resumes and the "failed" cycle disappears; this counter is the only
	// aggregated signal that the partition the parked path names (tier,
	// database and job are in the file name) may hold a short _compacted
	// output or both an output and its inputs. Counted only after the parked
	// copy and the delete both landed, never on entry.
	compactionManifestsParkedUnparseableTotal atomic.Int64

	// storageUnaddressableFiles is how many data files the MOST RECENT backup
	// found in source storage that no listing returns, so nothing could copy
	// them (#756).
	//
	// A gauge, not a counter, and the distinction is what makes it alertable:
	// the condition is a property of the store right now, and renaming the
	// files has to clear it. A counter would keep the alert firing forever
	// after the first bad file, including after the operator fixed it.
	storageUnaddressableFiles atomic.Int64

	// backupSkippedFiles is how many files the MOST RECENT backup inventoried
	// but could not store (#977): unreadable at copy time, or a destination key
	// over the storage key limit (#761). Spans every file group the backup
	// copies (data, in-root Iceberg metadata, compaction state, outside-root
	// warehouse), the same total the status endpoint reports as skipped_files;
	// only the first two groups are named in skipped_sample, the others are
	// named in the log. A gauge for the same reason as storageUnaddressableFiles: the next clean
	// backup clears it. Set by every backup that finishes its copy phases,
	// including one the skip ratio then fails; a backup that fails earlier
	// leaves the previous value.
	backupSkippedFiles atomic.Int64

	// Cluster auth metrics (Enterprise only — mutated on every FSM apply
	// of a token command). clusterAuthApplyTotal increments per applied
	// command type so operators can see "create vs update vs revoke"
	// distribution; clusterAuthRejectedTotal counts applier-side
	// validation refusals. Phase A: Cluster Auth Convergence.
	// clusterHeartbeatsUnknownNodeTotal counts heartbeats received from a
	// node this one has no record of. A non-zero growth rate means a peer
	// believes it is in a cluster that has forgotten it, which is the state
	// behind a node that left and never re-joined. The node id is in the log
	// line, not here: the metrics package has no dynamic labels.
	clusterHeartbeatsUnknownNodeTotal atomic.Int64

	clusterAuthApplyCreateTotal atomic.Int64
	// Reconcile of the local auth/RBAC SQLite with the cluster FSM (#1336).
	// The three gauges hold the last run's counts across all six tables.
	clusterAuthReconcileTotal       atomic.Int64
	clusterAuthReconcileFailedTotal atomic.Int64
	clusterAuthReconcileLocalRows   atomic.Int64
	clusterAuthReconcileCollisions  atomic.Int64
	clusterAuthReconcileKeptRows    atomic.Int64
	clusterAuthApplyUpdateTotal     atomic.Int64
	clusterAuthApplyRevokeTotal     atomic.Int64
	clusterAuthApplyDeleteTotal     atomic.Int64
	clusterAuthApplyRotateTotal     atomic.Int64
	clusterAuthRejectedTotal        atomic.Int64

	// Cluster RBAC metrics (Enterprise only — mutated on every FSM apply
	// of an RBAC command). Same shape as the auth metrics above: one
	// counter per applied command type so operators can see the
	// create vs update vs delete distribution; clusterRBACRejectedTotal
	// counts applier-side validation refusals across all 13 RBAC command
	// types. Phase A.1: Cluster Auth Convergence (RBAC).
	clusterRBACApplyCreateOrganizationTotal          atomic.Int64
	clusterRBACApplyUpdateOrganizationTotal          atomic.Int64
	clusterRBACApplyDeleteOrganizationTotal          atomic.Int64
	clusterRBACApplyCreateTeamTotal                  atomic.Int64
	clusterRBACApplyUpdateTeamTotal                  atomic.Int64
	clusterRBACApplyDeleteTeamTotal                  atomic.Int64
	clusterRBACApplyCreateRoleTotal                  atomic.Int64
	clusterRBACApplyUpdateRoleTotal                  atomic.Int64
	clusterRBACApplyDeleteRoleTotal                  atomic.Int64
	clusterRBACApplyCreateMeasurementPermissionTotal atomic.Int64
	clusterRBACApplyDeleteMeasurementPermissionTotal atomic.Int64
	clusterRBACApplyAddTokenToTeamTotal              atomic.Int64
	clusterRBACApplyRemoveTokenFromTeamTotal         atomic.Int64
	clusterRBACRejectedTotal                         atomic.Int64
	// Phase A.2 Item 2: counts DeleteOrganization / DeleteTeam calls
	// that the cluster-mode proposer refused because the local
	// descendant count exceeded cluster.rbac.max_cascade_descendants.
	// Non-zero growth means operators are issuing cascades large enough
	// that the FSM apply would risk a leadership-loss-mid-apply; alert
	// + ask the operator if they need a higher cap or a chunked-delete
	// workflow.
	clusterRBACCascadeRejectedTotal atomic.Int64

	logger zerolog.Logger
}

// Query engines are a closed label set. Keep these values stable: they are
// exported as Prometheus labels and must not contain request-controlled data.
const (
	QueryEngineDuckDB = "duckdb"
	QueryEngineArcx   = "arcx"
)

const (
	queryOutcomeSuccess = iota
	queryOutcomeError
)

type queryLatencyHistogram struct {
	buckets [11]atomic.Int64
	sum     atomic.Int64 // microseconds
	count   atomic.Int64
}

// RecordQueryEngineExecution records completed engine executions and their latency.
// The buckets are 10ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 30s,
// and +Inf. Query latency is recorded for failures as well as successes.
func (m *Metrics) RecordQueryEngineExecution(engine string, succeeded bool, durationMicros int64) {
	var engineIndex int
	switch engine {
	case QueryEngineDuckDB:
		engineIndex = 0
	case QueryEngineArcx:
		engineIndex = 1
	default:
		return
	}
	if durationMicros < 0 {
		durationMicros = 0
	}
	outcomeIndex := queryOutcomeError
	if succeeded {
		outcomeIndex = queryOutcomeSuccess
	}
	m.queryExecutions[engineIndex][outcomeIndex].Add(1)
	h := &m.queryEngineLatency[engineIndex]
	h.sum.Add(durationMicros)
	h.count.Add(1)
	h.buckets[queryLatencyBucket(durationMicros)].Add(1)
}

func queryLatencyBucket(micros int64) int {
	switch {
	case micros <= 10_000:
		return 0
	case micros <= 50_000:
		return 1
	case micros <= 100_000:
		return 2
	case micros <= 250_000:
		return 3
	case micros <= 500_000:
		return 4
	case micros <= 1_000_000:
		return 5
	case micros <= 2_500_000:
		return 6
	case micros <= 5_000_000:
		return 7
	case micros <= 10_000_000:
		return 8
	case micros <= 30_000_000:
		return 9
	default:
		return 10
	}
}

var (
	instance *Metrics
	once     sync.Once
)

// Get returns the singleton metrics instance
func Get() *Metrics {
	once.Do(func() {
		instance = &Metrics{
			startTime: time.Now(),
		}
	})
	return instance
}

// Init initializes the metrics with a logger
func Init(logger zerolog.Logger) *Metrics {
	m := Get()
	m.logger = logger.With().Str("component", "metrics").Logger()
	m.logger.Info().Msg("Metrics collector initialized")
	return m
}

// HTTP Metrics
func (m *Metrics) IncHTTPRequests() { m.httpRequestsTotal.Add(1) }
func (m *Metrics) IncHTTPSuccess()  { m.httpRequestsSuccess.Add(1) }
func (m *Metrics) IncHTTPError()    { m.httpRequestsError.Add(1) }

// RecordHTTPLatency records HTTP request latency in microseconds
func (m *Metrics) RecordHTTPLatency(durationMicros int64) {
	m.httpLatencySum.Add(durationMicros)
	m.httpLatencyCount.Add(1)

	// Update histogram bucket
	bucketIdx := m.getLatencyBucket(durationMicros)
	m.httpLatencyBuckets[bucketIdx].Add(1)
}

func (m *Metrics) getLatencyBucket(micros int64) int {
	// Buckets: 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, +Inf
	switch {
	case micros <= 1000:
		return 0
	case micros <= 5000:
		return 1
	case micros <= 10000:
		return 2
	case micros <= 25000:
		return 3
	case micros <= 50000:
		return 4
	case micros <= 100000:
		return 5
	case micros <= 250000:
		return 6
	case micros <= 500000:
		return 7
	case micros <= 1000000:
		return 8
	default:
		return 9
	}
}

// Ingestion Metrics
func (m *Metrics) IncIngestRecords(count int64) { m.ingestRecordsTotal.Add(count) }
func (m *Metrics) IncIngestBytes(bytes int64)   { m.ingestBytesTotal.Add(bytes) }
func (m *Metrics) IncIngestBatches()            { m.ingestBatchesTotal.Add(1) }
func (m *Metrics) IncIngestErrors()             { m.ingestErrorsTotal.Add(1) }
func (m *Metrics) IncIngestFlushDeferred()      { m.ingestFlushDeferred.Add(1) }

// SetBufferDeferredBuffers publishes the current deferred-buffer count.
func (m *Metrics) SetBufferDeferredBuffers(n int64) { m.bufferDeferredBuffers.Store(n) }

// MessagePack Metrics
func (m *Metrics) IncMsgPackRequests()           { m.msgpackRequestsTotal.Add(1) }
func (m *Metrics) IncMsgPackRecords(count int64) { m.msgpackRecordsTotal.Add(count) }
func (m *Metrics) IncMsgPackBytes(bytes int64)   { m.msgpackBytesTotal.Add(bytes) }

// Line Protocol Metrics
func (m *Metrics) IncLineProtocolRequests()           { m.lineprotocolRequestsTotal.Add(1) }
func (m *Metrics) IncLineProtocolRecords(count int64) { m.lineprotocolRecordsTotal.Add(count) }
func (m *Metrics) IncLineProtocolBytes(bytes int64)   { m.lineprotocolBytesTotal.Add(bytes) }

// Query Metrics
func (m *Metrics) IncQueryRequests()        { m.queryRequestsTotal.Add(1) }
func (m *Metrics) IncQuerySuccess()         { m.querySuccessTotal.Add(1) }
func (m *Metrics) IncQueryErrors()          { m.queryErrorsTotal.Add(1) }
func (m *Metrics) IncQueryTimeouts()        { m.queryTimeoutsTotal.Add(1) }
func (m *Metrics) IncSlowQueries()          { m.querySlowTotal.Add(1) }
func (m *Metrics) IncQueryRows(count int64) { m.queryRowsTotal.Add(count) }

// RecordQueryLatency records query latency in microseconds
func (m *Metrics) RecordQueryLatency(durationMicros int64) {
	m.RecordQueryLatencyForEngine(QueryEngineDuckDB, durationMicros)
}

// RecordQueryLatencyForEngine preserves the aggregate latency summary while
// recording the completed query under its serving engine.
func (m *Metrics) RecordQueryLatencyForEngine(engine string, durationMicros int64) {
	m.queryLatencySum.Add(durationMicros)
	m.queryLatencyCount.Add(1)
	m.RecordQueryEngineExecution(engine, true, durationMicros)
}

// Label values for the arc_query_client_disconnects_total Prometheus
// counter. Exported so callers in internal/api can reference them via
// these constants instead of repeating the string literal at every
// call site — typos become compile-time errors and the label set
// stays single-source-of-truth here.
const (
	DisconnectPathArrowIPC  = "arrow_ipc"
	DisconnectPathArrowJSON = "arrow_json"
	DisconnectPathSQLJSON   = "sql_json"
)

// IncQueryClientDisconnect increments the per-handler client-disconnect
// counter. `path` MUST be one of the DisconnectPath* constants above;
// any other value is silently dropped so a typo at the call site can't
// emit a malformed labelled time series.
func (m *Metrics) IncQueryClientDisconnect(path string) {
	switch path {
	case DisconnectPathArrowIPC:
		m.queryDisconnectsArrowIPC.Add(1)
	case DisconnectPathArrowJSON:
		m.queryDisconnectsArrowJSON.Add(1)
	case DisconnectPathSQLJSON:
		m.queryDisconnectsSQLJSON.Add(1)
	}
}

// Buffer Metrics
func (m *Metrics) SetBufferRecordsBuffered(count int64) { m.bufferRecordsBuffered.Store(count) }
func (m *Metrics) SetBufferBytesBuffered(bytes int64)   { m.bufferBytesBuffered.Store(bytes) }
func (m *Metrics) SetBufferOldestAge(age time.Duration) { m.bufferOldestAgeNS.Store(int64(age)) }
func (m *Metrics) SetBufferMemoryLimit(bytes int64)     { m.bufferMemoryLimit.Store(bytes) }
func (m *Metrics) SetBufferMemoryPressure(pressure bool) {
	if pressure {
		m.bufferMemoryPressure.Store(1)
	} else {
		m.bufferMemoryPressure.Store(0)
	}
}
func (m *Metrics) SetBufferRecordsWritten(count int64)  { m.bufferRecordsWritten.Store(count) }
func (m *Metrics) SetBufferFlushes(count int64)         { m.bufferFlushesTotal.Store(count) }
func (m *Metrics) SetBufferErrors(count int64)          { m.bufferErrorsTotal.Store(count) }
func (m *Metrics) IncBufferFlushFailures()              { m.bufferFlushFailures.Add(1) }
func (m *Metrics) SetBufferQueueDepth(depth int64)      { m.bufferQueueDepth.Store(depth) }
func (m *Metrics) SetBufferRecordsQueued(count int64)   { m.bufferRecordsQueued.Store(count) }
func (m *Metrics) SetBufferRecordsInflight(count int64) { m.bufferRecordsInflight.Store(count) }

// Storage Metrics
func (m *Metrics) IncStorageWrites()                { m.storageWritesTotal.Add(1) }
func (m *Metrics) IncStorageWriteBytes(bytes int64) { m.storageWriteBytesTotal.Add(bytes) }
func (m *Metrics) IncStorageReads()                 { m.storageReadsTotal.Add(1) }
func (m *Metrics) IncStorageReadBytes(bytes int64)  { m.storageReadBytesTotal.Add(bytes) }
func (m *Metrics) IncStorageErrors()                { m.storageErrorsTotal.Add(1) }

// Compaction Metrics
func (m *Metrics) IncCompactionJobs()        { m.compactionJobsTotal.Add(1) }
func (m *Metrics) IncCompactionSuccess()     { m.compactionJobsSuccess.Add(1) }
func (m *Metrics) IncCompactionFailed()      { m.compactionJobsFailed.Add(1) }
func (m *Metrics) IncCompactionInterrupted() { m.compactionJobsInterrupted.Add(1) }
func (m *Metrics) IncCompactionPartitionsSkippedTooWide() {
	m.compactionPartitionsSkippedTooWide.Add(1)
}
func (m *Metrics) SetCompactionPartitionsTooWide(n int64)  { m.compactionPartitionsTooWide.Store(n) }
func (m *Metrics) IncCompactionFilesCompacted(count int64) { m.compactionFilesCompacted.Add(count) }
func (m *Metrics) IncCompactionBytesRead(bytes int64)      { m.compactionBytesRead.Add(bytes) }
func (m *Metrics) IncCompactionBytesWritten(bytes int64)   { m.compactionBytesWritten.Add(bytes) }
func (m *Metrics) IncCompactionManifestsRecovered(count int64) {
	m.compactionManifestsRecovered.Add(count)
}

// EnableEdgeSyncSpokeScheduler makes scheduled network metrics visible.
// It is called only after the network scheduler has been constructed.
func (m *Metrics) EnableEdgeSyncSpokeScheduler() {
	m.edgeSyncSpokeSchedulerEnabled.Store(true)
}

// RecordEdgeSyncSpokeSuccess records a completed scheduled network pass.
func (m *Metrics) RecordEdgeSyncSpokeSuccess(at time.Time) {
	if m.edgeSyncSpokeSchedulerEnabled.Load() {
		m.edgeSyncSpokeLastSuccessUnix.Store(at.Unix())
	}
}

// IncEdgeSyncSpokePassFailures counts failed scheduled passes, excluding
// skipped overlaps, role-gated attempts and shutdown cancellation.
func (m *Metrics) IncEdgeSyncSpokePassFailures() {
	if m.edgeSyncSpokeSchedulerEnabled.Load() {
		m.edgeSyncSpokePassFailures.Add(1)
	}
}

// Auth Metrics
func (m *Metrics) IncAuthRequests()  { m.authRequestsTotal.Add(1) }
func (m *Metrics) IncAuthCacheHit()  { m.authCacheHits.Add(1) }
func (m *Metrics) IncAuthCacheMiss() { m.authCacheMisses.Add(1) }
func (m *Metrics) IncAuthFailures()  { m.authFailuresTotal.Add(1) }

// Database Metrics

// SetDBPoolStats records a sql.DBStats sample from the DuckDB connection pool.
//
// Taken as one call rather than a setter per field so a single sample lands
// coherently: separate setters would let a scrape observe OpenConnections from
// one instant and InUse from another, producing in-use > open. Called from the
// metrics handlers at read time (see api.Server.SetDBStats) — sql.DBStats is a
// cheap mutex-guarded read, and the values are only meaningful at the moment
// they are observed, so sampling on demand beats a background ticker.
func (m *Metrics) SetDBPoolStats(st sql.DBStats) {
	m.dbConnectionsMax.Store(int64(st.MaxOpenConnections))
	m.dbConnectionsOpen.Store(int64(st.OpenConnections))
	m.dbConnectionsInUse.Store(int64(st.InUse))
	m.dbConnectionsIdle.Store(int64(st.Idle))
	m.dbWaitCount.Store(st.WaitCount)
	m.dbWaitMicros.Store(st.WaitDuration.Microseconds())
}

func (m *Metrics) IncDBQueries()     { m.dbQueriesTotal.Add(1) }
func (m *Metrics) IncDBQueryErrors() { m.dbQueryErrorsTotal.Add(1) }

// Audit Metrics
//
// The batch variants exist because audit events are written in batches: a
// failed transaction loses every event in the batch, not one, so counting a
// single error would understate the loss (#802).
func (m *Metrics) IncAuditEvents()             { m.auditEventsTotal.Add(1) }
func (m *Metrics) AddAuditEvents(n int64)      { m.auditEventsTotal.Add(n) }
func (m *Metrics) IncAuditWriteErrors()        { m.auditWriteErrors.Add(1) }
func (m *Metrics) AddAuditWriteErrors(n int64) { m.auditWriteErrors.Add(n) }

// IncAuditEventsDropped counts audit events discarded before they were ever
// queued, because the event channel was full. These never reach the writer, so
// they are invisible to auditWriteErrors — and an audit trail with silent gaps
// is worse than one that reports them (#802).
func (m *Metrics) IncAuditEventsDropped() { m.auditEventsDropped.Add(1) }

// MQTT Metrics
func (m *Metrics) IncMQTTMessagesReceived()         { m.mqttMessagesReceived.Add(1) }
func (m *Metrics) IncMQTTMessagesFailed()           { m.mqttMessagesFailed.Add(1) }
func (m *Metrics) IncMQTTBytesReceived(bytes int64) { m.mqttBytesReceived.Add(bytes) }
func (m *Metrics) IncMQTTDecodeSuccess()            { m.mqttDecodeSuccess.Add(1) }
func (m *Metrics) IncMQTTDecodeErrors()             { m.mqttDecodeErrors.Add(1) }
func (m *Metrics) SetMQTTConnected(connected bool) {
	if connected {
		m.mqttConnected.Store(1)
	} else {
		m.mqttConnected.Store(0)
	}
}
func (m *Metrics) IncMQTTReconnects() { m.mqttReconnects.Add(1) }

// WAL Metrics
func (m *Metrics) IncWALRecordsPreserved(count int64) { m.walRecordsPreserved.Add(count) }

// SetWALRegisteredFloorLag records how many committed ingest batches are not
// yet below the registered floor of their key (#1207).
func (m *Metrics) SetWALRegisteredFloorLag(n int64) { m.walRegisteredFloorLag.Store(n) }

// SetWALHeldFiles reports the WAL files kept because batches in them are
// waiting for their files to register (#1207 S4c).
func (m *Metrics) SetWALHeldFiles(n int64) { m.walHeldFiles.Store(n) }

// SetWALParked reports the refused file registrations parked out of the
// query path, and their bytes (#1207 S4b).
func (m *Metrics) SetWALParked(files, bytes int64) {
	m.walParkedFiles.Store(files)
	m.walParkedBytes.Store(bytes)
}

// IncWALCoverageLost counts ingest batches that settled without being in a
// registered file (#1207).
func (m *Metrics) IncWALCoverageLost(n int64) { m.walCoverageLost.Add(n) }

// Origin stream and standby store (#1207 S3).
func (m *Metrics) SetOriginStreamSessions(n int64) { m.originStreamSessions.Store(n) }
func (m *Metrics) IncOriginStreamDropped()         { m.originStreamDropped.Add(1) }
func (m *Metrics) SetStandbyStreamConnected(c bool) {
	if c {
		m.standbyStreamConnected.Store(1)
	} else {
		m.standbyStreamConnected.Store(0)
	}
}
func (m *Metrics) IncStandbyStreamEntries()            { m.standbyStreamEntries.Add(1) }
func (m *Metrics) IncStandbyStreamGaps()               { m.standbyStreamGaps.Add(1) }
func (m *Metrics) AddStandbyStoreTrimmedBytes(n int64) { m.standbyStoreTrimmedBytes.Add(n) }
func (m *Metrics) IncStandbyStoreDropped()             { m.standbyStoreDropped.Add(1) }
func (m *Metrics) AddStandbyStoreEvictedBytes(n int64) { m.standbyStoreEvicted.Add(n) }
func (m *Metrics) SetDepartedWriters(n int64)          { m.departedWriters.Store(n) }

// SetOldestStreamClaimAge records how long the oldest unfinished stream
// claim has been open, in seconds (0: none).
func (m *Metrics) SetOldestStreamClaimAge(seconds int64) {
	m.oldestClaimAge.Store(seconds)
}

// SetStreamTakeoverUnpulledFiles sets how many registered files of the
// instances this node is claiming it does not hold yet.
func (m *Metrics) SetStreamTakeoverUnpulledFiles(n int64) {
	m.unpulledFiles.Store(n)
}

func (m *Metrics) SetStreamClaims(enabled bool, claims int64) {
	if enabled {
		m.streamClaimsEnabled.Store(1)
	} else {
		m.streamClaimsEnabled.Store(0)
	}
	m.streamClaims.Store(claims)
}
func (m *Metrics) IncWALCoverageRefused(n int64) { m.walCoverageRefused.Add(n) }
func (m *Metrics) SetStandbyStoreTrimState(lost, blocked, replacedBytes int64) {
	m.standbyStoreLost.Store(lost)
	m.standbyStoreBlocked.Store(blocked)
	m.standbyStoreReplaced.Store(replacedBytes)
}
func (m *Metrics) SetStandbyStorePinned(n int64) { m.standbyStorePinned.Store(n) }

// SetStandbyStoreCapped, SetStandbyStoreLowSpace and SetStandbyStoreMaxBytes
// report the standby store's cap state (#1266).
func (m *Metrics) SetStandbyStoreCapped(v bool)    { m.standbyStoreCapped.Store(b2i(v)) }
func (m *Metrics) SetStandbyStoreLowSpace(v bool)  { m.standbyStoreLowSpace.Store(b2i(v)) }
func (m *Metrics) SetStandbyStoreMaxBytes(n int64) { m.standbyStoreMaxBytes.Store(n) }

func b2i(v bool) int64 {
	if v {
		return 1
	}
	return 0
}
func (m *Metrics) SetStandbyStore(bytes, incomplete int64) {
	m.standbyStoreBytes.Store(bytes)
	m.standbyStoreIncomplete.Store(incomplete)
}
func (m *Metrics) IncWALRecoveryTotal()              { m.walRecoveryTotal.Add(1) }
func (m *Metrics) IncWALRecoveryRecords(count int64) { m.walRecoveryRecords.Add(count) }
func (m *Metrics) IncWALDroppedEntries()             { m.walDroppedEntries.Add(1) }
func (m *Metrics) IncWALFailedWrites()               { m.walFailedWrites.Add(1) }
func (m *Metrics) IncWALDiskFullWrites()             { m.walDiskFullWrites.Add(1) }
func (m *Metrics) IncWALRecoveredWrites()            { m.walRecoveredWrites.Add(1) }
func (m *Metrics) IncWALFailedSyncs()                { m.walFailedSyncs.Add(1) }
func (m *Metrics) IncWALOversizedPayloads()          { m.walOversizedPayloads.Add(1) }
func (m *Metrics) SetWALDirectoryBytes(bytes int64)  { m.walDirectoryBytes.Store(bytes) }

// SetWALDiskPressure records whether live writes are refused because the WAL
// volume is low on free space (#676).
func (m *Metrics) SetWALDiskPressure(on bool) {
	if on {
		m.walDiskPressure.Store(1)
	} else {
		m.walDiskPressure.Store(0)
	}
}
func (m *Metrics) SetWALDiskFreeBytes(bytes int64) { m.walDiskFreeBytes.Store(bytes) }
func (m *Metrics) IncWALDiskPressureRejections()   { m.walDiskPressureRejections.Add(1) }
func (m *Metrics) IncWALDiskStatErrors()           { m.walDiskStatErrors.Add(1) }

// SetReconciliationStorageSweepHeld records whether the last reconciliation
// run held its orphan-storage sweep (#1214).
func (m *Metrics) SetReconciliationStorageSweepHeld(held bool) {
	v := int64(0)
	if held {
		v = 1
	}
	m.reconciliationStorageSweepHeld.Store(v)
}

// ReconciliationStorageSweepHeld reports the last value set.
func (m *Metrics) ReconciliationStorageSweepHeld() bool {
	return m.reconciliationStorageSweepHeld.Load() == 1
}

// File registration outbox (#1209).
func (m *Metrics) SetFileRegistrationsPending(n int64) { m.fileRegistrationsPending.Store(n) }
func (m *Metrics) SetFileRegistrationOldestPending(ns int64) {
	m.fileRegistrationOldestPending.Store(ns)
}
func (m *Metrics) SetFileRegistrationsConfirmed(n int64) { m.fileRegistrationsConfirmed.Store(n) }
func (m *Metrics) IncFileRegistrationJournalErrors()     { m.fileRegistrationJournalErrs.Add(1) }

// SetIngestFenced records whether ingest fencing refuses writes (#1207).
func (m *Metrics) SetIngestFenced(fenced bool) {
	if fenced {
		m.ingestFenced.Store(1)
	} else {
		m.ingestFenced.Store(0)
	}
}

// IncIngestFencedWrites counts a live write refused by the ingest gate (#1207),
// under its reason (#1268).
func (m *Metrics) IncIngestFencedWrites(reason string) {
	for i, r := range IngestFencedReasons {
		if r == reason {
			m.ingestFencedWrites[i].Add(1)
			return
		}
	}
	m.ingestFencedWrites[len(IngestFencedReasons)-1].Add(1)
}

// WriteForwardResults are the result labels of
// arc_cluster_writes_forwarded_total (#1270).
var WriteForwardResults = [...]string{"ok", "refused", "error", "unavailable", "ambiguous"}

// IncWritesForwarded counts one write this node forwarded to a writer, by
// result: ok (2xx), refused (the writer's own fence refusal, 503), error (any
// other answer), unavailable (nothing sent: nothing written), ambiguous (sent,
// no answer: may have been written).
func (m *Metrics) IncWritesForwarded(result string) {
	for i, r := range WriteForwardResults {
		if r == result {
			m.writesForwarded[i].Add(1)
			return
		}
	}
}

// WritesForwardedFor returns the forwarded writes counted under result.
func (m *Metrics) WritesForwardedFor(result string) int64 {
	for i, r := range WriteForwardResults {
		if r == result {
			return m.writesForwarded[i].Load()
		}
	}
	return 0
}

// IngestFencedReasons are the reason labels of
// arc_cluster_ingest_fenced_writes_total, in export order (#1268). They match
// cluster's Refusal constants; anything else counts as "other", the last.
var IngestFencedReasons = [...]string{"contact", "standby", "claimed", "announcing", "not_designated", "delete_ack", "other"} // ingest.Refusal*

// IngestFencedWritesFor returns the refusals counted under reason ("other" for
// an unknown one).
func (m *Metrics) IngestFencedWritesFor(reason string) int64 {
	for i, r := range IngestFencedReasons {
		if r == reason {
			return m.ingestFencedWrites[i].Load()
		}
	}
	return m.ingestFencedWrites[len(IngestFencedReasons)-1].Load()
}

func (m *Metrics) ingestFencedWritesTotal() int64 {
	var n int64
	for i := range m.ingestFencedWrites {
		n += m.ingestFencedWrites[i].Load()
	}
	return n
}

func (m *Metrics) fileRegistrationOldestPendingSeconds() float64 {
	ns := m.fileRegistrationOldestPending.Load()
	if ns == 0 {
		return 0
	}
	return time.Since(time.Unix(0, ns)).Seconds()
}
func (m *Metrics) IncWALQuarantinedFiles() { m.walQuarantinedFiles.Add(1) }

// AddWALQuarantineReleased counts pending WAL entries the writer stopped
// waiting for because recovery quarantined their file (#1225).
func (m *Metrics) AddWALQuarantineReleased(n int64) { m.walQuarantineReleased.Add(n) }

// SetWALPartialRowRecoveryPending records whether the last WAL recovery pass
// retained a file whose parent entry had row ranges replayed. It makes the
// operations
// guide's "drain recovery with this version before downgrading" checkable
// rather than aspirational.
func (m *Metrics) SetWALPartialRowRecoveryPending(pending bool) {
	value := int64(0)
	if pending {
		value = 1
	}
	m.walPartialRowRecovery.Store(value)
}

// Decompression Pool Metrics

// Governance Metrics
func (m *Metrics) IncGovernanceRateLimited()           { m.governanceRateLimited.Add(1) }
func (m *Metrics) IncGovernanceQuotaExhausted()        { m.governanceQuotaExhausted.Add(1) }
func (m *Metrics) IncGovernanceQueriesCapped()         { m.governanceQueriesCapped.Add(1) }
func (m *Metrics) SetGovernancePoliciesActive(n int64) { m.governancePoliciesActive.Store(n) }

// Query Management Metrics
func (m *Metrics) SetQueryMgmtActiveQueries(n int64) { m.queryMgmtActiveQueries.Store(n) }
func (m *Metrics) IncQueryMgmtCancelled()            { m.queryMgmtCancelledTotal.Add(1) }
func (m *Metrics) SetQueryMgmtHistorySize(n int64)   { m.queryMgmtHistorySize.Store(n) }

// IncClusterManifestRejectedPaths increments the cluster FSM
// path-rejection counter. Called from internal/cluster/raft/fsm.go
// when ValidateManifestPath refuses a Register/Update/Restore path.
// See GHSA-f85q-mvg8-qf37 for the security context.
func (m *Metrics) IncClusterManifestRejectedPaths() { m.clusterManifestRejectedPathsTotal.Add(1) }

// IncStorageInvalidPathQuarantined records one entry dropped from a cleanup,
// reconciliation or replication work set because a storage call returned
// storage.ErrInvalidPath (#747). That error is permanent, so the alternative
// to counting it here is a loop that retries the same key forever.
func (m *Metrics) IncStorageInvalidPathQuarantined() { m.storageInvalidPathQuarantinedTotal.Add(1) }

// Counters a compaction subprocess moves in its own registry, which nothing
// scrapes (#1343): it returns them with its result and the parent adds them.
var childCounterKeys = []string{
	"storage_writes", "storage_write_bytes", "storage_reads", "storage_read_bytes",
	"storage_errors", "storage_invalid_path_quarantined",
}

func (m *Metrics) childCounter(key string) *atomic.Int64 {
	switch key {
	case "storage_writes":
		return &m.storageWritesTotal
	case "storage_write_bytes":
		return &m.storageWriteBytesTotal
	case "storage_reads":
		return &m.storageReadsTotal
	case "storage_read_bytes":
		return &m.storageReadBytesTotal
	case "storage_errors":
		return &m.storageErrorsTotal
	case "storage_invalid_path_quarantined":
		return &m.storageInvalidPathQuarantinedTotal
	}
	return nil
}

// ChildCounters is, in a compaction subprocess, the counters it moved that
// the parent must add (see AddChildCounters). Zero values are left out.
func (m *Metrics) ChildCounters() map[string]int64 {
	out := map[string]int64{}
	for _, k := range childCounterKeys {
		if v := m.childCounter(k).Load(); v > 0 {
			out[k] = v
		}
	}
	return out
}

// AddChildCounters adds a compaction subprocess's ChildCounters. A key this
// binary does not know (a child from another version) or a negative value is
// ignored; nil adds nothing.
func (m *Metrics) AddChildCounters(counts map[string]int64) {
	for k, v := range counts {
		if c := m.childCounter(k); c != nil && v > 0 {
			c.Add(v)
		}
	}
}

// IncFileTimePruningPass records a file-time pruning pass over files listed.
func (m *Metrics) IncFileTimePruningPass(files int) {
	m.fileTimePruningPasses.Add(1)
	m.fileTimePruningFilesListed.Add(int64(files))
}

// IncFileTimePruningExpanded records a pass that dropped files from the list.
func (m *Metrics) IncFileTimePruningExpanded(dropped int) {
	m.fileTimePruningExpanded.Add(1)
	m.fileTimePruningFilesDropped.Add(int64(dropped))
}

// IncFileTimePruningNothingDropped records a pass with every file in range.
func (m *Metrics) IncFileTimePruningNothingDropped() { m.fileTimePruningNothingDropped.Add(1) }

// IncFileTimePruningCapFallback records a pass that kept too many files to list.
func (m *Metrics) IncFileTimePruningCapFallback() { m.fileTimePruningCapFallback.Add(1) }

// IncFileTimePruningZeroSurvivors records a pass that kept no file.
func (m *Metrics) IncFileTimePruningZeroSurvivors() { m.fileTimePruningZeroSurvivors.Add(1) }

// AddIngestFutureStampedRows records rows stamped more than the file-time
// pruning margin past their flush, in its hour.
func (m *Metrics) AddIngestFutureStampedRows(n int) { m.ingestFutureStampedRows.Add(int64(n)) }

// AddFileTimePruningAnomalousKept records files kept because their name time
// is before their directory's hour (future-stamped rows).
func (m *Metrics) AddFileTimePruningAnomalousKept(n int) {
	if n > 0 {
		m.fileTimePruningAnomalousKept.Add(int64(n))
	}
}

// IncCompactionCopyFallback, IncCompactionFinished, IncCompactionWedged: the
// staged compaction commit on an object store (#1207 d2a2b).
func (m *Metrics) IncCompactionCopyFallback() { m.compactionCopyFallbackTotal.Add(1) }
func (m *Metrics) IncCompactionFinished()     { m.compactionFinishedTotal.Add(1) }
func (m *Metrics) IncCompactionWedged()       { m.compactionWedgedTotal.Add(1) }

// IncManifestRepairDropped, IncManifestRepairAdopted: the shared-object-store
// manifest repair (#1207).
func (m *Metrics) IncManifestRepairDropped() { m.manifestRepairDroppedTotal.Add(1) }
func (m *Metrics) IncManifestRepairAdopted() { m.manifestRepairAdoptedTotal.Add(1) }

// SetManifestRepairPending publishes the compaction outputs the last repair
// left for a later run (its read budget reached).
func (m *Metrics) SetManifestRepairPending(n int64) { m.manifestRepairPending.Store(n) }

// IncStaleLocalUnlinked counts a local copy the stale-local sweep removed.
func (m *Metrics) IncStaleLocalUnlinked() { m.staleLocalUnlinkedTotal.Add(1) }

// SetStaleLocalTracked publishes how many local files the sweep tracks.
func (m *Metrics) SetStaleLocalTracked(n int64) { m.staleLocalTracked.Store(n) }

// SetStaleLocalBlocked publishes 1 while the sweep's breaker refuses passes.
func (m *Metrics) SetStaleLocalBlocked(n int64) { m.staleLocalBlocked.Store(n) }

// SetStaleLocalTrackingFailed publishes 1 while the sweep cannot track files
// (its journal could not be opened or written).
func (m *Metrics) SetStaleLocalTrackingFailed(n int64) { m.staleLocalTrackingFail.Store(n) }

// SetClusterLocalDeletePending publishes how many manifest deletes are still
// waiting for a local unlink on this node.
func (m *Metrics) SetClusterLocalDeletePending(n int64) { m.clusterLocalDeletePending.Store(n) }

// SetClusterLocalDeleteDeferred publishes how many local unlinks wait for a
// DELETE rewrite to arrive on this node.
func (m *Metrics) SetClusterLocalDeleteDeferred(n int64) { m.clusterLocalDeleteDeferred.Store(n) }

// IncCompactionManifestParkedUnparseable records one crash-recovery manifest
// parked because its body could not be decoded (#926). Call it after the
// park succeeded; a park that fails transiently is retried next cycle and
// must not be reported as a drop from the work set that did not happen.
func (m *Metrics) IncCompactionManifestParkedUnparseable() {
	m.compactionManifestsParkedUnparseableTotal.Add(1)
}

// SetStorageUnaddressableFiles records how many data files the backup that just
// ran could not copy because no listing returns them (#756). Called on every
// backup including with 0, so fixing the files clears the gauge.
func (m *Metrics) SetStorageUnaddressableFiles(n int64) {
	m.storageUnaddressableFiles.Store(n)
}

// SetBackupSkippedFiles records how many files the backup that just finished
// its copy phases inventoried but could not store (#977). Called with 0 on a
// clean run, so fixing the files clears the gauge.
func (m *Metrics) SetBackupSkippedFiles(n int64) {
	m.backupSkippedFiles.Store(n)
}

// Cluster Auth metrics — incremented from the FSM apply path on every
// Token command. apply_* counts successful applies per type;
// IncClusterAuthRejected counts applier-side validation refusals.
// Phase A: Cluster Auth Convergence.
func (m *Metrics) IncClusterHeartbeatUnknownNode() { m.clusterHeartbeatsUnknownNodeTotal.Add(1) }

func (m *Metrics) IncClusterAuthApplyCreate() { m.clusterAuthApplyCreateTotal.Add(1) }

// RecordClusterAuthReconcileFailure counts a reconcile that failed before
// anything committed; the gauges keep the last run's counts.
func (m *Metrics) RecordClusterAuthReconcileFailure() {
	m.clusterAuthReconcileTotal.Add(1)
	m.clusterAuthReconcileFailedTotal.Add(1)
}

// RecordClusterAuthReconcile records one reconcile of the local auth and RBAC
// tables with the cluster FSM (#1336). failed counts a run that failed or hit
// statement errors; the gauges take the counts of whatever it did apply.
func (m *Metrics) RecordClusterAuthReconcile(failed bool, localRows, collisions, keptRows int) {
	m.clusterAuthReconcileTotal.Add(1)
	if failed {
		m.clusterAuthReconcileFailedTotal.Add(1)
	}
	m.clusterAuthReconcileLocalRows.Store(int64(localRows))
	m.clusterAuthReconcileCollisions.Store(int64(collisions))
	m.clusterAuthReconcileKeptRows.Store(int64(keptRows))
}
func (m *Metrics) IncClusterAuthApplyUpdate() { m.clusterAuthApplyUpdateTotal.Add(1) }
func (m *Metrics) IncClusterAuthApplyRevoke() { m.clusterAuthApplyRevokeTotal.Add(1) }
func (m *Metrics) IncClusterAuthApplyDelete() { m.clusterAuthApplyDeleteTotal.Add(1) }
func (m *Metrics) IncClusterAuthApplyRotate() { m.clusterAuthApplyRotateTotal.Add(1) }
func (m *Metrics) IncClusterAuthRejected()    { m.clusterAuthRejectedTotal.Add(1) }

// Phase A.1 (RBAC): one Inc method per command type + a single rejected
// counter. Same shape as the Phase A auth counters above.
func (m *Metrics) IncClusterRBACApplyCreateOrganization() {
	m.clusterRBACApplyCreateOrganizationTotal.Add(1)
}
func (m *Metrics) IncClusterRBACApplyUpdateOrganization() {
	m.clusterRBACApplyUpdateOrganizationTotal.Add(1)
}
func (m *Metrics) IncClusterRBACApplyDeleteOrganization() {
	m.clusterRBACApplyDeleteOrganizationTotal.Add(1)
}
func (m *Metrics) IncClusterRBACApplyCreateTeam() { m.clusterRBACApplyCreateTeamTotal.Add(1) }
func (m *Metrics) IncClusterRBACApplyUpdateTeam() { m.clusterRBACApplyUpdateTeamTotal.Add(1) }
func (m *Metrics) IncClusterRBACApplyDeleteTeam() { m.clusterRBACApplyDeleteTeamTotal.Add(1) }
func (m *Metrics) IncClusterRBACApplyCreateRole() { m.clusterRBACApplyCreateRoleTotal.Add(1) }
func (m *Metrics) IncClusterRBACApplyUpdateRole() { m.clusterRBACApplyUpdateRoleTotal.Add(1) }
func (m *Metrics) IncClusterRBACApplyDeleteRole() { m.clusterRBACApplyDeleteRoleTotal.Add(1) }
func (m *Metrics) IncClusterRBACApplyCreateMeasurementPermission() {
	m.clusterRBACApplyCreateMeasurementPermissionTotal.Add(1)
}
func (m *Metrics) IncClusterRBACApplyDeleteMeasurementPermission() {
	m.clusterRBACApplyDeleteMeasurementPermissionTotal.Add(1)
}
func (m *Metrics) IncClusterRBACApplyAddTokenToTeam() {
	m.clusterRBACApplyAddTokenToTeamTotal.Add(1)
}
func (m *Metrics) IncClusterRBACApplyRemoveTokenFromTeam() {
	m.clusterRBACApplyRemoveTokenFromTeamTotal.Add(1)
}
func (m *Metrics) IncClusterRBACRejected()        { m.clusterRBACRejectedTotal.Add(1) }
func (m *Metrics) IncClusterRBACCascadeRejected() { m.clusterRBACCascadeRejectedTotal.Add(1) }

// Snapshot returns all metrics as a map (for JSON endpoint)
func (m *Metrics) Snapshot() map[string]interface{} {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	snapshot := map[string]interface{}{
		// Process info
		"uptime_seconds": time.Since(m.startTime).Seconds(),

		// arcx decline census (closed label set; all-zero in stock builds —
		// only the arcx_engine-tagged census path increments these).
		"arcx_shape_census": m.arcxCensusSnapshot(),
		// arcx shadow outcomes (closed label set; all-zero in stock builds).
		// `skipped` is the one to watch when reading the others: a shed or
		// capped sample is NOT evidence that arcx matched.
		"arcx_shadow": m.arcxShadowSnapshot(),
		"goroutines":  runtime.NumGoroutine(),
		"go_version":  runtime.Version(),
		"num_cpu":     runtime.NumCPU(),
		"gomaxprocs":  runtime.GOMAXPROCS(0),

		// Memory (Go runtime)
		"memory_alloc_bytes":       memStats.Alloc,
		"memory_total_alloc_bytes": memStats.TotalAlloc,
		"memory_sys_bytes":         memStats.Sys,
		"memory_heap_alloc_bytes":  memStats.HeapAlloc,
		"memory_heap_sys_bytes":    memStats.HeapSys,
		"memory_heap_inuse_bytes":  memStats.HeapInuse,
		"memory_stack_inuse_bytes": memStats.StackInuse,
		"gc_cycles":                memStats.NumGC,
		"gc_pause_total_ns":        memStats.PauseTotalNs,

		// HTTP
		"http_requests_total":   m.httpRequestsTotal.Load(),
		"http_requests_success": m.httpRequestsSuccess.Load(),
		"http_requests_error":   m.httpRequestsError.Load(),
		"http_latency_sum_us":   m.httpLatencySum.Load(),
		"http_latency_count":    m.httpLatencyCount.Load(),

		// Ingestion
		"ingest_records_total":        m.ingestRecordsTotal.Load(),
		"ingest_bytes_total":          m.ingestBytesTotal.Load(),
		"ingest_batches_total":        m.ingestBatchesTotal.Load(),
		"ingest_errors_total":         m.ingestErrorsTotal.Load(),
		"ingest_flush_deferred_total": m.ingestFlushDeferred.Load(),
		"buffer_deferred_buffers":     m.bufferDeferredBuffers.Load(),

		// MessagePack
		"msgpack_requests_total": m.msgpackRequestsTotal.Load(),
		"msgpack_records_total":  m.msgpackRecordsTotal.Load(),
		"msgpack_bytes_total":    m.msgpackBytesTotal.Load(),

		// Line Protocol
		"lineprotocol_requests_total": m.lineprotocolRequestsTotal.Load(),
		"lineprotocol_records_total":  m.lineprotocolRecordsTotal.Load(),
		"lineprotocol_bytes_total":    m.lineprotocolBytesTotal.Load(),

		// Query
		"query_requests_total": m.queryRequestsTotal.Load(),
		"query_success_total":  m.querySuccessTotal.Load(),
		"query_errors_total":   m.queryErrorsTotal.Load(),
		"query_timeouts_total": m.queryTimeoutsTotal.Load(),
		"query_slow_total":     m.querySlowTotal.Load(),
		"query_rows_total":     m.queryRowsTotal.Load(),
		"query_latency_sum_us": m.queryLatencySum.Load(),
		"query_latency_count":  m.queryLatencyCount.Load(),
		"query_engine_executions": map[string]map[string]int64{
			QueryEngineDuckDB: queryEngineExecutionSnapshot(&m.queryExecutions[0]),
			QueryEngineArcx:   queryEngineExecutionSnapshot(&m.queryExecutions[1]),
		},

		"query_client_disconnects_arrow_ipc_total":  m.queryDisconnectsArrowIPC.Load(),
		"query_client_disconnects_arrow_json_total": m.queryDisconnectsArrowJSON.Load(),
		"query_client_disconnects_sql_json_total":   m.queryDisconnectsSQLJSON.Load(),

		// Buffer
		"buffer_records_buffered":         m.bufferRecordsBuffered.Load(),
		"buffer_bytes_buffered":           m.bufferBytesBuffered.Load(),
		"buffer_oldest_unflushed_seconds": float64(m.bufferOldestAgeNS.Load()) / float64(time.Second),
		"buffer_memory_limit_bytes":       m.bufferMemoryLimit.Load(),
		"buffer_memory_pressure":          m.bufferMemoryPressure.Load(),
		"buffer_records_written":          m.bufferRecordsWritten.Load(),
		"buffer_flushes_total":            m.bufferFlushesTotal.Load(),
		"buffer_errors_total":             m.bufferErrorsTotal.Load(),
		"buffer_flush_failures_total":     m.bufferFlushFailures.Load(),
		"buffer_queue_depth":              m.bufferQueueDepth.Load(),
		"buffer_records_queued":           m.bufferRecordsQueued.Load(),
		"buffer_records_inflight":         m.bufferRecordsInflight.Load(),

		// Storage
		"storage_writes_total":      m.storageWritesTotal.Load(),
		"storage_write_bytes_total": m.storageWriteBytesTotal.Load(),
		"storage_reads_total":       m.storageReadsTotal.Load(),
		"storage_read_bytes_total":  m.storageReadBytesTotal.Load(),
		"storage_errors_total":      m.storageErrorsTotal.Load(),

		// Compaction
		"compaction_jobs_total":                  m.compactionJobsTotal.Load(),
		"compaction_jobs_success":                m.compactionJobsSuccess.Load(),
		"compaction_jobs_failed":                 m.compactionJobsFailed.Load(),
		"compaction_jobs_interrupted":            m.compactionJobsInterrupted.Load(),
		"compaction_partitions_skipped_too_wide": m.compactionPartitionsSkippedTooWide.Load(),
		"compaction_partitions_too_wide":         m.compactionPartitionsTooWide.Load(),
		"compaction_files_compacted":             m.compactionFilesCompacted.Load(),
		"compaction_bytes_read":                  m.compactionBytesRead.Load(),
		"compaction_bytes_written":               m.compactionBytesWritten.Load(),
		"compaction_manifests_recovered":         m.compactionManifestsRecovered.Load(),

		// Auth
		"auth_requests_total": m.authRequestsTotal.Load(),
		"auth_cache_hits":     m.authCacheHits.Load(),
		"auth_cache_misses":   m.authCacheMisses.Load(),
		"auth_failures_total": m.authFailuresTotal.Load(),

		// Database
		"db_connections_max":    m.dbConnectionsMax.Load(),
		"db_connections_open":   m.dbConnectionsOpen.Load(),
		"db_wait_count":         m.dbWaitCount.Load(),
		"db_wait_micros":        m.dbWaitMicros.Load(),
		"db_connections_in_use": m.dbConnectionsInUse.Load(),
		"db_connections_idle":   m.dbConnectionsIdle.Load(),
		"db_queries_total":      m.dbQueriesTotal.Load(),
		"db_query_errors_total": m.dbQueryErrorsTotal.Load(),

		// Audit
		"audit_events_total":   m.auditEventsTotal.Load(),
		"audit_write_errors":   m.auditWriteErrors.Load(),
		"audit_events_dropped": m.auditEventsDropped.Load(),

		// MQTT
		"mqtt_messages_received": m.mqttMessagesReceived.Load(),
		"mqtt_messages_failed":   m.mqttMessagesFailed.Load(),
		"mqtt_bytes_received":    m.mqttBytesReceived.Load(),
		"mqtt_decode_success":    m.mqttDecodeSuccess.Load(),
		"mqtt_decode_errors":     m.mqttDecodeErrors.Load(),
		"mqtt_connected":         m.mqttConnected.Load(),
		"mqtt_reconnects":        m.mqttReconnects.Load(),

		// WAL
		"wal_records_preserved":             m.walRecordsPreserved.Load(),
		"wal_registered_floor_lag":          m.walRegisteredFloorLag.Load(),
		"wal_held_files":                    m.walHeldFiles.Load(),
		"wal_parked_files":                  m.walParkedFiles.Load(),
		"wal_parked_bytes":                  m.walParkedBytes.Load(),
		"wal_coverage_lost":                 m.walCoverageLost.Load(),
		"origin_stream_sessions":            m.originStreamSessions.Load(),
		"origin_stream_dropped":             m.originStreamDropped.Load(),
		"standby_stream_connected":          m.standbyStreamConnected.Load(),
		"standby_stream_entries":            m.standbyStreamEntries.Load(),
		"standby_stream_gaps":               m.standbyStreamGaps.Load(),
		"standby_store_bytes":               m.standbyStoreBytes.Load(),
		"standby_store_incomplete_keys":     m.standbyStoreIncomplete.Load(),
		"standby_store_trimmed_bytes":       m.standbyStoreTrimmedBytes.Load(),
		"standby_store_dropped":             m.standbyStoreDropped.Load(),
		"standby_store_evicted_bytes":       m.standbyStoreEvicted.Load(),
		"standby_store_lost_records":        m.standbyStoreLost.Load(),
		"standby_store_blocked_keys":        m.standbyStoreBlocked.Load(),
		"standby_store_replaced_bytes":      m.standbyStoreReplaced.Load(),
		"standby_store_pinned_bytes":        m.standbyStorePinned.Load(),
		"standby_store_capped":              m.standbyStoreCapped.Load(),
		"standby_store_low_space":           m.standbyStoreLowSpace.Load(),
		"standby_store_max_bytes":           m.standbyStoreMaxBytes.Load(),
		"stream_claims_enabled":             m.streamClaimsEnabled.Load(),
		"stream_claims":                     m.streamClaims.Load(),
		"stream_takeover_departed_writers":  m.departedWriters.Load(),
		"stream_takeover_oldest_claim_age":  m.oldestClaimAge.Load(),
		"stream_takeover_unpulled_files":    m.unpulledFiles.Load(),
		"wal_coverage_refused":              m.walCoverageRefused.Load(),
		"wal_recovery_total":                m.walRecoveryTotal.Load(),
		"wal_recovery_records":              m.walRecoveryRecords.Load(),
		"wal_dropped_entries":               m.walDroppedEntries.Load(),
		"wal_failed_writes":                 m.walFailedWrites.Load(),
		"wal_disk_full_writes":              m.walDiskFullWrites.Load(),
		"wal_recovered_writes":              m.walRecoveredWrites.Load(),
		"wal_failed_syncs":                  m.walFailedSyncs.Load(),
		"wal_oversized_payloads":            m.walOversizedPayloads.Load(),
		"wal_directory_bytes":               m.walDirectoryBytes.Load(),
		"wal_disk_pressure":                 m.walDiskPressure.Load(),
		"wal_disk_free_bytes":               m.walDiskFreeBytes.Load(),
		"wal_disk_pressure_rejections":      m.walDiskPressureRejections.Load(),
		"wal_disk_stat_errors":              m.walDiskStatErrors.Load(),
		"file_registrations_pending":        m.fileRegistrationsPending.Load(),
		"reconciliation_storage_sweep_held": m.reconciliationStorageSweepHeld.Load(),
		"file_registrations_confirmed":      m.fileRegistrationsConfirmed.Load(),
		"file_registration_journal_errors":  m.fileRegistrationJournalErrs.Load(),
		"ingest_fenced":                     m.ingestFenced.Load(),
		"ingest_fenced_writes":              m.ingestFencedWritesTotal(),
		"wal_quarantined_files":             m.walQuarantinedFiles.Load(),
		"wal_quarantine_released_entries":   m.walQuarantineReleased.Load(),
		"wal_partial_row_recovery_pending":  m.walPartialRowRecovery.Load(),

		// Decompression Pool

		// Governance
		"governance_rate_limited_total":    m.governanceRateLimited.Load(),
		"governance_quota_exhausted_total": m.governanceQuotaExhausted.Load(),
		"governance_queries_capped_total":  m.governanceQueriesCapped.Load(),
		"governance_policies_active":       m.governancePoliciesActive.Load(),

		// Query Management
		"query_mgmt_active_queries":  m.queryMgmtActiveQueries.Load(),
		"query_mgmt_cancelled_total": m.queryMgmtCancelledTotal.Load(),
		"query_mgmt_history_size":    m.queryMgmtHistorySize.Load(),

		// Cluster FSM security (Enterprise)
		"cluster_manifest_rejected_paths_total":   m.clusterManifestRejectedPathsTotal.Load(),
		"storage_invalid_path_quarantined_total":  m.storageInvalidPathQuarantinedTotal.Load(),
		"file_time_pruning_passes_total":          m.fileTimePruningPasses.Load(),
		"file_time_pruning_expanded_total":        m.fileTimePruningExpanded.Load(),
		"file_time_pruning_files_listed_total":    m.fileTimePruningFilesListed.Load(),
		"file_time_pruning_files_dropped_total":   m.fileTimePruningFilesDropped.Load(),
		"file_time_pruning_nothing_dropped_total": m.fileTimePruningNothingDropped.Load(),
		"file_time_pruning_cap_fallback_total":    m.fileTimePruningCapFallback.Load(),
		"file_time_pruning_zero_survivors_total":  m.fileTimePruningZeroSurvivors.Load(),
		"file_time_pruning_anomalous_kept_total":  m.fileTimePruningAnomalousKept.Load(),
		"ingest_future_stamped_rows_total":        m.ingestFutureStampedRows.Load(),
		"compaction_copy_fallback_total":          m.compactionCopyFallbackTotal.Load(),
		"compaction_finished_total":               m.compactionFinishedTotal.Load(),
		"compaction_wedged_total":                 m.compactionWedgedTotal.Load(),
		"manifest_repair_dropped_total":           m.manifestRepairDroppedTotal.Load(),
		"manifest_repair_adopted_total":           m.manifestRepairAdoptedTotal.Load(),
		"manifest_repair_pending":                 m.manifestRepairPending.Load(),
		"stale_local_unlinked_total":              m.staleLocalUnlinkedTotal.Load(),
		"stale_local_tracked":                     m.staleLocalTracked.Load(),
		"stale_local_blocked":                     m.staleLocalBlocked.Load(),
		"stale_local_tracking_failed":             m.staleLocalTrackingFail.Load(),
		// Cluster local delete workers (per-node storage with replication)
		"cluster_local_delete_pending":                  m.clusterLocalDeletePending.Load(),
		"cluster_local_delete_deferred":                 m.clusterLocalDeleteDeferred.Load(),
		"compaction_manifests_parked_unparseable_total": m.compactionManifestsParkedUnparseableTotal.Load(),
		"storage_unaddressable_files":                   m.storageUnaddressableFiles.Load(),
		"backup_skipped_files":                          m.backupSkippedFiles.Load(),

		// Cluster Auth (Enterprise, Phase A)
		"cluster_heartbeats_unknown_node_total": m.clusterHeartbeatsUnknownNodeTotal.Load(),

		"cluster_auth_apply_create_total":     m.clusterAuthApplyCreateTotal.Load(),
		"cluster_auth_reconcile_total":        m.clusterAuthReconcileTotal.Load(),
		"cluster_auth_reconcile_failed_total": m.clusterAuthReconcileFailedTotal.Load(),
		"cluster_auth_reconcile_local_rows":   m.clusterAuthReconcileLocalRows.Load(),
		"cluster_auth_reconcile_collisions":   m.clusterAuthReconcileCollisions.Load(),
		"cluster_auth_reconcile_kept_rows":    m.clusterAuthReconcileKeptRows.Load(),
		"cluster_auth_apply_update_total":     m.clusterAuthApplyUpdateTotal.Load(),
		"cluster_auth_apply_revoke_total":     m.clusterAuthApplyRevokeTotal.Load(),
		"cluster_auth_apply_delete_total":     m.clusterAuthApplyDeleteTotal.Load(),
		"cluster_auth_apply_rotate_total":     m.clusterAuthApplyRotateTotal.Load(),
		"cluster_auth_rejected_total":         m.clusterAuthRejectedTotal.Load(),

		// Cluster RBAC (Enterprise, Phase A.1)
		"cluster_rbac_apply_create_organization_total":           m.clusterRBACApplyCreateOrganizationTotal.Load(),
		"cluster_rbac_apply_update_organization_total":           m.clusterRBACApplyUpdateOrganizationTotal.Load(),
		"cluster_rbac_apply_delete_organization_total":           m.clusterRBACApplyDeleteOrganizationTotal.Load(),
		"cluster_rbac_apply_create_team_total":                   m.clusterRBACApplyCreateTeamTotal.Load(),
		"cluster_rbac_apply_update_team_total":                   m.clusterRBACApplyUpdateTeamTotal.Load(),
		"cluster_rbac_apply_delete_team_total":                   m.clusterRBACApplyDeleteTeamTotal.Load(),
		"cluster_rbac_apply_create_role_total":                   m.clusterRBACApplyCreateRoleTotal.Load(),
		"cluster_rbac_apply_update_role_total":                   m.clusterRBACApplyUpdateRoleTotal.Load(),
		"cluster_rbac_apply_delete_role_total":                   m.clusterRBACApplyDeleteRoleTotal.Load(),
		"cluster_rbac_apply_create_measurement_permission_total": m.clusterRBACApplyCreateMeasurementPermissionTotal.Load(),
		"cluster_rbac_apply_delete_measurement_permission_total": m.clusterRBACApplyDeleteMeasurementPermissionTotal.Load(),
		"cluster_rbac_apply_add_token_to_team_total":             m.clusterRBACApplyAddTokenToTeamTotal.Load(),
		"cluster_rbac_apply_remove_token_from_team_total":        m.clusterRBACApplyRemoveTokenFromTeamTotal.Load(),
		"cluster_rbac_rejected_total":                            m.clusterRBACRejectedTotal.Load(),
		"cluster_rbac_cascade_rejected_total":                    m.clusterRBACCascadeRejectedTotal.Load(),
	}
	if m.edgeSyncSpokeSchedulerEnabled.Load() {
		snapshot["edge_sync_spoke_scheduler_enabled"] = int64(1)
		snapshot["edge_sync_spoke_last_success_timestamp_seconds"] = m.edgeSyncSpokeLastSuccessUnix.Load()
		snapshot["edge_sync_spoke_pass_failures_total"] = m.edgeSyncSpokePassFailures.Load()
	}
	return snapshot
}

// PrometheusFormat returns metrics in Prometheus text exposition format
func (m *Metrics) PrometheusFormat() string {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	uptimeSeconds := time.Since(m.startTime).Seconds()

	// Build Prometheus format
	var b []byte
	b = append(b, "# HELP arc_uptime_seconds Time since Arc started\n"...)
	b = append(b, "# TYPE arc_uptime_seconds gauge\n"...)
	b = appendMetric(b, "arc_uptime_seconds", uptimeSeconds)

	b = append(b, "# HELP arc_goroutines Number of goroutines\n"...)
	b = append(b, "# TYPE arc_goroutines gauge\n"...)
	b = appendMetric(b, "arc_goroutines", float64(runtime.NumGoroutine()))

	// Memory metrics
	b = append(b, "# HELP arc_memory_alloc_bytes Current allocated memory\n"...)
	b = append(b, "# TYPE arc_memory_alloc_bytes gauge\n"...)
	b = appendMetric(b, "arc_memory_alloc_bytes", float64(memStats.Alloc))

	b = append(b, "# HELP arc_memory_heap_alloc_bytes Heap memory allocated\n"...)
	b = append(b, "# TYPE arc_memory_heap_alloc_bytes gauge\n"...)
	b = appendMetric(b, "arc_memory_heap_alloc_bytes", float64(memStats.HeapAlloc))

	b = append(b, "# HELP arc_memory_sys_bytes Total memory obtained from system\n"...)
	b = append(b, "# TYPE arc_memory_sys_bytes gauge\n"...)
	b = appendMetric(b, "arc_memory_sys_bytes", float64(memStats.Sys))

	b = append(b, "# HELP arc_gc_cycles_total Total number of GC cycles\n"...)
	b = append(b, "# TYPE arc_gc_cycles_total counter\n"...)
	b = appendMetric(b, "arc_gc_cycles_total", float64(memStats.NumGC))

	// HTTP metrics
	b = append(b, "# HELP arc_http_requests_total Total HTTP requests\n"...)
	b = append(b, "# TYPE arc_http_requests_total counter\n"...)
	b = appendMetric(b, "arc_http_requests_total", float64(m.httpRequestsTotal.Load()))

	b = append(b, "# HELP arc_http_requests_success_total Successful HTTP requests\n"...)
	b = append(b, "# TYPE arc_http_requests_success_total counter\n"...)
	b = appendMetric(b, "arc_http_requests_success_total", float64(m.httpRequestsSuccess.Load()))

	b = append(b, "# HELP arc_http_requests_error_total Failed HTTP requests\n"...)
	b = append(b, "# TYPE arc_http_requests_error_total counter\n"...)
	b = appendMetric(b, "arc_http_requests_error_total", float64(m.httpRequestsError.Load()))

	// HTTP latency histogram
	b = append(b, "# HELP arc_http_latency_seconds HTTP request latency\n"...)
	b = append(b, "# TYPE arc_http_latency_seconds histogram\n"...)
	bucketLabels := []string{"0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "+Inf"}
	var cumulative int64
	for i, label := range bucketLabels {
		cumulative += m.httpLatencyBuckets[i].Load()
		b = appendMetricWithLabel(b, "arc_http_latency_seconds_bucket", "le", label, float64(cumulative))
	}
	b = appendMetric(b, "arc_http_latency_seconds_sum", float64(m.httpLatencySum.Load())/1000000.0)
	b = appendMetric(b, "arc_http_latency_seconds_count", float64(m.httpLatencyCount.Load()))

	// Ingestion metrics
	b = append(b, "# HELP arc_ingest_records_total Total records ingested\n"...)
	b = append(b, "# TYPE arc_ingest_records_total counter\n"...)
	b = appendMetric(b, "arc_ingest_records_total", float64(m.ingestRecordsTotal.Load()))

	b = append(b, "# HELP arc_ingest_bytes_total Total bytes ingested\n"...)
	b = append(b, "# TYPE arc_ingest_bytes_total counter\n"...)
	b = appendMetric(b, "arc_ingest_bytes_total", float64(m.ingestBytesTotal.Load()))

	b = append(b, "# HELP arc_ingest_batches_total Total batches ingested\n"...)
	b = append(b, "# TYPE arc_ingest_batches_total counter\n"...)
	b = appendMetric(b, "arc_ingest_batches_total", float64(m.ingestBatchesTotal.Load()))

	b = append(b, "# HELP arc_ingest_errors_total Total ingestion errors\n"...)
	b = append(b, "# TYPE arc_ingest_errors_total counter\n"...)
	b = appendMetric(b, "arc_ingest_errors_total", float64(m.ingestErrorsTotal.Load()))
	b = append(b, "# HELP arc_ingest_flush_deferred_total Flushes deferred because the flush queue was full\n"...)
	b = append(b, "# TYPE arc_ingest_flush_deferred_total counter\n"...)
	b = appendMetric(b, "arc_ingest_flush_deferred_total", float64(m.ingestFlushDeferred.Load()))
	b = append(b, "# HELP arc_buffer_deferred_buffers Buffers currently holding records that no flush worker could take\n"...)
	b = append(b, "# TYPE arc_buffer_deferred_buffers gauge\n"...)
	b = appendMetric(b, "arc_buffer_deferred_buffers", float64(m.bufferDeferredBuffers.Load()))

	// MessagePack metrics
	b = append(b, "# HELP arc_msgpack_requests_total Total MessagePack requests\n"...)
	b = append(b, "# TYPE arc_msgpack_requests_total counter\n"...)
	b = appendMetric(b, "arc_msgpack_requests_total", float64(m.msgpackRequestsTotal.Load()))

	b = append(b, "# HELP arc_msgpack_records_total Total records via MessagePack\n"...)
	b = append(b, "# TYPE arc_msgpack_records_total counter\n"...)
	b = appendMetric(b, "arc_msgpack_records_total", float64(m.msgpackRecordsTotal.Load()))

	// Line Protocol metrics
	b = append(b, "# HELP arc_lineprotocol_requests_total Total Line Protocol requests\n"...)
	b = append(b, "# TYPE arc_lineprotocol_requests_total counter\n"...)
	b = appendMetric(b, "arc_lineprotocol_requests_total", float64(m.lineprotocolRequestsTotal.Load()))

	b = append(b, "# HELP arc_lineprotocol_records_total Total records via Line Protocol\n"...)
	b = append(b, "# TYPE arc_lineprotocol_records_total counter\n"...)
	b = appendMetric(b, "arc_lineprotocol_records_total", float64(m.lineprotocolRecordsTotal.Load()))

	// Query metrics
	b = append(b, "# HELP arc_query_requests_total Total query requests\n"...)
	b = append(b, "# TYPE arc_query_requests_total counter\n"...)
	b = appendMetric(b, "arc_query_requests_total", float64(m.queryRequestsTotal.Load()))

	b = append(b, "# HELP arc_query_success_total Successful queries\n"...)
	b = append(b, "# TYPE arc_query_success_total counter\n"...)
	b = appendMetric(b, "arc_query_success_total", float64(m.querySuccessTotal.Load()))

	b = append(b, "# HELP arc_query_errors_total Failed queries\n"...)
	b = append(b, "# TYPE arc_query_errors_total counter\n"...)
	b = appendMetric(b, "arc_query_errors_total", float64(m.queryErrorsTotal.Load()))

	// Per-engine execution outcomes and latency. The label set is deliberately
	// fixed to the engines Arc can dispatch to; request data is never used here.
	b = append(b, "# HELP arc_query_engine_executions_total Completed query executions by engine and outcome\n"...)
	b = append(b, "# TYPE arc_query_engine_executions_total counter\n"...)
	engineNames := [...]string{QueryEngineDuckDB, QueryEngineArcx}
	outcomeNames := [...]string{"success", "error"}
	for engineIndex, engine := range engineNames {
		for outcomeIndex, outcome := range outcomeNames {
			b = appendMetricWithLabels(b, "arc_query_engine_executions_total",
				[]string{"engine", "outcome"}, []string{engine, outcome},
				float64(m.queryExecutions[engineIndex][outcomeIndex].Load()))
		}
	}

	b = append(b, "# HELP arc_query_latency_seconds Query execution latency by engine\n"...)
	b = append(b, "# TYPE arc_query_latency_seconds histogram\n"...)
	queryLatencyBucketLabels := [...]string{"0.01", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "30", "+Inf"}
	for engineIndex, engine := range engineNames {
		h := &m.queryEngineLatency[engineIndex]
		var cumulative int64
		for i, label := range queryLatencyBucketLabels {
			cumulative += h.buckets[i].Load()
			b = appendMetricWithLabels(b, "arc_query_latency_seconds_bucket",
				[]string{"engine", "le"}, []string{engine, label}, float64(cumulative))
		}
		b = appendMetricWithLabel(b, "arc_query_latency_seconds_sum", "engine", engine,
			float64(h.sum.Load())/1_000_000.0)
		b = appendMetricWithLabel(b, "arc_query_latency_seconds_count", "engine", engine,
			float64(h.count.Load()))
	}

	b = append(b, "# HELP arc_query_timeouts_total Queries that exceeded timeout\n"...)
	b = append(b, "# TYPE arc_query_timeouts_total counter\n"...)
	b = appendMetric(b, "arc_query_timeouts_total", float64(m.queryTimeoutsTotal.Load()))

	b = append(b, "# HELP arc_slow_queries_total Queries that exceeded slow query threshold\n"...)
	b = append(b, "# TYPE arc_slow_queries_total counter\n"...)
	b = appendMetric(b, "arc_slow_queries_total", float64(m.querySlowTotal.Load()))

	b = append(b, "# HELP arc_query_rows_total Total rows returned by queries\n"...)
	b = append(b, "# TYPE arc_query_rows_total counter\n"...)
	b = appendMetric(b, "arc_query_rows_total", float64(m.queryRowsTotal.Load()))

	// Per-handler client-disconnect counters. The `path` label
	// distinguishes Arrow IPC (grafana-arrow-datasource style),
	// Arrow-to-JSON (duckdb_arrow build serving /api/v1/query), and
	// pure database/sql JSON (default build). Operators alert on a
	// rising rate per path to correlate with RSS profiles + dashboard
	// behaviour (#426).
	b = append(b, "# HELP arc_query_client_disconnects_total Streaming queries the client closed mid-response, by handler path\n"...)
	b = append(b, "# TYPE arc_query_client_disconnects_total counter\n"...)
	b = appendMetricWithLabel(b, "arc_query_client_disconnects_total", "path", DisconnectPathArrowIPC, float64(m.queryDisconnectsArrowIPC.Load()))
	b = appendMetricWithLabel(b, "arc_query_client_disconnects_total", "path", DisconnectPathArrowJSON, float64(m.queryDisconnectsArrowJSON.Load()))
	b = appendMetricWithLabel(b, "arc_query_client_disconnects_total", "path", DisconnectPathSQLJSON, float64(m.queryDisconnectsSQLJSON.Load()))

	// Buffer metrics
	b = append(b, "# HELP arc_buffer_records_buffered Records currently buffered\n"...)
	b = append(b, "# TYPE arc_buffer_records_buffered gauge\n"...)
	b = appendMetric(b, "arc_buffer_records_buffered", float64(m.bufferRecordsBuffered.Load()))

	b = append(b, "# HELP arc_buffer_bytes_buffered Approximate bytes currently buffered or awaiting flush\n"...)
	b = append(b, "# TYPE arc_buffer_bytes_buffered gauge\n"...)
	b = appendMetric(b, "arc_buffer_bytes_buffered", float64(m.bufferBytesBuffered.Load()))

	b = append(b, "# HELP arc_buffer_oldest_unflushed_seconds Age of the oldest buffered or in-flight flush batch\n"...)
	b = append(b, "# TYPE arc_buffer_oldest_unflushed_seconds gauge\n"...)
	b = appendMetric(b, "arc_buffer_oldest_unflushed_seconds", float64(m.bufferOldestAgeNS.Load())/float64(time.Second))

	b = append(b, "# HELP arc_buffer_memory_limit_bytes Memory limit detected for the process; zero means unavailable\n"...)
	b = append(b, "# TYPE arc_buffer_memory_limit_bytes gauge\n"...)
	b = appendMetric(b, "arc_buffer_memory_limit_bytes", float64(m.bufferMemoryLimit.Load()))

	b = append(b, "# HELP arc_buffer_memory_pressure Whether buffered bytes have reached 50 percent of the process memory limit\n"...)
	b = append(b, "# TYPE arc_buffer_memory_pressure gauge\n"...)
	b = appendMetric(b, "arc_buffer_memory_pressure", float64(m.bufferMemoryPressure.Load()))

	b = append(b, "# HELP arc_buffer_records_written_total Records written to storage\n"...)
	b = append(b, "# TYPE arc_buffer_records_written_total counter\n"...)
	b = appendMetric(b, "arc_buffer_records_written_total", float64(m.bufferRecordsWritten.Load()))

	b = append(b, "# HELP arc_buffer_flushes_total Total buffer flushes\n"...)
	b = append(b, "# TYPE arc_buffer_flushes_total counter\n"...)
	b = appendMetric(b, "arc_buffer_flushes_total", float64(m.bufferFlushesTotal.Load()))

	b = append(b, "# HELP arc_buffer_flush_failures_total Total buffer flush failures preserved for WAL recovery\n"...)
	b = append(b, "# TYPE arc_buffer_flush_failures_total counter\n"...)
	b = appendMetric(b, "arc_buffer_flush_failures_total", float64(m.bufferFlushFailures.Load()))

	b = append(b, "# HELP arc_buffer_queue_depth Current flush queue depth\n"...)
	b = append(b, "# TYPE arc_buffer_queue_depth gauge\n"...)
	b = appendMetric(b, "arc_buffer_queue_depth", float64(m.bufferQueueDepth.Load()))

	b = append(b, "# HELP arc_buffer_records_queued Records waiting in the flush queue\n"...)
	b = append(b, "# TYPE arc_buffer_records_queued gauge\n"...)
	b = appendMetric(b, "arc_buffer_records_queued", float64(m.bufferRecordsQueued.Load()))

	b = append(b, "# HELP arc_buffer_records_inflight Records currently being flushed\n"...)
	b = append(b, "# TYPE arc_buffer_records_inflight gauge\n"...)
	b = appendMetric(b, "arc_buffer_records_inflight", float64(m.bufferRecordsInflight.Load()))

	// Storage metrics
	b = append(b, "# HELP arc_storage_writes_total Total storage writes\n"...)
	b = append(b, "# TYPE arc_storage_writes_total counter\n"...)
	b = appendMetric(b, "arc_storage_writes_total", float64(m.storageWritesTotal.Load()))

	b = append(b, "# HELP arc_storage_write_bytes_total Total bytes written to storage\n"...)
	b = append(b, "# TYPE arc_storage_write_bytes_total counter\n"...)
	b = appendMetric(b, "arc_storage_write_bytes_total", float64(m.storageWriteBytesTotal.Load()))

	b = append(b, "# HELP arc_storage_reads_total Total storage reads\n"...)
	b = append(b, "# TYPE arc_storage_reads_total counter\n"...)
	b = appendMetric(b, "arc_storage_reads_total", float64(m.storageReadsTotal.Load()))

	b = append(b, "# HELP arc_storage_read_bytes_total Total bytes read from storage\n"...)
	b = append(b, "# TYPE arc_storage_read_bytes_total counter\n"...)
	b = appendMetric(b, "arc_storage_read_bytes_total", float64(m.storageReadBytesTotal.Load()))

	b = append(b, "# HELP arc_storage_errors_total Total storage errors\n"...)
	b = append(b, "# TYPE arc_storage_errors_total counter\n"...)
	b = appendMetric(b, "arc_storage_errors_total", float64(m.storageErrorsTotal.Load()))

	// Compaction metrics
	// Compaction job counters count ATTEMPTS (a batch retried at half size is
	// two), counted by the parent from each subprocess's result (#1343).
	b = append(b, "# HELP arc_compaction_jobs_total Compaction job attempts that ran (succeeded, failed or interrupted)\n"...)
	b = append(b, "# TYPE arc_compaction_jobs_total counter\n"...)
	b = appendMetric(b, "arc_compaction_jobs_total", float64(m.compactionJobsTotal.Load()))

	b = append(b, "# HELP arc_compaction_jobs_success_total Compaction job attempts that succeeded (a staged job: its output staged, before the cluster commits it)\n"...)
	b = append(b, "# TYPE arc_compaction_jobs_success_total counter\n"...)
	b = appendMetric(b, "arc_compaction_jobs_success_total", float64(m.compactionJobsSuccess.Load()))

	b = append(b, "# HELP arc_compaction_jobs_failed_total Compaction job attempts that failed, including a subprocess that crashed or was killed\n"...)
	b = append(b, "# TYPE arc_compaction_jobs_failed_total counter\n"...)
	b = appendMetric(b, "arc_compaction_jobs_failed_total", float64(m.compactionJobsFailed.Load()))

	b = append(b, "# HELP arc_compaction_jobs_interrupted_total Compaction job attempts cut short by shutdown or a cancelled cycle\n"...)
	b = append(b, "# TYPE arc_compaction_jobs_interrupted_total counter\n"...)
	b = appendMetric(b, "arc_compaction_jobs_interrupted_total", float64(m.compactionJobsInterrupted.Load()))

	b = append(b, "# HELP arc_compaction_partitions_skipped_too_wide_total Compaction candidates left alone because the partition failed at the smallest batch and is remembered as too wide to compact\n"...)
	b = append(b, "# TYPE arc_compaction_partitions_skipped_too_wide_total counter\n"...)
	b = appendMetric(b, "arc_compaction_partitions_skipped_too_wide_total", float64(m.compactionPartitionsSkippedTooWide.Load()))

	b = append(b, "# HELP arc_compaction_partitions_too_wide Partitions remembered as too wide to compact in a job's memory\n"...)
	b = append(b, "# TYPE arc_compaction_partitions_too_wide gauge\n"...)
	b = appendMetric(b, "arc_compaction_partitions_too_wide", float64(m.compactionPartitionsTooWide.Load()))

	b = append(b, "# HELP arc_compaction_files_compacted_total Input files compacted by successful compaction attempts (a staged job: before the cluster commits it)\n"...)
	b = append(b, "# TYPE arc_compaction_files_compacted_total counter\n"...)
	b = appendMetric(b, "arc_compaction_files_compacted_total", float64(m.compactionFilesCompacted.Load()))

	b = append(b, "# HELP arc_compaction_bytes_read_total Bytes of input files of successful compaction attempts\n"...)
	b = append(b, "# TYPE arc_compaction_bytes_read_total counter\n"...)
	b = appendMetric(b, "arc_compaction_bytes_read_total", float64(m.compactionBytesRead.Load()))

	b = append(b, "# HELP arc_compaction_bytes_written_total Bytes of output files of successful compaction attempts\n"...)
	b = append(b, "# TYPE arc_compaction_bytes_written_total counter\n"...)
	b = appendMetric(b, "arc_compaction_bytes_written_total", float64(m.compactionBytesWritten.Load()))

	b = append(b, "# HELP arc_compaction_manifests_recovered_total Compaction manifests recovered after crash\n"...)
	b = append(b, "# TYPE arc_compaction_manifests_recovered_total counter\n"...)
	b = appendMetric(b, "arc_compaction_manifests_recovered_total", float64(m.compactionManifestsRecovered.Load()))

	// Scheduled spoke metrics are absent on disabled and bundle-only nodes.
	if m.edgeSyncSpokeSchedulerEnabled.Load() {
		b = append(b, "# HELP arc_edgesync_spoke_scheduler_enabled Whether automatic network spoke sync is enabled\n"...)
		b = append(b, "# TYPE arc_edgesync_spoke_scheduler_enabled gauge\n"...)
		b = appendMetric(b, "arc_edgesync_spoke_scheduler_enabled", 1)

		b = append(b, "# HELP arc_edgesync_spoke_last_success_timestamp_seconds Unix timestamp of the last successfully completed scheduled pass, zero if none\n"...)
		b = append(b, "# TYPE arc_edgesync_spoke_last_success_timestamp_seconds gauge\n"...)
		b = appendMetric(b, "arc_edgesync_spoke_last_success_timestamp_seconds", float64(m.edgeSyncSpokeLastSuccessUnix.Load()))

		b = append(b, "# HELP arc_edgesync_spoke_pass_failures_total Failed scheduled network sync passes\n"...)
		b = append(b, "# TYPE arc_edgesync_spoke_pass_failures_total counter\n"...)
		b = appendMetric(b, "arc_edgesync_spoke_pass_failures_total", float64(m.edgeSyncSpokePassFailures.Load()))
	}

	// Auth metrics
	b = append(b, "# HELP arc_auth_requests_total Total authentication requests\n"...)
	b = append(b, "# TYPE arc_auth_requests_total counter\n"...)
	b = appendMetric(b, "arc_auth_requests_total", float64(m.authRequestsTotal.Load()))

	b = append(b, "# HELP arc_auth_cache_hits_total Auth cache hits\n"...)
	b = append(b, "# TYPE arc_auth_cache_hits_total counter\n"...)
	b = appendMetric(b, "arc_auth_cache_hits_total", float64(m.authCacheHits.Load()))

	b = append(b, "# HELP arc_auth_cache_misses_total Auth cache misses\n"...)
	b = append(b, "# TYPE arc_auth_cache_misses_total counter\n"...)
	b = appendMetric(b, "arc_auth_cache_misses_total", float64(m.authCacheMisses.Load()))

	// Database metrics (DuckDB connection pool, sampled at scrape time)
	b = append(b, "# HELP arc_db_connections_max Maximum open connections the pool allows\n"...)
	b = append(b, "# TYPE arc_db_connections_max gauge\n"...)
	b = appendMetric(b, "arc_db_connections_max", float64(m.dbConnectionsMax.Load()))

	b = append(b, "# HELP arc_db_connections_open Open database connections, in use plus idle\n"...)
	b = append(b, "# TYPE arc_db_connections_open gauge\n"...)
	b = appendMetric(b, "arc_db_connections_open", float64(m.dbConnectionsOpen.Load()))

	b = append(b, "# HELP arc_db_connections_in_use Database connections currently in use\n"...)
	b = append(b, "# TYPE arc_db_connections_in_use gauge\n"...)
	b = appendMetric(b, "arc_db_connections_in_use", float64(m.dbConnectionsInUse.Load()))

	b = append(b, "# HELP arc_db_connections_idle Database connections currently idle\n"...)
	b = append(b, "# TYPE arc_db_connections_idle gauge\n"...)
	b = appendMetric(b, "arc_db_connections_idle", float64(m.dbConnectionsIdle.Load()))

	// Pool saturation. These, not the in-use/open ratio, are the signal that
	// the pool is too small: a pool at max with zero waits is simply busy,
	// while sustained wait growth means queries are blocking on a connection.
	b = append(b, "# HELP arc_db_wait_count_total Cumulative number of times a query waited for a connection\n"...)
	b = append(b, "# TYPE arc_db_wait_count_total counter\n"...)
	b = appendMetric(b, "arc_db_wait_count_total", float64(m.dbWaitCount.Load()))

	b = append(b, "# HELP arc_db_wait_seconds_total Cumulative time queries spent blocked waiting for a connection\n"...)
	b = append(b, "# TYPE arc_db_wait_seconds_total counter\n"...)
	b = appendMetric(b, "arc_db_wait_seconds_total", float64(m.dbWaitMicros.Load())/1000000.0)

	// Audit metrics
	b = append(b, "# HELP arc_audit_events_total Total audit log events\n"...)
	b = append(b, "# TYPE arc_audit_events_total counter\n"...)
	b = appendMetric(b, "arc_audit_events_total", float64(m.auditEventsTotal.Load()))

	b = append(b, "# HELP arc_audit_write_errors_total Total audit log events that failed to persist\n"...)
	b = append(b, "# TYPE arc_audit_write_errors_total counter\n"...)
	b = appendMetric(b, "arc_audit_write_errors_total", float64(m.auditWriteErrors.Load()))

	b = append(b, "# HELP arc_audit_events_dropped_total Audit events discarded before queueing because the event channel was full\n"...)
	b = append(b, "# TYPE arc_audit_events_dropped_total counter\n"...)
	b = appendMetric(b, "arc_audit_events_dropped_total", float64(m.auditEventsDropped.Load()))

	// MQTT metrics
	b = append(b, "# HELP arc_mqtt_messages_received_total Total MQTT messages received\n"...)
	b = append(b, "# TYPE arc_mqtt_messages_received_total counter\n"...)
	b = appendMetric(b, "arc_mqtt_messages_received_total", float64(m.mqttMessagesReceived.Load()))

	b = append(b, "# HELP arc_mqtt_messages_failed_total Total MQTT messages that failed processing\n"...)
	b = append(b, "# TYPE arc_mqtt_messages_failed_total counter\n"...)
	b = appendMetric(b, "arc_mqtt_messages_failed_total", float64(m.mqttMessagesFailed.Load()))

	b = append(b, "# HELP arc_mqtt_bytes_received_total Total bytes received via MQTT\n"...)
	b = append(b, "# TYPE arc_mqtt_bytes_received_total counter\n"...)
	b = appendMetric(b, "arc_mqtt_bytes_received_total", float64(m.mqttBytesReceived.Load()))

	b = append(b, "# HELP arc_mqtt_decode_success_total Successful MQTT message decodes\n"...)
	b = append(b, "# TYPE arc_mqtt_decode_success_total counter\n"...)
	b = appendMetric(b, "arc_mqtt_decode_success_total", float64(m.mqttDecodeSuccess.Load()))

	b = append(b, "# HELP arc_mqtt_decode_errors_total MQTT message decode errors\n"...)
	b = append(b, "# TYPE arc_mqtt_decode_errors_total counter\n"...)
	b = appendMetric(b, "arc_mqtt_decode_errors_total", float64(m.mqttDecodeErrors.Load()))

	b = append(b, "# HELP arc_mqtt_connected MQTT connection status (1=connected, 0=disconnected)\n"...)
	b = append(b, "# TYPE arc_mqtt_connected gauge\n"...)
	b = appendMetric(b, "arc_mqtt_connected", float64(m.mqttConnected.Load()))

	b = append(b, "# HELP arc_mqtt_reconnects_total Total MQTT reconnection attempts\n"...)
	b = append(b, "# TYPE arc_mqtt_reconnects_total counter\n"...)
	b = appendMetric(b, "arc_mqtt_reconnects_total", float64(m.mqttReconnects.Load()))

	// WAL metrics
	b = append(b, "# HELP arc_wal_records_preserved_total Records preserved in WAL for recovery\n"...)
	b = append(b, "# TYPE arc_wal_records_preserved_total counter\n"...)
	b = appendMetric(b, "arc_wal_records_preserved_total", float64(m.walRecordsPreserved.Load()))

	b = append(b, "# HELP arc_wal_registered_floor_lag Committed ingest batches not yet below the registered floor of their key (WAL coverage only)\n"...)
	b = append(b, "# TYPE arc_wal_registered_floor_lag gauge\n"...)
	b = appendMetric(b, "arc_wal_registered_floor_lag", float64(m.walRegisteredFloorLag.Load()))
	b = appendMetric(b, "arc_wal_held_files", float64(m.walHeldFiles.Load()))

	b = append(b, "# HELP arc_wal_parked_files Refused file registrations parked out of the query path until their rows are registered elsewhere\n"...)
	b = append(b, "# TYPE arc_wal_parked_files gauge\n"...)
	b = appendMetric(b, "arc_wal_parked_files", float64(m.walParkedFiles.Load()))

	b = append(b, "# HELP arc_wal_parked_bytes Bytes of parked refused files\n"...)
	b = append(b, "# TYPE arc_wal_parked_bytes gauge\n"...)
	b = appendMetric(b, "arc_wal_parked_bytes", float64(m.walParkedBytes.Load()))

	b = append(b, "# HELP arc_wal_coverage_lost_total Ingest batches that settled without being in a registered file (WAL coverage only)\n"...)
	b = append(b, "# TYPE arc_wal_coverage_lost_total counter\n"...)
	b = appendMetric(b, "arc_wal_coverage_lost_total", float64(m.walCoverageLost.Load()))

	b = append(b, "# HELP arc_origin_stream_sessions Standby writers this primary is streaming committed batches to\n"...)
	b = append(b, "# TYPE arc_origin_stream_sessions gauge\n"...)
	b = appendMetric(b, "arc_origin_stream_sessions", float64(m.originStreamSessions.Load()))

	b = append(b, "# HELP arc_origin_stream_dropped_total Batches not sent to a standby because its send queue was full\n"...)
	b = append(b, "# TYPE arc_origin_stream_dropped_total counter\n"...)
	b = appendMetric(b, "arc_origin_stream_dropped_total", float64(m.originStreamDropped.Load()))

	b = append(b, "# HELP arc_standby_stream_connected 1 while this standby writer receives from its primary\n"...)
	b = append(b, "# TYPE arc_standby_stream_connected gauge\n"...)
	b = appendMetric(b, "arc_standby_stream_connected", float64(m.standbyStreamConnected.Load()))

	b = append(b, "# HELP arc_standby_stream_entries_total Batches stored by this standby writer\n"...)
	b = append(b, "# TYPE arc_standby_stream_entries_total counter\n"...)
	b = appendMetric(b, "arc_standby_stream_entries_total", float64(m.standbyStreamEntries.Load()))

	b = append(b, "# HELP arc_standby_stream_gaps_total Gaps found in the batches this standby writer holds\n"...)
	b = append(b, "# TYPE arc_standby_stream_gaps_total counter\n"...)
	b = appendMetric(b, "arc_standby_stream_gaps_total", float64(m.standbyStreamGaps.Load()))

	b = append(b, "# HELP arc_standby_store_bytes Bytes held in the standby store\n"...)
	b = append(b, "# TYPE arc_standby_store_bytes gauge\n"...)
	b = appendMetric(b, "arc_standby_store_bytes", float64(m.standbyStoreBytes.Load()))

	b = append(b, "# HELP arc_standby_store_incomplete_keys Measurements with a gap in the standby store above the registered floor\n"...)
	b = append(b, "# TYPE arc_standby_store_incomplete_keys gauge\n"...)
	b = appendMetric(b, "arc_standby_store_incomplete_keys", float64(m.standbyStoreIncomplete.Load()))

	b = append(b, "# HELP arc_standby_store_trimmed_bytes_total Bytes trimmed from the standby store\n"...)
	b = append(b, "# TYPE arc_standby_store_trimmed_bytes_total counter\n"...)
	b = appendMetric(b, "arc_standby_store_trimmed_bytes_total", float64(m.standbyStoreTrimmedBytes.Load()))

	b = append(b, "# HELP arc_standby_store_dropped_total Batches not stored because the standby store was at its cap\n"...)
	b = append(b, "# TYPE arc_standby_store_dropped_total counter\n"...)
	b = appendMetric(b, "arc_standby_store_dropped_total", float64(m.standbyStoreDropped.Load()))

	b = append(b, "# HELP arc_standby_store_evicted_bytes_total Bytes of replaced primary instances evicted from the standby store at its cap\n"...)
	b = append(b, "# TYPE arc_standby_store_evicted_bytes_total counter\n"...)
	b = appendMetric(b, "arc_standby_store_evicted_bytes_total", float64(m.standbyStoreEvicted.Load()))

	b = append(b, "# HELP arc_standby_store_lost_records Batches held by this standby that the primary reported lost (kept: possibly the only copy)\n"...)
	b = append(b, "# TYPE arc_standby_store_lost_records gauge\n"...)
	b = appendMetric(b, "arc_standby_store_lost_records", float64(m.standbyStoreLost.Load()))

	b = append(b, "# HELP arc_standby_store_blocked_keys Measurements with registered batches this standby may not trim yet (a file holding them not here, its FSM behind, a claim, or an origin not heard from)\n"...)
	b = append(b, "# TYPE arc_standby_store_blocked_keys gauge\n"...)
	b = appendMetric(b, "arc_standby_store_blocked_keys", float64(m.standbyStoreBlocked.Load()))

	b = append(b, "# HELP arc_standby_store_replaced_bytes Bytes held for primary instances that have since restarted\n"...)
	b = append(b, "# TYPE arc_standby_store_replaced_bytes gauge\n"...)
	b = appendMetric(b, "arc_standby_store_replaced_bytes", float64(m.standbyStoreReplaced.Load()))
	b = append(b, "# HELP arc_standby_store_pinned_bytes Bytes of the standby store kept for a possible stream takeover; not evicted at the cap\n"...)
	b = append(b, "# TYPE arc_standby_store_pinned_bytes gauge\n"...)
	b = appendMetric(b, "arc_standby_store_pinned_bytes", float64(m.standbyStorePinned.Load()))

	b = append(b, "# HELP arc_standby_store_capped 1 while the standby store refuses batches at its cap (cluster.standby_store_max_bytes)\n"...)
	b = append(b, "# TYPE arc_standby_store_capped gauge\n"...)
	b = appendMetric(b, "arc_standby_store_capped", float64(m.standbyStoreCapped.Load()))

	b = append(b, "# HELP arc_standby_store_low_space 1 while the standby store's volume is below its free-space reserve (batches not kept)\n"...)
	b = append(b, "# TYPE arc_standby_store_low_space gauge\n"...)
	b = appendMetric(b, "arc_standby_store_low_space", float64(m.standbyStoreLowSpace.Load()))

	b = append(b, "# HELP arc_standby_store_max_bytes The standby store's effective cap in bytes (0 = no cap)\n"...)
	b = append(b, "# TYPE arc_standby_store_max_bytes gauge\n"...)
	b = appendMetric(b, "arc_standby_store_max_bytes", float64(m.standbyStoreMaxBytes.Load()))

	b = append(b, "# HELP arc_cluster_stream_claims_enabled 1 once stream claims are enabled in the cluster\n"...)
	b = append(b, "# TYPE arc_cluster_stream_claims_enabled gauge\n"...)
	b = appendMetric(b, "arc_cluster_stream_claims_enabled", float64(m.streamClaimsEnabled.Load()))

	b = append(b, "# HELP arc_cluster_stream_claims Stream claims held: primary instances a standby writer took over\n"...)
	b = append(b, "# TYPE arc_cluster_stream_claims gauge\n"...)
	b = appendMetric(b, "arc_cluster_stream_claims", float64(m.streamClaims.Load()))
	b = append(b, "# HELP arc_stream_takeover_departed_writers Writers that left the cluster gracefully and have not re-joined; their takeover state expires after cluster.stream_takeover_departed_ttl\n"...)
	b = append(b, "# TYPE arc_stream_takeover_departed_writers gauge\n"...)
	b = appendMetric(b, "arc_stream_takeover_departed_writers", float64(m.departedWriters.Load()))
	b = append(b, "# HELP arc_stream_takeover_oldest_claim_age_seconds How long the oldest unfinished stream claim has been open (0: none); a claim that does not finish keeps DELETEs of its measurements waiting\n"...)
	b = append(b, "# TYPE arc_stream_takeover_oldest_claim_age_seconds gauge\n"...)
	b = appendMetric(b, "arc_stream_takeover_oldest_claim_age_seconds", float64(m.oldestClaimAge.Load()))
	b = append(b, "# HELP arc_stream_takeover_unpulled_files Registered files of the writer instances this node is taking over that it does not hold yet; a claim finishes only once it holds them, or they are found gone from every node\n"...)
	b = append(b, "# TYPE arc_stream_takeover_unpulled_files gauge\n"...)
	b = appendMetric(b, "arc_stream_takeover_unpulled_files", float64(m.unpulledFiles.Load()))

	b = append(b, "# HELP arc_wal_coverage_refused_total Batches whose file the cluster refused because another writer claimed them\n"...)
	b = append(b, "# TYPE arc_wal_coverage_refused_total counter\n"...)
	b = appendMetric(b, "arc_wal_coverage_refused_total", float64(m.walCoverageRefused.Load()))

	b = append(b, "# HELP arc_wal_recovery_total Successful WAL recovery operations\n"...)
	b = append(b, "# TYPE arc_wal_recovery_total counter\n"...)
	b = appendMetric(b, "arc_wal_recovery_total", float64(m.walRecoveryTotal.Load()))

	b = append(b, "# HELP arc_wal_recovery_records_total Total records recovered from WAL\n"...)
	b = append(b, "# TYPE arc_wal_recovery_records_total counter\n"...)
	b = appendMetric(b, "arc_wal_recovery_records_total", float64(m.walRecoveryRecords.Load()))

	b = append(b, "# HELP arc_wal_dropped_entries_total WAL entries dropped due to full buffer\n"...)
	b = append(b, "# TYPE arc_wal_dropped_entries_total counter\n"...)
	b = appendMetric(b, "arc_wal_dropped_entries_total", float64(m.walDroppedEntries.Load()))

	b = append(b, "# HELP arc_wal_failed_writes_total WAL entries lost to a failed write (not written to any WAL file)\n"...)
	b = append(b, "# TYPE arc_wal_failed_writes_total counter\n"...)
	b = appendMetric(b, "arc_wal_failed_writes_total", float64(m.walFailedWrites.Load()))
	b = append(b, "# HELP arc_wal_disk_full_writes_total WAL entries lost to a write that failed with ENOSPC or EDQUOT (a subset of arc_wal_failed_writes_total)\n"...)
	b = append(b, "# TYPE arc_wal_disk_full_writes_total counter\n"...)
	b = appendMetric(b, "arc_wal_disk_full_writes_total", float64(m.walDiskFullWrites.Load()))
	b = append(b, "# HELP arc_wal_recovered_writes_total Failed WAL writes that succeeded on a new WAL file; nothing was lost\n"...)
	b = append(b, "# TYPE arc_wal_recovered_writes_total counter\n"...)
	b = appendMetric(b, "arc_wal_recovered_writes_total", float64(m.walRecoveredWrites.Load()))
	b = append(b, "# HELP arc_wal_failed_syncs_total WAL fsync/fdatasync failures; each forces a rotation to a new WAL file\n"...)
	b = append(b, "# TYPE arc_wal_failed_syncs_total counter\n"...)
	b = appendMetric(b, "arc_wal_failed_syncs_total", float64(m.walFailedSyncs.Load()))

	b = append(b, "# HELP arc_wal_oversized_payloads_total Payloads rejected for exceeding the single-entry cap even after chunking\n"...)
	b = append(b, "# TYPE arc_wal_oversized_payloads_total counter\n"...)
	b = appendMetric(b, "arc_wal_oversized_payloads_total", float64(m.walOversizedPayloads.Load()))

	b = append(b, "# HELP arc_wal_dir_bytes Current bytes occupied by every file in the WAL directory\n"...)
	b = append(b, "# TYPE arc_wal_dir_bytes gauge\n"...)
	b = appendMetric(b, "arc_wal_dir_bytes", float64(m.walDirectoryBytes.Load()))

	b = append(b, "# HELP arc_wal_disk_pressure 1 while live writes are refused (503) because the WAL volume is low on free space or the WAL directory is at wal.max_directory_mb\n"...)
	b = append(b, "# TYPE arc_wal_disk_pressure gauge\n"...)
	b = appendMetric(b, "arc_wal_disk_pressure", float64(m.walDiskPressure.Load()))
	b = append(b, "# HELP arc_wal_disk_free_bytes Bytes available on the WAL volume at the last sample\n"...)
	b = append(b, "# TYPE arc_wal_disk_free_bytes gauge\n"...)
	b = appendMetric(b, "arc_wal_disk_free_bytes", float64(m.walDiskFreeBytes.Load()))
	b = append(b, "# HELP arc_wal_disk_pressure_rejections_total Live writes refused for WAL volume disk pressure; nothing was written, clients retry\n"...)
	b = append(b, "# TYPE arc_wal_disk_pressure_rejections_total counter\n"...)
	b = appendMetric(b, "arc_wal_disk_pressure_rejections_total", float64(m.walDiskPressureRejections.Load()))
	b = append(b, "# HELP arc_wal_disk_stat_errors_total Failed reads of the WAL volume's free space; the disk pressure gate fails open while they fail\n"...)
	b = append(b, "# TYPE arc_wal_disk_stat_errors_total counter\n"...)
	b = appendMetric(b, "arc_wal_disk_stat_errors_total", float64(m.walDiskStatErrors.Load()))

	b = append(b, "# HELP arc_reconciliation_storage_sweep_held 1 while the last reconciliation run held its orphan-storage sweep because a node that can ingest did not answer, or a published pending-registration list could not be read; a node that stays down holds it until it returns or is removed from the cluster\n"...)
	b = append(b, "# TYPE arc_reconciliation_storage_sweep_held gauge\n"...)
	b = appendMetric(b, "arc_reconciliation_storage_sweep_held", float64(m.reconciliationStorageSweepHeld.Load()))

	b = append(b, "# HELP arc_cluster_file_registrations_pending Flushed files not yet confirmed in the cluster manifest\n"...)
	b = append(b, "# TYPE arc_cluster_file_registrations_pending gauge\n"...)
	b = appendMetric(b, "arc_cluster_file_registrations_pending", float64(m.fileRegistrationsPending.Load()))

	b = append(b, "# HELP arc_cluster_file_registration_oldest_pending_seconds Age of the oldest flushed file not yet confirmed in the cluster manifest\n"...)
	b = append(b, "# TYPE arc_cluster_file_registration_oldest_pending_seconds gauge\n"...)
	b = appendMetric(b, "arc_cluster_file_registration_oldest_pending_seconds", m.fileRegistrationOldestPendingSeconds())

	b = append(b, "# HELP arc_cluster_file_registrations_confirmed_total Flushed files the registration outbox confirmed in the cluster manifest\n"...)
	b = append(b, "# TYPE arc_cluster_file_registrations_confirmed_total counter\n"...)
	b = appendMetric(b, "arc_cluster_file_registrations_confirmed_total", float64(m.fileRegistrationsConfirmed.Load()))

	b = append(b, "# HELP arc_cluster_ingest_fenced 1 while this writer refuses writes because it may no longer be the primary\n"...)
	b = append(b, "# TYPE arc_cluster_ingest_fenced gauge\n"...)
	b = appendMetric(b, "arc_cluster_ingest_fenced", float64(m.ingestFenced.Load()))

	b = append(b, "# HELP arc_cluster_ingest_fenced_writes_total Live writes refused because this node may not accept ingest, by reason (gate refusals: a retried MQTT message or CQ write counts each attempt)\n"...)
	b = append(b, "# TYPE arc_cluster_ingest_fenced_writes_total counter\n"...)
	for i, r := range IngestFencedReasons {
		b = appendMetricWithLabel(b, "arc_cluster_ingest_fenced_writes_total", "reason", r, float64(m.ingestFencedWrites[i].Load()))
	}

	b = append(b, "# HELP arc_cluster_writes_forwarded_total Writes this node forwarded to a writer (a reader or compactor, or a standby writer forwarding to the primary), by result\n"...)
	b = append(b, "# TYPE arc_cluster_writes_forwarded_total counter\n"...)
	for i, r := range WriteForwardResults {
		b = appendMetricWithLabel(b, "arc_cluster_writes_forwarded_total", "result", r, float64(m.writesForwarded[i].Load()))
	}

	b = append(b, "# HELP arc_cluster_file_registration_journal_errors_total File registration journal write or fsync failures\n"...)
	b = append(b, "# TYPE arc_cluster_file_registration_journal_errors_total counter\n"...)
	b = appendMetric(b, "arc_cluster_file_registration_journal_errors_total", float64(m.fileRegistrationJournalErrs.Load()))

	b = append(b, "# HELP arc_wal_quarantined_files_total WAL files isolated after repeated recovery failures\n"...)
	b = append(b, "# TYPE arc_wal_quarantined_files_total counter\n"...)
	b = appendMetric(b, "arc_wal_quarantined_files_total", float64(m.walQuarantinedFiles.Load()))

	b = append(b, "# HELP arc_wal_quarantine_released_entries_total Pending WAL entries the writer stopped waiting for because their file was quarantined\n"...)
	b = append(b, "# TYPE arc_wal_quarantine_released_entries_total counter\n"...)
	b = appendMetric(b, "arc_wal_quarantine_released_entries_total", float64(m.walQuarantineReleased.Load()))

	b = append(b, "# HELP arc_wal_partial_row_recovery_pending 1 when the last WAL recovery pass left a retained file whose parent entry had row ranges replayed\n"...)
	b = append(b, "# TYPE arc_wal_partial_row_recovery_pending gauge\n"...)
	b = appendMetric(b, "arc_wal_partial_row_recovery_pending", float64(m.walPartialRowRecovery.Load()))

	// Governance metrics
	b = append(b, "# HELP arc_governance_rate_limited_total Queries rejected by rate limiting\n"...)
	b = append(b, "# TYPE arc_governance_rate_limited_total counter\n"...)
	b = appendMetric(b, "arc_governance_rate_limited_total", float64(m.governanceRateLimited.Load()))

	b = append(b, "# HELP arc_governance_quota_exhausted_total Queries rejected by quota exhaustion\n"...)
	b = append(b, "# TYPE arc_governance_quota_exhausted_total counter\n"...)
	b = appendMetric(b, "arc_governance_quota_exhausted_total", float64(m.governanceQuotaExhausted.Load()))

	b = append(b, "# HELP arc_governance_queries_capped_total Queries whose results reached a policy row cap and may be incomplete\n"...)
	b = append(b, "# TYPE arc_governance_queries_capped_total counter\n"...)
	b = appendMetric(b, "arc_governance_queries_capped_total", float64(m.governanceQueriesCapped.Load()))

	b = append(b, "# HELP arc_governance_policies_active Number of active governance policies\n"...)
	b = append(b, "# TYPE arc_governance_policies_active gauge\n"...)
	b = appendMetric(b, "arc_governance_policies_active", float64(m.governancePoliciesActive.Load()))

	// Query Management metrics
	b = append(b, "# HELP arc_query_mgmt_active_queries Currently running tracked queries\n"...)
	b = append(b, "# TYPE arc_query_mgmt_active_queries gauge\n"...)
	b = appendMetric(b, "arc_query_mgmt_active_queries", float64(m.queryMgmtActiveQueries.Load()))

	b = append(b, "# HELP arc_query_mgmt_cancelled_total Total queries cancelled via management API\n"...)
	b = append(b, "# TYPE arc_query_mgmt_cancelled_total counter\n"...)
	b = appendMetric(b, "arc_query_mgmt_cancelled_total", float64(m.queryMgmtCancelledTotal.Load()))

	b = append(b, "# HELP arc_query_mgmt_history_size Completed queries in history buffer\n"...)
	b = append(b, "# TYPE arc_query_mgmt_history_size gauge\n"...)
	b = appendMetric(b, "arc_query_mgmt_history_size", float64(m.queryMgmtHistorySize.Load()))

	// Cluster FSM security metrics (Enterprise — see GHSA-f85q-mvg8-qf37)
	b = append(b, "# HELP arc_cluster_manifest_rejected_paths_total Total manifest path proposals refused by the cluster FSM. Non-zero growth indicates a peer/snapshot/log entry proposed a path validation refused (URL scheme, absolute, parent-traversal, NUL, oversize).\n"...)
	b = append(b, "# TYPE arc_cluster_manifest_rejected_paths_total counter\n"...)
	b = appendMetric(b, "arc_cluster_manifest_rejected_paths_total", float64(m.clusterManifestRejectedPathsTotal.Load()))

	b = append(b, "# HELP arc_storage_invalid_path_quarantined_total Total entries dropped from a cleanup, reconciliation or replication work set because a storage key was permanently unusable. Non-zero growth means a stored key (compaction manifest, cluster manifest entry, edge-sync ledger row) names something no storage backend can address; the entry is no longer retried and needs operator action.\n"...)
	b = append(b, "# TYPE arc_storage_invalid_path_quarantined_total counter\n"...)
	b = appendMetric(b, "arc_storage_invalid_path_quarantined_total", float64(m.storageInvalidPathQuarantinedTotal.Load()))

	for _, c := range []struct {
		name, help string
		v          int64
	}{
		{"arc_query_file_time_pruning_passes_total", "Table reads whose range starts in the current hour that file-level time pruning listed (query.file_time_pruning); each pass ends as one of expanded, nothing_dropped, cap_fallback, or zero_survivors with a single path.", m.fileTimePruningPasses.Load()},
		{"arc_query_file_time_pruning_expanded_total", "File-time pruning passes that left files out of the live-hour read.", m.fileTimePruningExpanded.Load()},
		{"arc_query_file_time_pruning_files_listed_total", "Live-hour files listed by file-time pruning passes.", m.fileTimePruningFilesListed.Load()},
		{"arc_query_file_time_pruning_files_dropped_total", "Live-hour files left out of reads by file-time pruning.", m.fileTimePruningFilesDropped.Load()},
		{"arc_query_file_time_pruning_nothing_dropped_total", "File-time pruning passes where every live-hour file was in range (the margin covers the files listed).", m.fileTimePruningNothingDropped.Load()},
		{"arc_query_file_time_pruning_cap_fallback_total", "File-time pruning passes that kept too many files to list and read the whole hour.", m.fileTimePruningCapFallback.Load()},
		{"arc_query_file_time_pruning_zero_survivors_total", "File-time pruning passes that kept no live-hour file: with the hour as the only path the glob is kept (fail safe); with other paths the hour is dropped and the pass also counts as expanded.", m.fileTimePruningZeroSurvivors.Load()},
		{"arc_query_file_time_pruning_anomalous_kept_total", "Live-hour files kept because their name time is before their hour (rows stamped ahead of the flushing node's clock).", m.fileTimePruningAnomalousKept.Load()},
		{"arc_ingest_future_stamped_rows_total", "Rows stamped more than query.file_time_pruning_margin_seconds past their flush, in the flush's hour: with file-level time pruning on, a query can miss them until the hour closes.", m.ingestFutureStampedRows.Load()},
	} {
		b = append(b, "# HELP "+c.name+" "+c.help+"\n"...)
		b = append(b, "# TYPE "+c.name+" counter\n"...)
		b = appendMetric(b, c.name, float64(c.v))
	}
	b = appendMetric(b, "arc_compaction_copy_fallback_total", float64(m.compactionCopyFallbackTotal.Load()))
	b = appendMetric(b, "arc_compaction_finished_total", float64(m.compactionFinishedTotal.Load()))
	b = appendMetric(b, "arc_compaction_wedged_total", float64(m.compactionWedgedTotal.Load()))
	b = append(b, "# HELP arc_manifest_repair_dropped_total Cluster manifest entries the shared object store repair dropped because their object is gone (#1207). Growth after the first repairs following an upgrade means something removes objects without updating the manifest.\n"...)
	b = append(b, "# TYPE arc_manifest_repair_dropped_total counter\n"...)
	b = appendMetric(b, "arc_manifest_repair_dropped_total", float64(m.manifestRepairDroppedTotal.Load()))
	b = append(b, "# HELP arc_manifest_repair_adopted_total Compaction outputs in the shared object store the repair registered in the cluster manifest, which compaction never listed before #1207. Growth after the first repairs following an upgrade means compaction outputs are not being registered.\n"...)
	b = append(b, "# TYPE arc_manifest_repair_adopted_total counter\n"...)
	b = appendMetric(b, "arc_manifest_repair_adopted_total", float64(m.manifestRepairAdoptedTotal.Load()))
	b = append(b, "# HELP arc_manifest_repair_pending Compaction outputs the last cluster manifest repair left for a later run (its read budget reached). Falls to zero as the repairs after an upgrade catch up.\n"...)
	b = append(b, "# TYPE arc_manifest_repair_pending gauge\n"...)
	b = appendMetric(b, "arc_manifest_repair_pending", float64(m.manifestRepairPending.Load()))

	b = append(b, "# HELP arc_stale_local_unlinked_total Local copies this node removed because the cluster deleted them while the node missed the delete (down, or crashed with the unlink queued).\n"...)
	b = append(b, "# TYPE arc_stale_local_unlinked_total counter\n"...)
	b = appendMetric(b, "arc_stale_local_unlinked_total", float64(m.staleLocalUnlinkedTotal.Load()))
	b = append(b, "# HELP arc_stale_local_tracked Local files this node saw registered and tracks for the stale-local sweep.\n"...)
	b = append(b, "# TYPE arc_stale_local_tracked gauge\n"...)
	b = appendMetric(b, "arc_stale_local_tracked", float64(m.staleLocalTracked.Load()))
	b = append(b, "# HELP arc_stale_local_blocked 1 while the stale-local sweep refuses to run because it would remove more than cluster.stale_local_sweep_max_fraction of this node's files.\n"...)
	b = append(b, "# TYPE arc_stale_local_blocked gauge\n"...)
	b = appendMetric(b, "arc_stale_local_blocked", float64(m.staleLocalBlocked.Load()))
	b = append(b, "# HELP arc_stale_local_tracking_failed 1 while this node cannot track its local files for the stale-local sweep (its journal in raft_data_dir could not be opened or written); copies whose delete it misses then stay until a restart.\n"...)
	b = append(b, "# TYPE arc_stale_local_tracking_failed gauge\n"...)
	b = appendMetric(b, "arc_stale_local_tracking_failed", float64(m.staleLocalTrackingFail.Load()))

	b = append(b, "# HELP arc_cluster_local_delete_pending Manifest deletes this per-node-storage cluster node has been told about and has not yet unlinked locally. Returns to zero within a grace period of every burst; a value that keeps climbing means the delete workers cannot keep up with retention or a compaction backlog.\n"...)
	b = append(b, "# TYPE arc_cluster_local_delete_pending gauge\n"...)
	b = appendMetric(b, "arc_cluster_local_delete_pending", float64(m.clusterLocalDeletePending.Load()))

	b = append(b, "# HELP arc_cluster_local_delete_deferred Local unlinks of files a row DELETE rewrote that wait for the rewrite to arrive on this node, so reads here never miss its rows. Rises while pulls are slow and returns to zero as they land; a value that never falls means the rewrites are not arriving.\n"...)
	b = append(b, "# TYPE arc_cluster_local_delete_deferred gauge\n"...)
	b = appendMetric(b, "arc_cluster_local_delete_deferred", float64(m.clusterLocalDeleteDeferred.Load()))

	b = append(b, "# HELP arc_compaction_manifests_parked_unparseable_total Total compaction crash-recovery manifests parked under the .quarantined suffix because their body could not be decoded. Growth means a manifest stopped blocking compaction without being completed; the parked file name gives the tier, database and job, and that partition should be checked for a zero-length _compacted output or for duplicate rows.\n"...)
	b = append(b, "# TYPE arc_compaction_manifests_parked_unparseable_total counter\n"...)
	b = appendMetric(b, "arc_compaction_manifests_parked_unparseable_total", float64(m.compactionManifestsParkedUnparseableTotal.Load()))

	b = append(b, "# HELP arc_storage_unaddressable_files Data files the most recent backup found in source storage that no listing returns, so they could not be copied. Non-zero means that backup is incomplete: the files exist and the query path still serves them, but their key does not conform to the storage key rules and no backend method can address one. Rename them and the next backup clears this.\n"...)
	b = append(b, "# TYPE arc_storage_unaddressable_files gauge\n"...)
	b = appendMetric(b, "arc_storage_unaddressable_files", float64(m.storageUnaddressableFiles.Load()))

	b = append(b, "# HELP arc_backup_skipped_files Files the most recent backup inventoried but could not store: unreadable at copy time, or a backup destination key over the storage key limit. Counts every file group the backup copies, the same total the backup status endpoint reports as skipped_files; the backup's manifest and status name up to 32 of the skipped data and Iceberg metadata files in skipped_sample, while a skipped compaction recovery manifest or outside-root warehouse file is counted here and named only in the log. Set by every backup that finishes its copy phases, including one the skip ratio then fails; a backup that fails earlier leaves the previous value. A clean backup sets it to 0.\n"...)
	b = append(b, "# TYPE arc_backup_skipped_files gauge\n"...)
	b = appendMetric(b, "arc_backup_skipped_files", float64(m.backupSkippedFiles.Load()))

	// Cluster Auth metrics (Enterprise, Phase A — Cluster Auth Convergence).
	// apply_* counters increment per applied token command, per node — so
	// every node in a healthy cluster sees the same monotonic count
	// (they all apply the same Raft log). rejected_total counts applier-
	// side validation refusals; non-zero growth is the security alerting
	// signal that something is proposing invalid tokens.
	b = append(b, "# HELP arc_cluster_heartbeats_unknown_node_total Heartbeats received from a node absent from this node's registry.\n"...)
	b = append(b, "# TYPE arc_cluster_heartbeats_unknown_node_total counter\n"...)
	b = appendMetric(b, "arc_cluster_heartbeats_unknown_node_total", float64(m.clusterHeartbeatsUnknownNodeTotal.Load()))
	b = append(b, "# HELP arc_cluster_auth_reconcile_total Reconciles of this node's auth and RBAC tables with the cluster state (#1336).\n"...)
	b = append(b, "# TYPE arc_cluster_auth_reconcile_total counter\n"...)
	b = appendMetric(b, "arc_cluster_auth_reconcile_total", float64(m.clusterAuthReconcileTotal.Load()))
	b = append(b, "# HELP arc_cluster_auth_reconcile_failed_total Reconciles that failed; the local tables may still differ from the cluster.\n"...)
	b = append(b, "# TYPE arc_cluster_auth_reconcile_failed_total counter\n"...)
	b = appendMetric(b, "arc_cluster_auth_reconcile_failed_total", float64(m.clusterAuthReconcileFailedTotal.Load()))
	b = append(b, "# HELP arc_cluster_auth_reconcile_local_rows Local auth/RBAC rows the cluster does not know, at the last reconcile (valid only on this node).\n"...)
	b = append(b, "# TYPE arc_cluster_auth_reconcile_local_rows gauge\n"...)
	b = appendMetric(b, "arc_cluster_auth_reconcile_local_rows", float64(m.clusterAuthReconcileLocalRows.Load()))
	b = append(b, "# HELP arc_cluster_auth_reconcile_collisions Cluster auth/RBAC entries blocked by a local row at the last reconcile (unusable on this node).\n"...)
	b = append(b, "# TYPE arc_cluster_auth_reconcile_collisions gauge\n"...)
	b = appendMetric(b, "arc_cluster_auth_reconcile_collisions", float64(m.clusterAuthReconcileCollisions.Load()))
	b = append(b, "# HELP arc_cluster_auth_reconcile_kept_rows Cluster rows the FSM lacks that the last reconcile kept instead of deleting.\n"...)
	b = append(b, "# TYPE arc_cluster_auth_reconcile_kept_rows gauge\n"...)
	b = appendMetric(b, "arc_cluster_auth_reconcile_kept_rows", float64(m.clusterAuthReconcileKeptRows.Load()))
	b = append(b, "# HELP arc_cluster_auth_apply_create_total Total CommandCreateToken applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_auth_apply_create_total counter\n"...)
	b = appendMetric(b, "arc_cluster_auth_apply_create_total", float64(m.clusterAuthApplyCreateTotal.Load()))
	b = append(b, "# HELP arc_cluster_auth_apply_update_total Total CommandUpdateToken applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_auth_apply_update_total counter\n"...)
	b = appendMetric(b, "arc_cluster_auth_apply_update_total", float64(m.clusterAuthApplyUpdateTotal.Load()))
	b = append(b, "# HELP arc_cluster_auth_apply_revoke_total Total CommandRevokeToken applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_auth_apply_revoke_total counter\n"...)
	b = appendMetric(b, "arc_cluster_auth_apply_revoke_total", float64(m.clusterAuthApplyRevokeTotal.Load()))
	b = append(b, "# HELP arc_cluster_auth_apply_delete_total Total CommandDeleteToken applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_auth_apply_delete_total counter\n"...)
	b = appendMetric(b, "arc_cluster_auth_apply_delete_total", float64(m.clusterAuthApplyDeleteTotal.Load()))
	b = append(b, "# HELP arc_cluster_auth_apply_rotate_total Total CommandRotateToken applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_auth_apply_rotate_total counter\n"...)
	b = appendMetric(b, "arc_cluster_auth_apply_rotate_total", float64(m.clusterAuthApplyRotateTotal.Load()))
	b = append(b, "# HELP arc_cluster_auth_rejected_total Total token command applies refused by FSM-side validation. Non-zero growth indicates a proposer is submitting malformed tokens; alert.\n"...)
	b = append(b, "# TYPE arc_cluster_auth_rejected_total counter\n"...)
	b = appendMetric(b, "arc_cluster_auth_rejected_total", float64(m.clusterAuthRejectedTotal.Load()))

	// Cluster RBAC apply counters (Enterprise, Phase A.1). Same shape as
	// the auth counters above — one per command type, monotonic per node,
	// every node in a healthy cluster sees the same count (they all apply
	// the same Raft log). rejected_total counts applier-side validation
	// refusals across all 13 RBAC command types; non-zero growth is the
	// security alerting signal.
	b = append(b, "# HELP arc_cluster_rbac_apply_create_organization_total Total CommandCreateOrganization applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_apply_create_organization_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_apply_create_organization_total", float64(m.clusterRBACApplyCreateOrganizationTotal.Load()))
	b = append(b, "# HELP arc_cluster_rbac_apply_update_organization_total Total CommandUpdateOrganization applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_apply_update_organization_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_apply_update_organization_total", float64(m.clusterRBACApplyUpdateOrganizationTotal.Load()))
	b = append(b, "# HELP arc_cluster_rbac_apply_delete_organization_total Total CommandDeleteOrganization applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_apply_delete_organization_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_apply_delete_organization_total", float64(m.clusterRBACApplyDeleteOrganizationTotal.Load()))
	b = append(b, "# HELP arc_cluster_rbac_apply_create_team_total Total CommandCreateTeam applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_apply_create_team_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_apply_create_team_total", float64(m.clusterRBACApplyCreateTeamTotal.Load()))
	b = append(b, "# HELP arc_cluster_rbac_apply_update_team_total Total CommandUpdateTeam applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_apply_update_team_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_apply_update_team_total", float64(m.clusterRBACApplyUpdateTeamTotal.Load()))
	b = append(b, "# HELP arc_cluster_rbac_apply_delete_team_total Total CommandDeleteTeam applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_apply_delete_team_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_apply_delete_team_total", float64(m.clusterRBACApplyDeleteTeamTotal.Load()))
	b = append(b, "# HELP arc_cluster_rbac_apply_create_role_total Total CommandCreateRole applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_apply_create_role_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_apply_create_role_total", float64(m.clusterRBACApplyCreateRoleTotal.Load()))
	b = append(b, "# HELP arc_cluster_rbac_apply_update_role_total Total CommandUpdateRole applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_apply_update_role_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_apply_update_role_total", float64(m.clusterRBACApplyUpdateRoleTotal.Load()))
	b = append(b, "# HELP arc_cluster_rbac_apply_delete_role_total Total CommandDeleteRole applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_apply_delete_role_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_apply_delete_role_total", float64(m.clusterRBACApplyDeleteRoleTotal.Load()))
	b = append(b, "# HELP arc_cluster_rbac_apply_create_measurement_permission_total Total CommandCreateMeasurementPermission applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_apply_create_measurement_permission_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_apply_create_measurement_permission_total", float64(m.clusterRBACApplyCreateMeasurementPermissionTotal.Load()))
	b = append(b, "# HELP arc_cluster_rbac_apply_delete_measurement_permission_total Total CommandDeleteMeasurementPermission applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_apply_delete_measurement_permission_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_apply_delete_measurement_permission_total", float64(m.clusterRBACApplyDeleteMeasurementPermissionTotal.Load()))
	b = append(b, "# HELP arc_cluster_rbac_apply_add_token_to_team_total Total CommandAddTokenToTeam applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_apply_add_token_to_team_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_apply_add_token_to_team_total", float64(m.clusterRBACApplyAddTokenToTeamTotal.Load()))
	b = append(b, "# HELP arc_cluster_rbac_apply_remove_token_from_team_total Total CommandRemoveTokenFromTeam applies on this node.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_apply_remove_token_from_team_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_apply_remove_token_from_team_total", float64(m.clusterRBACApplyRemoveTokenFromTeamTotal.Load()))
	b = append(b, "# HELP arc_cluster_rbac_rejected_total Total RBAC command applies refused by FSM-side validation across all 13 RBAC command types. Non-zero growth indicates a proposer is submitting malformed RBAC commands; alert.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_rejected_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_rejected_total", float64(m.clusterRBACRejectedTotal.Load()))
	b = append(b, "# HELP arc_cluster_rbac_cascade_rejected_total Total DeleteOrganization/DeleteTeam calls refused by the proposer-side cascade-depth cap. Phase A.2 Item 2: see cluster.rbac.max_cascade_descendants. Non-zero growth means operators are issuing cascades large enough to risk FSM apply blocking past the Raft heartbeat margin; consider raising the cap or chunking the delete.\n"...)
	b = append(b, "# TYPE arc_cluster_rbac_cascade_rejected_total counter\n"...)
	b = appendMetric(b, "arc_cluster_rbac_cascade_rejected_total", float64(m.clusterRBACCascadeRejectedTotal.Load()))

	return string(b)
}

// Helper functions for Prometheus format
func appendMetric(b []byte, name string, value float64) []byte {
	b = append(b, name...)
	b = append(b, ' ')
	b = appendFloat(b, value)
	b = append(b, '\n')
	return b
}

func appendMetricWithLabel(b []byte, name, labelName, labelValue string, value float64) []byte {
	b = append(b, name...)
	b = append(b, '{')
	b = append(b, labelName...)
	b = append(b, '=', '"')
	b = append(b, labelValue...)
	b = append(b, '"', '}', ' ')
	b = appendFloat(b, value)
	b = append(b, '\n')
	return b
}

func appendMetricWithLabels(b []byte, name string, labelNames, labelValues []string, value float64) []byte {
	b = append(b, name...)
	b = append(b, '{')
	for i := range labelNames {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, labelNames[i]...)
		b = append(b, '=', '"')
		b = append(b, labelValues[i]...)
		b = append(b, '"')
	}
	b = append(b, '}', ' ')
	b = appendFloat(b, value)
	b = append(b, '\n')
	return b
}

func queryEngineExecutionSnapshot(counters *[2]atomic.Int64) map[string]int64 {
	return map[string]int64{
		"success": counters[queryOutcomeSuccess].Load(),
		"error":   counters[queryOutcomeError].Load(),
	}
}

func appendFloat(b []byte, v float64) []byte {
	// Simple float formatting - enough for metrics
	if v == float64(int64(v)) {
		return appendInt(b, int64(v))
	}
	// Format with up to 6 decimal places
	intPart := int64(v)
	fracPart := int64((v - float64(intPart)) * 1000000)
	if fracPart < 0 {
		fracPart = -fracPart
	}
	b = appendInt(b, intPart)
	b = append(b, '.')
	// Pad with zeros
	if fracPart < 100000 {
		b = append(b, '0')
	}
	if fracPart < 10000 {
		b = append(b, '0')
	}
	if fracPart < 1000 {
		b = append(b, '0')
	}
	if fracPart < 100 {
		b = append(b, '0')
	}
	if fracPart < 10 {
		b = append(b, '0')
	}
	b = appendInt(b, fracPart)
	return b
}

func appendInt(b []byte, v int64) []byte {
	if v < 0 {
		b = append(b, '-')
		v = -v
	}
	if v == 0 {
		return append(b, '0')
	}
	var digits [20]byte
	i := len(digits)
	for v > 0 {
		i--
		digits[i] = byte('0' + v%10)
		v /= 10
	}
	return append(b, digits[i:]...)
}
