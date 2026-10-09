# Arc - Claude Code Instructions

## Project Overview

Arc is a high-performance columnar analytical database written in Go. Built on DuckDB (query engine), Parquet (storage format), and Arrow (data format). Use for product analytics, observability, AI agent memory, IoT telemetry, log analysis, or data warehousing.

**Tech stack:** Fiber (HTTP), zerolog (logging), DuckDB (queries), SQLite (metadata/auth/audit), Apache Arrow/Parquet (columnar storage).


## Architecture

- **Storage path format:** `{database}/{measurement}/{year}/{month}/{day}/{hour}/{filename}.parquet`
- **Shared SQLite DB** at `cfg.Auth.DBPath` — used by auth, audit, tiering metadata, and MQTT
- **2-tier storage system:** hot (local) and cold (S3/Azure)
- **Clustering:** Raft consensus, writer/reader roles, WAL replication
- **Enterprise product definition:** `docs/progress/2026-01-08-arc-enterprise-product-definition.md`

### Enterprise Features (require license gating)

Features that require a license key: tiering, audit logging, writer failover, clustering, RBAC, auto-aggregation, **arcx** (proprietary DuckDB extension; loader in `internal/database/duckdb.go#configureArcxExtension`, license-gated at `cmd/arc/main.go` before `database.New`; extension repo is `Basekick-Labs/arcx`, private). **arcx's licence gating is going away** — it ships in the Arc binary at release (user, 2026-10-02). The gate is still in the code today; do not describe arcx to users as optional or enterprise-only.

**arcx issues on the public repo: label `wontfix`, leave them open.** arcx is
developed closed source in the private `Basekick-Labs/arcx` repo and ships
with Arc as a prebuilt binary. Its source is not in this repo, so a report
about arcx's own behaviour, internals, SQL coverage, performance or roadmap
cannot be acted on here no matter how valid it is — the fix would land in a
repository the reporter cannot see and neither can this one.

Apply the `wontfix` label and leave the issue **open**, so it stays visible and
the work can still be tracked internally. Say why; the reporter is owed the
reason, not just a label:

> arcx is developed closed source in a separate repository, so there is
> nothing in this repo that can be changed to address this — tagging
> `wontfix` on that basis. Leaving it open for visibility; it is tracked on
> the arcx side. If you are seeing this on a deployment, raising it through
> support gets it in front of the people who can act on it.

Do not frame arcx as optional or licence-gated in that reply. It ships in the
Arc binary at release. (The code today still gates it — see the licence
pattern below — but that gating is going away, so do not build the
explanation on it.)

**What is NOT an arcx issue:** Arc's own glue around it — `internal/arcxrouter`,
`configureArcxExtension` in `internal/database/duckdb.go`, the licence gate in
`cmd/arc/main.go`, and any build-tag combination involving `arcx_engine`. That
is Arc source in this repo, it ships to every user, and a bug there is an
ordinary Arc bug (#1016 is one: `go test -tags=duckdb_arrow,arcx_engine
./internal/api/` panics on a nil `*DuckDB` receiver). Read the report before
labelling — "mentions arcx" is not the test, "the fix would land in the arcx
repo" is.

**License gating pattern:**
1. Add feature constant in `internal/license/license.go` (e.g., `FeatureWriterFailover = "writer_failover"`)
2. Add `CanUseX()` method on both `License` struct and `Client` struct (`internal/license/client.go`)
3. Check at startup in `cmd/arc/main.go` or coordinator before initializing the feature
4. Add runtime re-validation middleware on API routes (see `requireTieringLicense` pattern)

**Auth pattern for API routes:**
- **CRITICAL: Every new API endpoint MUST have auth middleware.** No endpoint should ever be accessible without authentication when auth is enabled. This is a non-negotiable security requirement — missing auth on CQ and delete endpoints was a real vulnerability found in production code.
- Use `auth.RequireAdmin(authManager)` middleware on admin-only routes (mutating operations: create, update, delete, execute)
- Read-only routes (list, get, status) may use `auth.RequireRead(authManager)` where available, or `RequireAdmin` if no read-level auth exists
- Always guard with `if h.authManager != nil` (auth can be disabled)
- License middleware goes after auth middleware
- When reviewing or writing handlers, **always verify auth is wired** in both `RegisterRoutes` and `cmd/arc/main.go` (authManager must be passed to the constructor)

## Build & Test

```bash
go build ./cmd/... ./internal/...
go test ./internal/<pkg>/... -v
go test -race ./internal/<pkg>/...   # for any concurrency-touching change
gofmt -l ./internal ./cmd            # must return empty
go vet ./...                         # must return empty for affected packages
```

**Before opening a PR** that touches `cmd/arc/main.go`, `internal/cluster/`, or any startup wiring: actually run the built binary against demo data and confirm the changed path executes. Unit tests + reviewer agents have a blind spot for startup-time nil-derefs and configuration-interaction bugs; the binary running for ten seconds catches the loudest of them for free.

**If the diff adds a config key, run the binary at least once with that key set to a NON-DEFAULT value.** "I ran the binary" is not coverage of a key you only ever left at its default — that is the same run as everyone else's. Iceberg export shipped a broken `iceberg.warehouse` through 7 review rounds and a dozen binary runs because every run used the default (#534). One run with the key actually set would have caught it in seconds.

**Known quirk:** `cmd/arc` is in `.gitignore` — use `git add -f cmd/arc/main.go` when staging changes to main.go.

## Conventions

### Code Style
- Structured logging with zerolog: `.Str("key", val).Msg("description")`
- Component loggers: `logger.With().Str("component", "name").Logger()`
- Context with timeout on all API handlers: `context.WithTimeout(c.Context(), 30*time.Second)`
- Test loggers: `zerolog.Nop()`
- Parameterized SQL queries only (never string interpolation)
- Validate inputs at system boundaries (API handlers, file path parsing)

### Git & PRs
- Always create a branch from main.
- Branch naming: `feat/description`, `fix/description`
- Commit format: `feat(scope): description` or `fix(scope): description`
- PR descriptions: Summary bullets + Test plan checklist
- Main branch: `main`
- **PR review:** There is no external PR-review bot. Review is entirely internal — the configuration matrix + single deep adversarial reviewer described under [Post-Implementation Review](#post-implementation-review). Complete that pass and address every finding before marking the PR ready. (Gemini Code Assist was the external backstop through mid-2026; it has been sunset and removed from this workflow.)

### API Handler Pattern
```go
type XHandler struct {
    manager       *pkg.Manager
    authManager   *auth.AuthManager
    licenseClient *license.Client
    logger        zerolog.Logger
}

func (h *XHandler) RegisterRoutes(app fiber.Router) {
    group := app.Group("/api/v1/x")
    if h.authManager != nil {
        group.Use(auth.RequireAdmin(h.authManager))
    }
    group.Use(h.requireXLicense)
    // ... routes
}
```

## Planning & Review Process

### Plan Documentation
When exiting plan mode to begin implementation, ALWAYS save the implementation plan first as a markdown file in `docs/progress/` with the date and phase name in the filename. For example: `2024-11-24-phase1-foundation.md`. These files shouldn't be tracked. 

### Pre-Plan Validation
Before finalizing a plan, use another agent to validate your findings. If there is consensus, ask for authorization from the user to move forward.

### Post-Implementation Review

**Internal review is now the entire review process** — there is no external reviewer bot to catch what it misses, so it has to be good. Do NOT let that tempt you into a sprawling, pattern-matching prompt: past attempts to make one agent "catch everything" produced prompts that matched on past findings while missing the actual bugs (PR #444 needed 3 external-review rounds + a follow-up PR; PR #445 shipped a critical nil-deref a 4-agent review missed). The failure mode is always the same: **reviewers run on the diff, not on the running system.** The defense is the configuration matrix plus actually running the binary (below), not a longer prompt.

The fix is a three-step process. All three are mandatory; **the two tables come first, before any reviewer is invoked.**

#### Step 1: Configuration matrix (written by the implementer, not an agent)

Before invoking any reviewer, write down — in the conversation, not a file — a small table:

| Configuration | Reaches new code? | Preconditions established? |
|---|---|---|
| OSS standalone (no cluster, no compaction) | ... | ... |
| OSS + compaction enabled | ... | ... |
| Cluster + no compaction | ... | ... |
| Cluster + compaction + server.tls_enabled | ... | ... |
| Cluster + compaction + cluster.tls_enabled | ... | ... |

For each cell:
- "Reaches new code?" — yes / no / partial. Trace from process startup through the actual `if` blocks. Do not handwave; cite line numbers.
- "Preconditions established?" — for every pointer dereference the new code performs, what invariant guarantees non-nil? Cite the line that establishes it.

If a row is "yes, but I don't know if precondition X holds in this mode" — that row IS the bug. Find it and fix it before continuing.

**Then add one row per config key the diff introduces, set to a NON-DEFAULT value.** Not just "feature on/off" — every new key, at a value an operator would plausibly set. For each: does the new code read that key on a path the default never exercises? What breaks if it does?

| Configuration | Reaches new code? | Preconditions established? |
|---|---|---|
| `iceberg.enabled=true`, `iceberg.warehouse` = **subdirectory of storage root** (non-default) | ... | ... |
| `iceberg.enabled=true`, `iceberg.retain_snapshots` = **1** (non-default) | ... | ... |

**Why this row exists (PR #534):** Iceberg export survived 7 external-review rounds, a full internal review, and a dozen live binary runs — every one of them with `iceberg.warehouse` at its default (the storage root). `warehouseRelKey` trimmed the *warehouse* to build keys that backends resolve against the *storage root*. Those are the same string **only at the default**, so pointing the key at a subdirectory silently dropped that segment: `version-hint.text` landed outside the warehouse and DuckDB/Spark could not resolve the snapshot — the feature's headline path, broken, in the config the feature itself invites you to set. Defaults are the one configuration everything gets tested in; a new key's non-default values are the one nobody tries.

**Corollary — the same blind spot applies to the fix.** #534's first fix then shipped a `strings.HasPrefix` that matched `wh-other` as `wh`. When you write a fix for a config-path bug, test the fix's *own* edge cases (sibling names, trailing slashes, empty values), not just the case that prompted it.

**Why this works:** the bugs that internal review keeps missing are configuration-interaction bugs (PR #445 critical: new code inside `if compactionManager != nil` touched `clusterCoordinator` — two independently-enabled subsystems). Forcing the matrix surfaces them mechanically.

**Concrete shapes to enumerate explicitly**, because they have bitten us:
- `if compactionManager != nil` + dereferencing `clusterCoordinator` (independently enabled)
- `if cfg.Cluster.Enabled` + assuming `cfg.Server.TLSEnabled` matches (independent flags)
- `if licenseClient != nil` + assuming `authManager != nil` (independent)
- New constructor inside `func main()` after `licenseClient`/`authManager`/`clusterCoordinator` initialization — each may be nil in supported modes
- **A new config key whose default makes two different values identical** — `iceberg.warehouse` defaults to the storage root, so "trim the warehouse" and "trim the storage root" were the same operation until an operator set it (#534). Any `X defaults to Y` invites code that conflates X and Y. Ask: which expressions in the new code are equal *only because* the key is at its default?
- **A path built by concatenation/trim rather than by a path helper** — `strings.HasPrefix`/`TrimPrefix` on directory strings match mid-segment (`/data/wh` matches `/data/wh-other`). For "is p under dir", match at a boundary: `p == dir || strings.HasPrefix(p, dir+"/")`.

#### Step 1b: Invariant delta (written by the implementer, not an agent)

The configuration matrix asks *which configurations reach this code*. It does
not ask **what this diff changed about the guarantees the surrounding code was
relying on** — and that is where PR #1145 nearly shipped silent data loss on a
restore. Second small table, same rules: in the conversation, before any
reviewer.

One row for each of these the diff contains. If it contains none, say so
explicitly — that is a one-line answer, not a reason to skip the step.

| Change | What the old form guaranteed | Who relied on it (file:line) | Still true? |
|---|---|---|---|

**What counts as a row:**

- **A condition widened.** A new way to reach an existing branch, a renamed
  flag with an extra assignment, an `||` added to an `if`. The old, narrower
  form was an invariant something else was built on. *#1145: `coldToHot` was
  set only when `coldBackendOrNil() == nil`; renaming it `forceHot` and adding
  a second assignment meant the branch could now run on a node that HAS a cold
  backend — and the orphan sweep's safety argument rested on exactly that
  being impossible.*
- **A comment deleted or rewritten.** Ask what the old comment was *for*.
  A comment carrying issue numbers, or a correction of its own earlier claim,
  is a warning someone paid for. If the diff both deletes the reasoning and
  invalidates its precondition, that is the bug, and the deleted text is the
  only place it was ever written down.
- **A state write deferred, batched or reordered.** Name every other
  subsystem that reads that state, and what it does during the window.
  *#1145: the forced tier-row update moved into a batched flush, and
  `ReconcileOrphanedFiles` selects on exactly the stale row that leaves
  behind.*
- **A precedent cited in a comment or PR body.** "the same discriminator used
  by X", "matching the existing behaviour in Y" — **grep for it before
  believing it.** In #1145 the cited expression occurred exactly once in the
  tree: in the PR's own new line. A false precedent is worse than no
  justification, because it is what the next person will weaken the check
  against. See also: check whether the precedent was safe *because* of the
  mechanism cited, or because of a second source the new site lacks.

**Two rules that need no table, because they are absolute:**

- **Observability must not change control flow.** A diagnostic read, log, or
  metric added to a path that previously could not fail must not make it
  failable. #1145 added a `GetFile` purely to log a refusal; its error
  returned into a caller whose `case err != nil` increments a failure counter
  and logs "the next tier scan will reconcile" — for an operation that had
  already succeeded. Swallow the diagnostic's error, or compute it where
  failure is already expected.
- **A test that observes only after the call returns pins balance, not
  placement.** An increment paired with a deferred decrement is invisible from
  outside the function; moving it does not change what any post-call assertion
  sees. To pin an *edge* of a window, observe from inside it — a log hook, or
  an injectable seam the production path already calls. And when you
  mutation-test, apply the **faithful** mutation: moving a statement, not
  adding a second copy of it, which fails for the wrong reason.

#### Step 2: Single deep reviewer (one agent, not four)

Spawn **one** general-purpose agent. The prompt MUST include:

1. **The configuration matrix from Step 1 and the invariant delta from Step 1b**, both verbatim. The reviewer confirms each row by tracing the code and flags any row that is wrong — including an invariant-delta row whose "still true?" is asserted rather than traced.
2. **The diff to review** (paste or reference `git diff main..HEAD`).
3. **Specific things to check, in order**:
   - **(a) Outer-block precondition trace** — for every new pointer dereference, what guarantees non-nil under the configurations in the matrix? Flag any deref that doesn't have a cited establishment line. This is the highest-yield check; do it FIRST.
   - **(a2) Non-default config values** — for every config key the diff introduces or newly reads: trace the code with that key at a non-default value. Flag any expression that is correct *only because* the key sits at its default (e.g. two paths that are the same string until an operator overrides one). See #534 in Step 1.
   - **(a3) Invariant delta** — for every row of the Step 1b table, trace it. For a widened condition, find the code that depended on the narrow form and say whether it still holds. For a deleted comment, read the removed text in `git diff` and judge whether its reasoning still applies. For a cited precedent, grep for it. This is the second-highest-yield check after (a), and it is the one that catches a diff which is locally correct and globally wrong.
   - **(b) OSS standalone smoke test** — does the new code execute in OSS (no cluster, no license)? If yes, is every cluster/license-only field gated? If no, does the gate let `main.go` complete startup cleanly?
   - **(c) Failure modes that the matrix surfaces** — for each "yes" row, what does a partial failure look like? What state does the system land in if step N fails after step N-1 succeeded?
   - **(d) Doc-vs-code drift** — release notes claims, comments above the affected functions, doc-comments on exported helpers. Does every prose claim match the code that just shipped?
   - **(e) Hot-path nits** — string concat in loops, `fmt.Sprintf` where concat works, `http.DefaultClient` where a tuned client exists nearby, missing `defer Close()`. Brief pass; this is the cheap section.
   - **(f) For SQLite-touching diffs** — if the diff writes to `*sql.DB`, also run the [SQLite Review Checklist](#sqlite-review-checklist). This is a 30-second mechanical pass that catches the most common SQLite bugs (time format mismatch, missing ORDER BY in batched DELETEs, holding app mutexes across DB I/O, missing `v.SetDefault` for new config keys, double-logging).

4. **Output format constraint**: Blockers / High / Medium / Style. For each finding, cite file:line. **For Blockers and High, cite which row of the configuration matrix the finding falls into** — this proves the reviewer actually traced rather than pattern-matched.

5. **"Don't be deferential"** directive remains.

6. **Working-tree safety, stated explicitly in the prompt**: "The branch under
   review may be uncommitted. Do NOT run `git checkout`, `git restore`,
   `git stash`, `git reset`, or anything else that mutates the working tree or
   index. If you mutation-test a change, revert it with the editing tool you
   used to make it, never with git." A reviewer on PR #1159 mutation-tested the
   gate, then ran `git checkout <file>` to undo its own edit and reverted the
   entire uncommitted fix; it survived only because that agent happened to have
   taken a backup first.

   **Commit before handing a dirty worktree to any agent.** The prompt line is
   the backstop, not the defense.

#### Reviewing a contributor PR (the throughput path)

Most open PRs are contributions. The slow part is almost never the reading —
it is round-trips measured in days, and reviewing the wrong tree.

1. **Rebase onto current `main` before reading a line.** GitHub's
   `CONFLICTING` is its plain-merge view; a `git rebase origin/main` in a
   worktree usually applies cleanly and takes seconds. Reviewing the submitted
   tree finds bugs that no longer exist and misses the ones that only appear
   against current `main` — #1145's base was 8 commits behind.
2. **Confirm nothing from `main` was lost in the rebase**, especially when a
   file was renamed on either side. Git reports a clean rebase that silently
   drops nothing, but a rename-plus-modify is where content vanishes; grep for
   a distinctive string from each intervening commit.
3. **The two tables are yours, not theirs.** A contributor will not have
   written a configuration matrix or an invariant delta. Writing them is the
   review — it is how you find what the diff did to the surrounding
   guarantees, and it is cheaper than a second review round.
4. **Mutation-test their regression test, don't trust the claim.** "Failed on
   pristine main" is checkable in one command, and it is the difference
   between a test and a decoration.
5. **Decide early: request changes, or carry it.** A change request to an
   external contributor costs days of latency and often a rebase war. If the
   remaining work is a judgement call about *our* invariants — the trade-off
   touches history they cannot see, or needs a decision only a maintainer can
   make — say "we will take it from here", list what you are keeping, and
   credit them on the PR that lands. Reserve change requests for work they are
   positioned to do: their own test coverage, their own naming, a mistake
   local to their diff. #1145 is the model: approach kept, blocker carried,
   credit given, and the contributor told plainly rather than left waiting.
6. **Tell them what actually blocks their queue.** If their other PRs are
   CONFLICTING, saying so is worth more than any individual review comment.

#### What NOT to do

- **Do not run four parallel agents** with overlapping prompts. Past data: they find ~the same things and miss the same things. One deep agent with the matrix is cheaper and more effective.
- **Do not prompt the reviewer with a long list of past findings**. That trains shallow-broad pattern matching. The matrix forces deep-narrow tracing.
- **Do not skip the matrix** because "it's a small diff". The bugs we miss are exactly in small diffs touching wiring code.
- **Do not let the invariant delta become a findings checklist.** Its rows are categories of *change* — a condition widened, a comment deleted, a write deferred, a precedent cited — not a list of bugs we have seen. The moment someone appends "check for the #1145 sweep bug" it stops forcing tracing and starts inviting pattern-matching, which is the failure this whole section exists to avoid. Add a category only when a finding could not have been a row in any existing one.
- **Do not review the tree as submitted** on a contributor PR. Rebase first; see the throughput path above.

#### When to use additional reviewers

- **Cluster operations with Raft/manifest interactions**: add a second agent focused specifically on the [Cluster Operations Checklist](#cluster-operations-checklist) below. Its job is the manifest-before-storage / batch-not-per-file / abort-on-quorum-loss patterns.
- **Security-relevant changes (auth, HMAC, TLS, SQL, file paths)**: add a second agent focused on the [Security Checklist](#security-checklist) below. Its job is auth wiring, parameterized queries, path traversal, CSRF surfaces.
- **SQLite-touching changes (new queries, schema changes, cleanup jobs)**: add a second agent focused on the [SQLite Review Checklist](#sqlite-review-checklist) below. Its job is catching time-format mismatches, missing ORDER BY in batched DELETEs, holding application mutexes across DB I/O, missing `v.SetDefault` for new config keys, and double-logging across layers. These are cheap-to-catch, expensive-to-fix bugs — PR #483 needed 9 review rounds, 4 of which were SQLite-specific.
- **Skip the additional reviewer** for changes that don't touch those domains. Most PRs need just the single deep reviewer + the matrix.

#### Release hygiene (carry-over, unchanged)

- Update release notes and (for enterprise features) the enterprise-product-definition file at implementation time.
- **Re-update both after each fix-up commit** — do not let docs drift behind the code as findings are addressed. A recurring past finding was exactly this shape: release notes claimed a fix the patch didn't include (PR #445).

### Review Loop Discipline

When the deep reviewer finds issues, address all of them in a single follow-up commit before marking the PR ready. Don't leave known-caught issues in intermediate commits.

Because internal review is now the only review, the discipline shifts to **not trusting a single pass**: after fixing the reviewer's findings, decide honestly whether the fix itself opened a new configuration cell (the #534 pattern — the *fix* for a config-path bug shipped its own edge-case bug). If it did, add that row to the matrix and re-check that row specifically. If a finding revealed a class the matrix didn't enumerate:

1. **First, update the configuration matrix — or the invariant delta — to include the row you missed.** These are the durable artifacts; the next PR's tables should inherit them. A finding that came from a widened condition, a deleted comment, a deferred write or a cited precedent belongs in the Step 1b table, not the matrix.
2. Then fix the finding.
3. Do NOT blanket re-spawn the reviewer on the whole diff for a finding that's already understood — re-check only the cell that changed. A full re-review of unchanged code is wasted tokens.

**Honest expectations**: with no external backstop, the bar is that the matrix + one deep pass + a real binary run catch the embarrassing classes — nil-derefs, OSS-mode panics, doc-vs-code drift, missing auth, config-key-at-non-default. Genuinely subtle issues (cross-endpoint MAC replay, NUL-delimiter ambiguity) may still slip; the mitigation is the binary run and the matrix, not a second reviewer agent.

## Cluster Operations Checklist

When writing any operation that mutates storage or cluster state in cluster mode:

1. **Writer-only execution for scheduled jobs** — retention, CQ, and similar schedulers must gate on `IsPrimaryWriter()` checked at every tick (not at `Start()`), so failover and demotion take effect without a restart. Use the `ClusterGate` interface pattern from `internal/compaction/scheduler.go` as the reference.

2. **Manifest-before-storage ordering** — always update the Raft manifest *before* deleting from storage. If the manifest update fails, the file still exists in both places and the next run retries it. The reverse order creates permanent orphan manifest entries with no retry path.

3. **Abort on manifest failure** — a Raft quorum loss is not transient. If `BatchFileOpsInManifest` fails, stop the current operation and return an error. Do not continue processing further chunks.

4. **Batch Raft proposals, never per-file** — use `BatchFileOpsInManifest` (not `DeleteFileFromManifest` in a loop). Chunk at 1000 ops max to avoid oversized Raft log entries. Interleave manifest update and storage delete per chunk so a mid-run failure limits orphan blast radius to one chunk.

5. **No self-heal assumptions** — nothing re-registers a file whose manifest entry was lost, and nothing restores a file whose manifest entry outlived it. The only reconciliation is the Phase 5 sweep in `internal/reconciliation`, and it is OPT-IN (`reconciliation.enabled`, default false) and REPORT-ONLY until the operator also flips `reconciliation.manifest_only_dry_run` to false. When it does act, it deletes in both directions after the grace window (default 24 h): a manifest entry with no file is removed from the manifest, and a file with no manifest entry is removed from storage. So a lost registration is not "recovered later" — on an enabled cluster it is an orphan-storage delete candidate. Do not write comments or log lines claiming orphans will "self-heal", "be retried by anti-entropy" or "be cleaned up by Phase 5" as if that were automatic; log at `Error`/`Warn`, say nothing re-registers it, and where the sweep would delete it, say that (see `CoordinatorFileRegistrar` in `internal/cluster/file_registrar.go` for the wording).

6. **DuckDB `read_parquet()` paths must be escaped** — DuckDB does not support parameterized `read_parquet()` calls. Always escape single quotes in interpolated paths: `strings.ReplaceAll(path, "'", "''")`.

## Security Checklist

When adding or modifying any API endpoint, verify ALL of the following:

1. **Auth middleware is present** — check `RegisterRoutes` has `auth.RequireAdmin` (or appropriate auth level)
2. **Auth is wired in `main.go`** — `authManager` is passed to the handler constructor
3. **User input is validated** — WHERE clauses, SQL fragments, file paths, database/measurement names
4. **No SQL interpolation** — use parameterized queries for SQLite; for DuckDB `read_parquet()` WHERE clauses (which can't use parameters), escape single quotes (`strings.ReplaceAll(path, "'", "''")`)
5. **Temp files/dirs use restrictive permissions** — `0700` for directories, `0600` for files containing data
6. **Large data is streamed, not buffered** — use `WriteReader` for S3/Azure uploads, never `os.ReadFile` on potentially large files

## SQLite Review Checklist

When any diff touches `*sql.DB` (new queries, schema changes, periodic cleanup, or maintenance jobs), verify ALL of the following. These are cheap to catch in review, expensive to fix post-merge (PR #483: 9 review rounds, 4 SQLite-specific).

1. **Time comparison: one domain only.** Never mix Go `time.Time` (serialized by go-sqlite3 as RFC3339: `"2026-05-29T13:00:00Z"`) with SQLite native datetime functions (space-separated: `"2026-05-29 15:00:00"`). The `T` (ASCII 84) vs space (ASCII 32) mismatch makes `"2026-03-06T…" > "2026-03-06 …"` — boundary-day records silently escape deletion. **Rule: if values are stored via Go `time.Time` parameters, compare against Go `time.Time` parameters.** If values are stored via SQLite `CURRENT_TIMESTAMP`, use SQLite `datetime()` for comparison. Never cross the streams.

2. **Batch large DELETEs.** A single `DELETE FROM t WHERE …` on a table with millions of rows holds the SQLite write lock for seconds/minutes, blocking all other writes (ingestion file registration, auth token updates). Chunk at 1000 rows with `LIMIT` in a loop, and check `ctx.Err()` between batches.

3. **Subquery DELETEs need ORDER BY.** `DELETE … WHERE id IN (SELECT id … LIMIT 1000)` without `ORDER BY` is non-deterministic — SQLite may return arbitrary rows. Always add `ORDER BY` aligned with an existing index (e.g. `ORDER BY started_at ASC` for `idx_tier_migrations_started`).

4. **Don't vacuum hot tables.** `PRAGMA incremental_vacuum` after a DELETE on a table that is continuously written (tier_migrations, audit_logs during active ingest) causes write amplification: vacuum shrinks the file, the next INSERT re-grows it. Freed pages are automatically reused by subsequent INSERTs — the DELETE alone stops unbounded growth. Reserve vacuum for infrequent maintenance or tables that genuinely shrink long-term.

5. **Don't hold application mutexes across DB I/O.** `*sql.DB` is thread-safe. If a method only uses `s.db` and doesn't touch in-memory state, it does not need `s.mu.Lock()`. Holding the mutex during a slow DELETE blocks all concurrent readers (e.g. `GetTiersForMeasurement` → query latency spikes).

6. **Every new config key needs `v.SetDefault()`.** Consumer-side fallbacks (`if x <= 0 { x = 90 }`) are a secondary safety net, not a substitute. Always add the corresponding `v.SetDefault("tiered_storage.…", …)` in `internal/config/config.go#setDefaults`. This makes the default visible to operators (via `--help` / config docs) and prevents subtle drift between the code default and the documented default.

7. **Don't log at multiple layers.** If the caller (e.g. `Manager.cleanupOldMigrations`) already logs the result, the callee (e.g. `MetadataStore.CleanupOldMigrations`) should not also log on success. Pick one layer — prefer the higher one. Logging errors/warnings at the lower layer is fine.

8. **Test SQLite performance:** For tests that do bulk inserts (hundreds+ rows), each insert in its own implicit transaction triggers an fsync. Use `PRAGMA synchronous = OFF` on the test connection, or wrap inserts in a single explicit transaction. Keep bulk-insert test record counts just above the threshold they're testing (e.g. 1050 for a 1000-row batch limit).

## Common Pitfalls

- Path traversal: always validate database/measurement names extracted from file paths (no `..`, no `/\`)
- SQLite file permissions: should be `0600` (contains auth tokens, audit logs)
- `sync.RWMutex`: two concurrent `RLock`s don't deadlock (safe for read-only getters)
- Fiber middleware ordering matters: auth before license before handlers
- When wiring new features in `cmd/arc/main.go`, check that `licenseClient`, `authManager`, `clusterCoordinator`, `compactionManager` may EACH be nil — **and they are independently enabled**. `if compactionManager != nil` does NOT imply `clusterCoordinator != nil`; OSS deployments run compaction without cluster. New code inside one nil-check that dereferences another subsystem must add its own guard.
- Missing auth is a **critical vulnerability** — never skip it, even on "internal" endpoints
- Cluster gate checks belong in the execution path (e.g. `runRetention()`), not in `Start()` — role changes must take effect without restart
- **Shutdown: every `RegisterHook` runs before every `Register` component, whatever the priorities.** Priority orders only within a group. To order a step relative to a component (the arrow-buffer flush, the WAL, the registrar) it must be a component too — see `registerClusterCoordinatorShutdown`/`registerTieringShutdown`/`registerFileRegistrarShutdown` in `cmd/arc/main.go`. The cluster coordinator (Raft) and tiering are components for this reason (#1014, #803); a comment that claims "X at priority 31 runs after Y at 30" is false when one is a hook and the other a component, and `TestClusterShutdownOrderKeepsRaftAliveForTheFinalFlush` is the model to extend.
- **TLS config flags are NOT one thing**: `cfg.Server.TLSEnabled` gates the Fiber HTTP listener (the public API + every cluster-internal HTTP endpoint mounted on it); `cfg.Cluster.TLSEnabled` gates Raft RPC + raw-TCP peer-fetch. An operator can enable either independently. Inter-node HTTP URL scheme must key off `cfg.Server.TLSEnabled` (what peers actually serve); inter-node HTTP TLS verification config keys off `cfg.Cluster.TLSEnabled` (private cluster CA, if configured).
- **Reverse-proxy URL building**: when forwarding an `*http.Request` to another node, use `originalReq.URL.EscapedPath()`, not `.Path`. `URL.Path` is the *decoded* form; paths containing spaces, percent-encoded bytes, or non-ASCII characters land malformed at the peer.
- **Inter-node HTTP clients**: never use `http.DefaultClient` for cluster fan-out. Construct via `security.NewClusterHTTPTransport(coordinator.ClusterTLSConfig())` so the cluster TLS config (if any) is shared with the cluster-internal HTTP path, and pool defaults match the rest of `internal/cluster/`.
- **SQLite time comparison**: never compare Go `time.Time` values (RFC3339, `T`-separated) against SQLite `datetime()` output (space-separated). The format mismatch causes incorrect string comparisons. Match domains: Go↔Go or SQLite↔SQLite, never cross them. See [SQLite Review Checklist](#sqlite-review-checklist) for the full list.
- **SQLite batch DELETEs**: always chunk at ≤1000 rows with `LIMIT`, add `ORDER BY` on an indexed column, and check `ctx.Err()` between batches. Single large DELETEs block all other SQLite writers.
- **Config defaults**: every new config key in `TieredStorageConfig` (or any config struct) MUST have a corresponding `v.SetDefault()` in `setDefaults()`. Consumer-side fallbacks are not a substitute. See SQLite Review Checklist item 6.
- **Application mutex + DB I/O**: don't hold `sync.Mutex`/`sync.RWMutex` across `*sql.DB` calls. The DB is thread-safe; the mutex only protects in-memory state. Holding it during I/O blocks all concurrent readers unnecessarily.

## DeepSeek-specific notes

DeepSeek is action-oriented and will skip process steps unless explicitly constrained.
When using DeepSeek for a task:

1. **Prompt with explicit gates.** Start with: "Follow CLAUDE.MD — branch, fix,
   release notes, config matrix, deep review, wait for CI, merge. Do not skip steps."
2. **It will jump to code first.** That's fine — redirect it to branch/process
   after the first correction. It learns within the session.
3. **Descriptive output.** Ask for full commit messages, PR bodies, and matrix
   tables explicitly — the default is terse.
4. **One issue per chat.** Token budget matters; DeepSeek sessions benefit from
   clean starts with no residual state from previous tasks.
5. **Curl over `gh` for API calls.** DeepSeek defaults to `gh` CLI for GitHub
   operations; prefer explicit `curl` commands against the GitHub API for
   comments/reviews to avoid interactive auth prompts.
