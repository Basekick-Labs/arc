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

Features that require a license key: tiering, audit logging, writer failover, clustering, RBAC, auto-aggregation, **arcx** (proprietary DuckDB extension; loader in `internal/database/duckdb.go#configureArcxExtension`, license-gated at `cmd/arc/main.go` before `database.New`; extension repo is `Basekick-Labs/arcx`, private).

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
- **PR review:** After creating a PR, always request a review from `gemini-code-assist` by adding it as a reviewer (`gh pr edit --add-reviewer gemini-code-assist[bot]`). Wait for Gemini's review comments and address any findings before merging.

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

**Be honest about what internal review is.** It's a sanity pass on the first draft before Gemini sees it. It is NOT a substitute for Gemini. Past attempts to make internal review "Gemini-equivalent" produced sprawling prompts that pattern-matched on past findings while missing the actual bugs (PR #444: 3 Gemini rounds + a follow-up PR; PR #445: critical nil-deref the 4-agent review missed). The pattern: **reviewers run on the diff, not on the running system.**

The fix is a two-step process. Both steps are mandatory; **the configuration matrix is not optional and must come first.**

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

**Why this works:** the bugs that internal review keeps missing are configuration-interaction bugs (PR #445 critical: new code inside `if compactionManager != nil` touched `clusterCoordinator` — two independently-enabled subsystems). Forcing the matrix surfaces them mechanically.

**Concrete shapes to enumerate explicitly**, because they have bitten us:
- `if compactionManager != nil` + dereferencing `clusterCoordinator` (independently enabled)
- `if cfg.Cluster.Enabled` + assuming `cfg.Server.TLSEnabled` matches (independent flags)
- `if licenseClient != nil` + assuming `authManager != nil` (independent)
- New constructor inside `func main()` after `licenseClient`/`authManager`/`clusterCoordinator` initialization — each may be nil in supported modes

#### Step 2: Single deep reviewer (one agent, not four)

Spawn **one** general-purpose agent. The prompt MUST include:

1. **The configuration matrix from Step 1**, verbatim. The reviewer is told to confirm each row by tracing the code, and to flag any row that's wrong.
2. **The diff to review** (paste or reference `git diff main..HEAD`).
3. **Five specific things to check, in order**:
   - **(a) Outer-block precondition trace** — for every new pointer dereference, what guarantees non-nil under the configurations in the matrix? Flag any deref that doesn't have a cited establishment line. This is the highest-yield check; do it FIRST.
   - **(b) OSS standalone smoke test** — does the new code execute in OSS (no cluster, no license)? If yes, is every cluster/license-only field gated? If no, does the gate let `main.go` complete startup cleanly?
   - **(c) Failure modes that the matrix surfaces** — for each "yes" row, what does a partial failure look like? What state does the system land in if step N fails after step N-1 succeeded?
   - **(d) Doc-vs-code drift** — release notes claims, comments above the affected functions, doc-comments on exported helpers. Does every prose claim match the code that just shipped?
   - **(e) Hot-path nits** — string concat in loops, `fmt.Sprintf` where concat works, `http.DefaultClient` where a tuned client exists nearby, missing `defer Close()`. Brief pass; this is the cheap section.

4. **Output format constraint**: Blockers / High / Medium / Style. For each finding, cite file:line. **For Blockers and High, cite which row of the configuration matrix the finding falls into** — this proves the reviewer actually traced rather than pattern-matched.

5. **"Don't be deferential"** directive remains.

#### What NOT to do

- **Do not run four parallel agents** with overlapping prompts. Past data: they find ~the same things and miss the same things. One deep agent with the matrix is cheaper and more effective.
- **Do not prompt the reviewer with a long list of "Gemini past findings"**. That trains shallow-broad pattern matching. The matrix forces deep-narrow tracing.
- **Do not skip the matrix** because "it's a small diff". The bugs we miss are exactly in small diffs touching wiring code.

#### When to use additional reviewers

- **Cluster operations with Raft/manifest interactions**: add a second agent focused specifically on the [Cluster Operations Checklist](#cluster-operations-checklist) below. Its job is the manifest-before-storage / batch-not-per-file / abort-on-quorum-loss patterns.
- **Security-relevant changes (auth, HMAC, TLS, SQL, file paths)**: add a second agent focused on the [Security Checklist](#security-checklist) below. Its job is auth wiring, parameterized queries, path traversal, CSRF surfaces.
- **Skip the additional reviewer** for changes that don't touch those domains. Most PRs need just the single deep reviewer + the matrix.

#### Release hygiene (carry-over, unchanged)

- Update release notes and (for enterprise features) the enterprise-product-definition file at implementation time.
- **Re-update both after each post-review fix-up commit** — do not let docs drift behind the code across review rounds. The PR #445 round-2 Gemini finding was exactly this shape (release notes claimed a fix the patch didn't include).

### Review Loop Discipline

When the deep reviewer finds issues, address all of them in a single follow-up commit BEFORE asking Gemini for review. Don't ship intermediate commits to Gemini that the internal review already caught.

If Gemini flags items the internal review missed:

1. **First, update the configuration matrix to include the row Gemini found.** This is the durable artifact — next PR's matrix should learn from this one.
2. Then fix the finding.
3. Do NOT re-spawn the internal review agent on the same diff. The matrix update is what prevents the next miss; another review pass on the same code is wasted tokens.

**Honest expectations**: Gemini will catch things. The goal of internal review is not zero Gemini findings; it's zero **embarrassing** Gemini findings — nil-derefs, OSS-mode panics, doc-vs-code drift, missing auth. If Gemini's findings are all genuinely subtle (cross-endpoint MAC replay, NUL-delimiter ambiguity), internal review did its job.

## Cluster Operations Checklist

When writing any operation that mutates storage or cluster state in cluster mode:

1. **Writer-only execution for scheduled jobs** — retention, CQ, and similar schedulers must gate on `IsPrimaryWriter()` checked at every tick (not at `Start()`), so failover and demotion take effect without a restart. Use the `ClusterGate` interface pattern from `internal/compaction/scheduler.go` as the reference.

2. **Manifest-before-storage ordering** — always update the Raft manifest *before* deleting from storage. If the manifest update fails, the file still exists in both places and the next run retries it. The reverse order creates permanent orphan manifest entries with no retry path.

3. **Abort on manifest failure** — a Raft quorum loss is not transient. If `BatchFileOpsInManifest` fails, stop the current operation and return an error. Do not continue processing further chunks.

4. **Batch Raft proposals, never per-file** — use `BatchFileOpsInManifest` (not `DeleteFileFromManifest` in a loop). Chunk at 1000 ops max to avoid oversized Raft log entries. Interleave manifest update and storage delete per chunk so a mid-run failure limits orphan blast radius to one chunk.

5. **No self-heal assumptions** — there is no automatic manifest-vs-storage reconciliation today (Phase 5). Do not write comments claiming orphaned entries will "self-heal". Log at `Error` level and note that Phase 5 reconciliation will clean them up.

6. **DuckDB `read_parquet()` paths must be escaped** — DuckDB does not support parameterized `read_parquet()` calls. Always escape single quotes in interpolated paths: `strings.ReplaceAll(path, "'", "''")`.

## Security Checklist

When adding or modifying any API endpoint, verify ALL of the following:

1. **Auth middleware is present** — check `RegisterRoutes` has `auth.RequireAdmin` (or appropriate auth level)
2. **Auth is wired in `main.go`** — `authManager` is passed to the handler constructor
3. **User input is validated** — WHERE clauses, SQL fragments, file paths, database/measurement names
4. **No SQL interpolation** — use parameterized queries for SQLite; for DuckDB `read_parquet()` WHERE clauses (which can't use parameters), escape single quotes (`strings.ReplaceAll(path, "'", "''")`)
5. **Temp files/dirs use restrictive permissions** — `0700` for directories, `0600` for files containing data
6. **Large data is streamed, not buffered** — use `WriteReader` for S3/Azure uploads, never `os.ReadFile` on potentially large files

## Common Pitfalls

- Path traversal: always validate database/measurement names extracted from file paths (no `..`, no `/\`)
- SQLite file permissions: should be `0600` (contains auth tokens, audit logs)
- `sync.RWMutex`: two concurrent `RLock`s don't deadlock (safe for read-only getters)
- Fiber middleware ordering matters: auth before license before handlers
- When wiring new features in `cmd/arc/main.go`, check that `licenseClient`, `authManager`, `clusterCoordinator`, `compactionManager` may EACH be nil — **and they are independently enabled**. `if compactionManager != nil` does NOT imply `clusterCoordinator != nil`; OSS deployments run compaction without cluster. New code inside one nil-check that dereferences another subsystem must add its own guard.
- Missing auth is a **critical vulnerability** — never skip it, even on "internal" endpoints
- Cluster gate checks belong in the execution path (e.g. `runRetention()`), not in `Start()` — role changes must take effect without restart
- **TLS config flags are NOT one thing**: `cfg.Server.TLSEnabled` gates the Fiber HTTP listener (the public API + every cluster-internal HTTP endpoint mounted on it); `cfg.Cluster.TLSEnabled` gates Raft RPC + raw-TCP peer-fetch. An operator can enable either independently. Inter-node HTTP URL scheme must key off `cfg.Server.TLSEnabled` (what peers actually serve); inter-node HTTP TLS verification config keys off `cfg.Cluster.TLSEnabled` (private cluster CA, if configured).
- **Reverse-proxy URL building**: when forwarding an `*http.Request` to another node, use `originalReq.URL.EscapedPath()`, not `.Path`. `URL.Path` is the *decoded* form; paths containing spaces, percent-encoded bytes, or non-ASCII characters land malformed at the peer.
- **Inter-node HTTP clients**: never use `http.DefaultClient` for cluster fan-out. Construct via `security.NewClusterHTTPTransport(coordinator.ClusterTLSConfig())` so the cluster TLS config (if any) is shared with the cluster-internal HTTP path, and pool defaults match the rest of `internal/cluster/`.

## DeepSeek-specific notes

DeepSeek is action-oriented and will skip process steps unless explicitly constrained.
When using DeepSeek for a task:

1. **Prompt with explicit gates.** Start with: "Follow CLAUDE.MD — branch, fix,
   release notes, config matrix, deep review, Gemini, merge. Do not skip steps."
2. **It will jump to code first.** That's fine — redirect it to branch/process
   after the first correction. It learns within the session.
3. **Descriptive output.** Ask for full commit messages, PR bodies, and matrix
   tables explicitly — the default is terse.
4. **One issue per chat.** Token budget matters; DeepSeek sessions benefit from
   clean starts with no residual state from previous tasks.
5. **Curl over `gh` for API calls.** DeepSeek defaults to `gh` CLI for GitHub
   operations; prefer explicit `curl` commands against the GitHub API for
   comments/reviews to avoid interactive auth prompts.
