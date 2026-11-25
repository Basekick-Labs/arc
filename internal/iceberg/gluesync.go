package iceberg

// Glue sync (#1690, option B): one AWS Glue table per published Arc table,
// pointing at the same v<N>.metadata.json the REST catalog serves
// (LoadPublished). It is a pointer swap: Arc keeps writing its own metadata,
// Glue stores metadata_location, and nothing in Glue is a source of truth for
// Arc. Athena, EMR, Redshift Spectrum, Snowflake (CATALOG_SOURCE = GLUE) and
// the engines that federate Glue read the tables from there.
//
// Rules, each from the plan's adversarial review:
//
//   - Driven by publishes. The exporter reports every successful hint write
//     (SetOnPublished); the tables are coalesced and synced at once, so Glue
//     usually lags a publish by seconds. While Glue is unreachable, or
//     during a long full pass, it can name a version the prune has since
//     deleted; readers then fail until the next sync. A timer retries
//     failures; an hourly full pass verifies everything and finds deletions.
//   - Arc only moves pointers it set. A Glue table is Arc's when it carries
//     arc.managed and this warehouse's arc.warehouse; its metadata_location is
//     Arc's when it equals arc.synced_location and is a v<N>.metadata.json in
//     one of the table's standard directories. Anything else was advanced by
//     another writer (Athena, Spark), and Arc leaves it and says so.
//   - Never backwards: a stale exporter (a lease handover) does not move a
//     pointer from v<M> to an older v<N> of the same directory while v<M>
//     exists. Every write re-checks the gate and re-reads the published
//     version first.
//   - Deletion is driven from the Glue side, so tables of a dropped table or
//     database are found after a restart, and is conservative: only a table
//     Arc owns by every rule above (its pointer included), living under the
//     Glue name its recorded identity maps to, recorded under the current
//     namespace prefix, absent from a complete listing and not found by
//     LoadPublished on two consecutive complete full passes at least
//     glueDeleteGap apart, and still all of that when re-read right before
//     the delete. DeleteDatabase is never called.
//   - UpdateTable sends SkipArchive: Glue's table-version quotas are per
//     account, and a version per publish would exhaust them.
//   - Glue failures never block the export: the sync is a separate loop,
//     Notify never blocks, errors are retried by the timer.

import (
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/smithy-go"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// GlueAPI is the subset of *glue.Client the sync calls.
type GlueAPI interface {
	GetDatabase(ctx context.Context, in *glue.GetDatabaseInput, opts ...func(*glue.Options)) (*glue.GetDatabaseOutput, error)
	CreateDatabase(ctx context.Context, in *glue.CreateDatabaseInput, opts ...func(*glue.Options)) (*glue.CreateDatabaseOutput, error)
	GetDatabases(ctx context.Context, in *glue.GetDatabasesInput, opts ...func(*glue.Options)) (*glue.GetDatabasesOutput, error)
	GetTable(ctx context.Context, in *glue.GetTableInput, opts ...func(*glue.Options)) (*glue.GetTableOutput, error)
	GetTables(ctx context.Context, in *glue.GetTablesInput, opts ...func(*glue.Options)) (*glue.GetTablesOutput, error)
	CreateTable(ctx context.Context, in *glue.CreateTableInput, opts ...func(*glue.Options)) (*glue.CreateTableOutput, error)
	UpdateTable(ctx context.Context, in *glue.UpdateTableInput, opts ...func(*glue.Options)) (*glue.UpdateTableOutput, error)
	DeleteTable(ctx context.Context, in *glue.DeleteTableInput, opts ...func(*glue.Options)) (*glue.DeleteTableOutput, error)
}

// Glue table and database parameters the sync reads and writes.
const (
	glueParamTableType      = "table_type"
	glueParamMetadata       = "metadata_location"
	glueParamPrevMetadata   = "previous_metadata_location"
	glueParamManaged        = "arc.managed"
	glueParamWarehouse      = "arc.warehouse"
	glueParamDatabase       = "arc.database"
	glueParamMeasurement    = "arc.measurement"
	glueParamPrefix         = "arc.namespace_prefix"
	glueParamSyncedLocation = "arc.synced_location"
	glueTableTypeIceberg    = "ICEBERG"
	glueExternalTable       = "EXTERNAL_TABLE"
	glueNameMax             = 255
)

const (
	// glueFullEvery is how often every table is verified against Glue and
	// deletions are looked for.
	glueFullEvery = time.Hour
	// glueDeleteGap is how far apart the two full passes that find a table
	// absent must be before its Glue table is deleted.
	glueDeleteGap = 10 * time.Minute
	// glueParallel bounds Glue calls in flight: Glue quotas are account-wide
	// and shared with the customer's Athena.
	glueParallel = 4
	// glueRefusedMax bounds the refusal memo; past it a refusal is logged on
	// every pass rather than remembered.
	glueRefusedMax = 10000
	// glueCallTimeout bounds one table's sync and every other Glue or
	// storage call of a pass: the loop is the only one, and a stalled call
	// must not stop Glue following publishes.
	glueCallTimeout = 30 * time.Second
)

var vMetadataName = regexp.MustCompile(`^v([0-9]+)\.metadata\.json$`)

// GlueSyncConfig configures NewGlueSync.
type GlueSyncConfig struct {
	Exporter  *Exporter
	API       GlueAPI
	CatalogID string        // "" = the caller's account
	Interval  time.Duration // retry timer
	Gate      WriterGate    // nil: always runs (standalone)
	Logger    zerolog.Logger
}

// GlueSync keeps Glue in step with the published tables.
type GlueSync struct {
	exp       *Exporter
	api       GlueAPI
	catalogID *string
	interval  time.Duration
	gate      WriterGate
	warehouse string
	logger    zerolog.Logger
	now       func() time.Time

	signal chan struct{}
	cancel context.CancelFunc
	done   chan struct{}

	mu     sync.Mutex
	dirty  map[glueKey]struct{}
	failed map[glueKey]struct{}
	// synced is the location last written or confirmed per table; it only
	// spares Glue calls, and is dropped on any doubt.
	synced map[glueKey]string
	// absentSince is when a full pass first found an Arc-managed Glue table's
	// Arc table absent.
	absentSince map[glueKey]time.Time
	// refused records refusals already logged at Error.
	refused map[string]struct{}
	// listArc lists the published tables (arcTables); tests replace it.
	listArc func(ctx context.Context) (map[glueKey]struct{}, bool)
}

type glueKey struct{ database, measurement string }

// NewGlueSync builds the sync. It does nothing until Start.
func NewGlueSync(cfg GlueSyncConfig) *GlueSync {
	g := &GlueSync{
		exp:         cfg.Exporter,
		api:         cfg.API,
		interval:    cfg.Interval,
		gate:        cfg.Gate,
		warehouse:   normalizeWarehouse(cfg.Exporter.warehouse),
		logger:      cfg.Logger.With().Str("component", "iceberg-glue").Logger(),
		now:         time.Now,
		signal:      make(chan struct{}, 1),
		dirty:       map[glueKey]struct{}{},
		failed:      map[glueKey]struct{}{},
		synced:      map[glueKey]string{},
		absentSince: map[glueKey]time.Time{},
		refused:     map[string]struct{}{},
	}
	g.listArc = g.arcTables
	if cfg.CatalogID != "" {
		g.catalogID = aws.String(cfg.CatalogID)
	}
	if g.interval <= 0 {
		g.interval = time.Minute
	}
	return g
}

func normalizeWarehouse(w string) string { return strings.TrimSuffix(strings.TrimSpace(w), "/") }

// SetOnPublished sets the exporter's publish callback. Call it during
// wiring, before the reconcile scheduler starts; f must not block.
func (e *Exporter) SetOnPublished(f func(database, measurement string)) { e.onPublished = f }

// Notify marks a table published. It never blocks: the exporter calls it
// from its commit path.
func (g *GlueSync) Notify(database, measurement string) {
	g.mu.Lock()
	g.dirty[glueKey{database, measurement}] = struct{}{}
	g.mu.Unlock()
	select {
	case g.signal <- struct{}{}:
	default:
	}
}

// Start runs the sync loop.
func (g *GlueSync) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	g.done = make(chan struct{})
	go g.loop(ctx)
}

// Stop cancels in-flight Glue calls and waits for the loop to end.
func (g *GlueSync) Stop() {
	if g.cancel == nil {
		return
	}
	g.cancel()
	<-g.done
}

func (g *GlueSync) canRun() bool { return g.gate == nil || g.gate.CanRun() }

func (g *GlueSync) loop(ctx context.Context) {
	defer close(g.done)
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()
	wasOpen := false
	var fullDue time.Time
	for {
		if g.canRun() {
			if !wasOpen {
				// Another node may have synced while this one could not: forget
				// what this one believed and verify everything.
				g.dropCache()
				fullDue = time.Time{}
				wasOpen = true
			}
			if !g.now().Before(fullDue) {
				g.fullPass(ctx)
				fullDue = g.now().Add(glueFullEvery)
			} else {
				g.dirtyPass(ctx)
			}
		} else {
			wasOpen = false
		}
		select {
		case <-ctx.Done():
			return
		case <-g.signal:
		case <-ticker.C:
		}
	}
}

func (g *GlueSync) dropCache() {
	g.mu.Lock()
	g.synced = map[glueKey]string{}
	g.mu.Unlock()
}

// dirtyPass syncs the tables published or failed since the last pass.
func (g *GlueSync) dirtyPass(ctx context.Context) {
	g.mu.Lock()
	work := make([]glueKey, 0, len(g.dirty)+len(g.failed))
	for k := range g.dirty {
		work = append(work, k)
	}
	for k := range g.failed {
		if _, ok := g.dirty[k]; !ok {
			work = append(work, k)
		}
	}
	g.dirty = map[glueKey]struct{}{}
	g.failed = map[glueKey]struct{}{}
	g.mu.Unlock()
	g.syncAll(ctx, work, nil)
}

// syncAll syncs tables with at most glueParallel in flight. known, when set,
// holds the Glue state a full pass already read (absent key: no Glue table).
// A throttled call ends the pass; the rest is retried by the timer.
func (g *GlueSync) syncAll(ctx context.Context, work []glueKey, known map[glueKey]*gluetypes.Table) {
	sort.Slice(work, func(i, j int) bool {
		if work[i].database != work[j].database {
			return work[i].database < work[j].database
		}
		return work[i].measurement < work[j].measurement
	})
	var (
		wg        sync.WaitGroup
		sem       = make(chan struct{}, glueParallel)
		throttled sync.Once
		stop      = make(chan struct{})
	)
	for _, k := range work {
		select {
		case <-stop:
			g.markFailed(k)
			continue
		case <-ctx.Done():
			g.markFailed(k)
			continue
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			var cur *gluetypes.Table
			fromFull := known != nil
			if fromFull {
				cur = known[k]
			}
			tctx, cancel := context.WithTimeout(ctx, glueCallTimeout)
			err := g.syncTable(tctx, k, cur, fromFull)
			cancel()
			if err != nil {
				g.markFailed(k)
				if isGlueThrottle(err) {
					throttled.Do(func() {
						g.logger.Warn().Err(err).Msg("Glue throttled the sync; this pass stops, the rest is retried")
						close(stop)
					})
					return
				}
				g.logger.Warn().Err(err).Str("database", k.database).Str("measurement", k.measurement).
					Msg("Glue sync failed for the table; retried on the next pass")
			}
		}()
	}
	wg.Wait()
}

func (g *GlueSync) markFailed(k glueKey) {
	g.mu.Lock()
	g.failed[k] = struct{}{}
	delete(g.synced, k)
	g.mu.Unlock()
}

// refuseOnce logs a refusal at Error the first time this process sees it.
func (g *GlueSync) refuseOnce(key string, msg func() string) {
	g.mu.Lock()
	_, seen := g.refused[key]
	if !seen && len(g.refused) < glueRefusedMax {
		g.refused[key] = struct{}{}
	}
	g.mu.Unlock()
	if !seen {
		g.logger.Error().Msg(msg())
	}
}

// GlueName maps an Arc name to a Glue/Athena name: lowercase, with every
// character outside [a-z0-9_] turned into '_'. ok is false when the result
// is empty or longer than Glue allows.
func GlueName(s string) (string, bool) {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	out := b.String()
	return out, out != "" && len(out) <= glueNameMax
}

func (g *GlueSync) glueNames(k glueKey) (db, table string, ok bool) {
	db, ok1 := GlueName(g.exp.nsPrefix + "_" + k.database)
	table, ok2 := GlueName(k.measurement)
	return db, table, ok1 && ok2
}

// syncTable brings one table's Glue entry to its published version. cur is
// the Glue table when fromFull (nil: none); otherwise it is read here.
func (g *GlueSync) syncTable(ctx context.Context, k glueKey, cur *gluetypes.Table, fromFull bool) error {
	if !g.canRun() {
		return nil
	}
	pub, found, err := g.exp.LoadPublished(ctx, k.database, k.measurement)
	if err != nil {
		return err
	}
	if !found {
		return nil // deletion is the full pass's decision
	}
	gdb, gtable, ok := g.glueNames(k)
	if !ok {
		g.refuseOnce("name\x00"+k.database+"\x00"+k.measurement, func() string {
			return fmt.Sprintf("Glue sync: %s.%s has no valid Glue name (empty or over %d characters); it is not registered", k.database, k.measurement, glueNameMax)
		})
		return nil
	}
	g.mu.Lock()
	cached := g.synced[k]
	g.mu.Unlock()
	if !fromFull && cached == pub.MetadataLocation {
		return nil
	}
	if !fromFull {
		out, err := g.api.GetTable(ctx, &glue.GetTableInput{CatalogId: g.catalogID, DatabaseName: aws.String(gdb), Name: aws.String(gtable)})
		switch {
		case isGlueNotFound(err):
			cur = nil
		case err != nil:
			return fmt.Errorf("get Glue table %s.%s: %w", gdb, gtable, err)
		default:
			cur = out.Table
		}
	}
	parse := func() (icetable.Metadata, error) {
		meta, err := icetable.ParseMetadataBytes(pub.Metadata)
		if err != nil {
			return nil, fmt.Errorf("parse published metadata of %s.%s: %w", k.database, k.measurement, err)
		}
		return meta, nil
	}

	if cur == nil {
		meta, err := parse()
		if err != nil {
			return err
		}
		if err := g.ensureDatabase(ctx, gdb); err != nil {
			return err
		}
		if !g.canRun() {
			return nil
		}
		in := g.tableInput(gtable, k, meta, pub.MetadataLocation, "", nil)
		_, err = g.api.CreateTable(ctx, &glue.CreateTableInput{CatalogId: g.catalogID, DatabaseName: aws.String(gdb), TableInput: in})
		if err != nil {
			if isGlueAlreadyExists(err) {
				g.dropCache()
			}
			return fmt.Errorf("create Glue table %s.%s: %w", gdb, gtable, err)
		}
		g.noteSynced(k, pub.MetadataLocation)
		g.logger.Info().Str("glue_table", gdb+"."+gtable).Str("metadata", pub.MetadataLocation).Msg("Registered the Iceberg table in Glue")
		return nil
	}

	params := cur.Parameters
	if !g.ownsTable(cur) {
		g.refuseOnce("foreign\x00"+gdb+"\x00"+gtable, func() string {
			return fmt.Sprintf("Glue sync: Glue table %s.%s exists and was not created by this Arc warehouse (%s); Arc leaves it alone and does not register %s.%s. "+
				"Drop or rename the Glue table to let Arc register it (after an iceberg.warehouse change, every table Arc created "+
				"under the old warehouse is in this state)", gdb, gtable, g.warehouse, k.database, k.measurement)
		})
		return nil
	}
	if params[glueParamDatabase] != k.database || params[glueParamMeasurement] != k.measurement {
		g.refuseOnce("collide\x00"+gdb+"\x00"+gtable+"\x00"+k.database+"\x00"+k.measurement, func() string {
			return fmt.Sprintf("Glue sync: %s.%s maps to Glue table %s.%s, which Arc already uses for %s.%s; it is not registered (Glue names are lowercase, with other characters as '_')",
				k.database, k.measurement, gdb, gtable, params[glueParamDatabase], params[glueParamMeasurement])
		})
		return nil
	}
	curLoc := params[glueParamMetadata]
	if curLoc == pub.MetadataLocation {
		g.noteSynced(k, curLoc)
		return nil
	}
	if !g.pointerIsArcs(k, cur) {
		g.refuseOnce("advanced\x00"+gdb+"\x00"+gtable, func() string {
			return fmt.Sprintf("Glue sync: Glue table %s.%s names %s, which Arc did not set: another writer committed to it. Arc no longer updates it; "+
				"Arc is the only writer of its tables, so drop the Glue table to let Arc register it again", gdb, gtable, curLoc)
		})
		return nil
	}
	if back, err := g.movesBackwards(ctx, curLoc, pub.MetadataLocation); err != nil {
		return err
	} else if back {
		g.logger.Warn().Str("glue_table", gdb+"."+gtable).Str("glue", curLoc).Str("published_here", pub.MetadataLocation).
			Msg("Glue already names a newer version of this table than this node read; not moving it back")
		return nil
	}
	if !g.canRun() {
		return nil
	}
	// The listing a full pass read may lack VersionId; never update without
	// the optimistic lock (iceberg-go refuses the same case).
	if cur.VersionId == nil {
		out, err := g.api.GetTable(ctx, &glue.GetTableInput{CatalogId: g.catalogID, DatabaseName: aws.String(gdb), Name: aws.String(gtable)})
		if err != nil {
			return fmt.Errorf("get Glue table %s.%s: %w", gdb, gtable, err)
		}
		if out.Table == nil || out.Table.VersionId == nil {
			return fmt.Errorf("Glue table %s.%s has no version id; not updated without one", gdb, gtable)
		}
		if out.Table.Parameters[glueParamMetadata] != curLoc {
			return fmt.Errorf("Glue table %s.%s changed while it was being synced; retried", gdb, gtable)
		}
		cur = out.Table
	}
	meta, err := parse()
	if err != nil {
		return err
	}
	in := g.tableInput(gtable, k, meta, pub.MetadataLocation, curLoc, cur)
	_, err = g.api.UpdateTable(ctx, &glue.UpdateTableInput{
		CatalogId: g.catalogID, DatabaseName: aws.String(gdb), TableInput: in,
		VersionId: cur.VersionId, SkipArchive: aws.Bool(true),
	})
	if err != nil {
		if isGlueConcurrent(err) {
			g.dropCache()
		}
		return fmt.Errorf("update Glue table %s.%s: %w", gdb, gtable, err)
	}
	g.noteSynced(k, pub.MetadataLocation)
	g.logger.Debug().Str("glue_table", gdb+"."+gtable).Str("metadata", pub.MetadataLocation).Msg("Moved the Glue table to the published version")
	return nil
}

func (g *GlueSync) noteSynced(k glueKey, loc string) {
	g.mu.Lock()
	g.synced[k] = loc
	g.mu.Unlock()
}

// ownsTable reports whether a Glue table was created by this warehouse's Arc.
func (g *GlueSync) ownsTable(t *gluetypes.Table) bool {
	return t != nil && t.Parameters[glueParamManaged] == "true" && normalizeWarehouse(t.Parameters[glueParamWarehouse]) == g.warehouse
}

// pointerIsArcs reports whether the table's metadata_location is the one Arc
// last set: equal to arc.synced_location and a v<N>.metadata.json in one of
// the table's standard directories. False means another writer committed.
func (g *GlueSync) pointerIsArcs(k glueKey, t *gluetypes.Table) bool {
	loc := t.Parameters[glueParamMetadata]
	return loc != "" && loc == t.Parameters[glueParamSyncedLocation] && g.arcOwnedLocation(k, loc)
}

// arcOwnedLocation reports whether loc is a v<N>.metadata.json in one of the
// table's two standard metadata directories.
func (g *GlueSync) arcOwnedLocation(k glueKey, loc string) bool {
	if !vMetadataName.MatchString(path.Base(loc)) {
		return false
	}
	_, dirs, _ := g.exp.publishedDirs(k.database, k.measurement)
	for _, d := range dirs {
		if sameLocation(uriDir(loc), d.loc) {
			return true
		}
	}
	return false
}

// movesBackwards reports whether replacing cur with next would move the
// pointer from v<M> to an older v<N> of the same directory while v<M> still
// exists (a recreated table restarts at v0, so the version alone decides
// nothing).
func (g *GlueSync) movesBackwards(ctx context.Context, cur, next string) (bool, error) {
	if !sameLocation(uriDir(cur), uriDir(next)) {
		return false, nil
	}
	m := vMetadataName.FindStringSubmatch(path.Base(cur))
	n := vMetadataName.FindStringSubmatch(path.Base(next))
	if m == nil || n == nil {
		return false, nil
	}
	mv, _ := strconv.Atoi(m[1])
	nv, _ := strconv.Atoi(n[1])
	if nv >= mv {
		return false, nil
	}
	key, ok := g.exp.warehouseRelKey(cur)
	if !ok {
		return false, nil
	}
	size, err := g.exp.backend.StatFile(ctx, key)
	if err != nil {
		return false, fmt.Errorf("check %s: %w", key, err)
	}
	return size >= 0, nil
}

func (g *GlueSync) ensureDatabase(ctx context.Context, gdb string) error {
	_, err := g.api.GetDatabase(ctx, &glue.GetDatabaseInput{CatalogId: g.catalogID, Name: aws.String(gdb)})
	if err == nil {
		return nil // an operator-created database is fine; only tables carry markers
	}
	if !isGlueNotFound(err) {
		return fmt.Errorf("get Glue database %s: %w", gdb, err)
	}
	_, err = g.api.CreateDatabase(ctx, &glue.CreateDatabaseInput{CatalogId: g.catalogID, DatabaseInput: &gluetypes.DatabaseInput{
		Name:        aws.String(gdb),
		Description: aws.String("Iceberg tables exported by Arc"),
		Parameters:  map[string]string{glueParamManaged: "true", glueParamWarehouse: g.warehouse},
	}})
	if err != nil && !isGlueAlreadyExists(err) {
		return fmt.Errorf("create Glue database %s: %w", gdb, err)
	}
	return nil
}

// tableInput builds the Glue table input the way Iceberg's own Glue catalogs
// do (iceberg-go, Java GlueCatalog), merging the previous table's parameters
// and column comments so an operator's additions survive.
func (g *GlueSync) tableInput(name string, k glueKey, meta icetable.Metadata, loc, prevLoc string, prev *gluetypes.Table) *gluetypes.TableInput {
	params := map[string]string{}
	var prevCols []gluetypes.Column
	var desc *string
	if prev != nil {
		for k, v := range prev.Parameters {
			params[k] = v
		}
		if prev.StorageDescriptor != nil {
			prevCols = prev.StorageDescriptor.Columns
		}
		desc = prev.Description
	}
	params[glueParamTableType] = glueTableTypeIceberg
	params[glueParamMetadata] = loc
	if prevLoc != "" {
		params[glueParamPrevMetadata] = prevLoc
	}
	params[glueParamManaged] = "true"
	params[glueParamWarehouse] = g.warehouse
	params[glueParamDatabase] = k.database
	params[glueParamMeasurement] = k.measurement
	params[glueParamPrefix] = g.exp.nsPrefix
	params[glueParamSyncedLocation] = loc
	return &gluetypes.TableInput{
		Name:        aws.String(name),
		TableType:   aws.String(glueExternalTable),
		Description: desc,
		Parameters:  params,
		StorageDescriptor: &gluetypes.StorageDescriptor{
			Location: aws.String(meta.Location()),
			Columns:  glueColumns(meta.CurrentSchema(), prevCols),
		},
	}
}

func glueColumns(schema *iceberg.Schema, prev []gluetypes.Column) []gluetypes.Column {
	comments := map[string]*string{}
	for _, c := range prev {
		if c.Name != nil && c.Comment != nil {
			comments[*c.Name] = c.Comment
		}
	}
	if schema == nil {
		return nil
	}
	cols := make([]gluetypes.Column, 0, len(schema.Fields()))
	for _, f := range schema.Fields() {
		c := gluetypes.Column{
			Name: aws.String(f.Name),
			Type: aws.String(glueType(f.Type)),
			Parameters: map[string]string{
				"iceberg.field.id":       strconv.Itoa(f.ID),
				"iceberg.field.optional": strconv.FormatBool(!f.Required),
				"iceberg.field.current":  "true",
			},
		}
		if f.Doc != "" {
			c.Comment = aws.String(f.Doc)
		} else if cm, ok := comments[f.Name]; ok {
			c.Comment = cm
		}
		cols = append(cols, c)
	}
	return cols
}

// glueType is the Hive type name Glue and Athena show for an Iceberg type.
func glueType(t iceberg.Type) string {
	switch t := t.(type) {
	case iceberg.BooleanType:
		return "boolean"
	case iceberg.Int32Type:
		return "int"
	case iceberg.Int64Type:
		return "bigint"
	case iceberg.Float32Type:
		return "float"
	case iceberg.Float64Type:
		return "double"
	case iceberg.DateType:
		return "date"
	case iceberg.TimestampType, iceberg.TimestampTzType:
		return "timestamp"
	case iceberg.BinaryType, iceberg.FixedType:
		return "binary"
	case iceberg.DecimalType:
		return fmt.Sprintf("decimal(%d,%d)", t.Precision(), t.Scale())
	case *iceberg.StructType:
		parts := make([]string, 0, len(t.Fields()))
		for _, f := range t.Fields() {
			parts = append(parts, f.Name+":"+glueType(f.Type))
		}
		return "struct<" + strings.Join(parts, ",") + ">"
	case *iceberg.ListType:
		return "array<" + glueType(t.ElementField().Type) + ">"
	case *iceberg.MapType:
		return "map<" + glueType(t.KeyField().Type) + "," + glueType(t.ValueField().Type) + ">"
	default:
		return "string"
	}
}

// fullPass syncs every published table from one read of Glue, and deletes
// the Glue tables of Arc tables confirmed gone.
func (g *GlueSync) fullPass(ctx context.Context) {
	arcTables, listingComplete := g.listArc(ctx)
	ours, all, err := g.glueTables(ctx)
	if err != nil {
		g.logger.Warn().Err(err).Msg("Glue sync could not list Glue's tables; the published tables are retried on the timer, deletions wait for the next full pass")
		g.mu.Lock()
		for k := range arcTables {
			g.failed[k] = struct{}{}
		}
		g.absentSince = map[glueKey]time.Time{} // strikes must come from consecutive complete passes
		g.mu.Unlock()
		return
	}
	work := make([]glueKey, 0, len(arcTables))
	known := map[glueKey]*gluetypes.Table{}
	for k := range arcTables {
		work = append(work, k)
		if gdb, gt, ok := g.glueNames(k); ok {
			if t, ok := all[gdb+"\x00"+gt]; ok {
				known[k] = t
			}
		}
	}
	g.syncAll(ctx, work, known)
	if !listingComplete {
		g.mu.Lock()
		g.absentSince = map[glueKey]time.Time{}
		g.mu.Unlock()
		return
	}
	g.deleteAbsent(ctx, ours, arcTables)
}

// arcTables lists the published tables. complete is false when any database
// could not be listed: deletions then wait for a complete listing.
func (g *GlueSync) arcTables(ctx context.Context) (map[glueKey]struct{}, bool) {
	out := map[glueKey]struct{}{}
	lctx, cancel := context.WithTimeout(ctx, glueCallTimeout)
	dbs, err := g.exp.PublishedDatabases(lctx)
	cancel()
	if err != nil {
		g.logger.Warn().Err(err).Msg("Glue sync could not list the published databases; no Glue table is deleted this pass")
		return out, false
	}
	complete := true
	for _, db := range dbs {
		tctx, cancel := context.WithTimeout(ctx, glueCallTimeout)
		tables, err := g.exp.PublishedTables(tctx, db, 0)
		cancel()
		if err != nil {
			g.logger.Warn().Err(err).Str("database", db).Msg("Glue sync could not list a database's published tables; no Glue table is deleted this pass")
			complete = false
			continue
		}
		for _, m := range tables {
			out[glueKey{db, m}] = struct{}{}
		}
	}
	return out, complete
}

// glueTables reads the Glue databases named with this exporter's prefix.
// ours are this warehouse's Arc-managed tables by Arc identity; all are every
// table in those databases by Glue name.
func (g *GlueSync) glueTables(ctx context.Context) (ours map[glueKey]glueEntry, all map[string]*gluetypes.Table, err error) {
	ours, all = map[glueKey]glueEntry{}, map[string]*gluetypes.Table{}
	prefix, _ := GlueName(g.exp.nsPrefix + "_")
	var token *string
	for {
		pctx, cancel := context.WithTimeout(ctx, glueCallTimeout)
		out, err := g.api.GetDatabases(pctx, &glue.GetDatabasesInput{CatalogId: g.catalogID, NextToken: token})
		cancel()
		if err != nil {
			return nil, nil, fmt.Errorf("list Glue databases: %w", err)
		}
		for _, d := range out.DatabaseList {
			if d.Name == nil || !strings.HasPrefix(*d.Name, prefix) {
				continue
			}
			if err := g.glueTablesIn(ctx, *d.Name, ours, all); err != nil {
				return nil, nil, err
			}
		}
		if out.NextToken == nil || *out.NextToken == "" {
			return ours, all, nil
		}
		token = out.NextToken
	}
}

type glueEntry struct {
	database, table string
	params          map[string]string
	tbl             *gluetypes.Table
}

func (g *GlueSync) glueTablesIn(ctx context.Context, gdb string, ours map[glueKey]glueEntry, all map[string]*gluetypes.Table) error {
	var token *string
	for {
		pctx, cancel := context.WithTimeout(ctx, glueCallTimeout)
		out, err := g.api.GetTables(pctx, &glue.GetTablesInput{CatalogId: g.catalogID, DatabaseName: aws.String(gdb), NextToken: token})
		cancel()
		if err != nil {
			return fmt.Errorf("list Glue tables of %s: %w", gdb, err)
		}
		for i := range out.TableList {
			t := &out.TableList[i]
			if t.Name == nil {
				continue
			}
			all[gdb+"\x00"+*t.Name] = t
			p := t.Parameters
			if !g.ownsTable(t) || p[glueParamDatabase] == "" || p[glueParamMeasurement] == "" {
				continue
			}
			k := glueKey{p[glueParamDatabase], p[glueParamMeasurement]}
			// Only the table living under the Glue name its identity maps to
			// is Arc's: a copy carrying the parameters (Glue "copy table") is
			// someone else's.
			if wdb, wt, ok := g.glueNames(k); !ok || wdb != gdb || wt != *t.Name {
				continue
			}
			ours[k] = glueEntry{database: gdb, table: *t.Name, params: p, tbl: t}
		}
		if out.NextToken == nil || *out.NextToken == "" {
			return nil
		}
		token = out.NextToken
	}
}

// deleteAbsent deletes the Glue tables of Arc tables found absent on two full
// passes glueDeleteGap apart.
func (g *GlueSync) deleteAbsent(ctx context.Context, ours map[glueKey]glueEntry, arcTables map[glueKey]struct{}) {
	now := g.now()
	seen := map[glueKey]struct{}{}
	for k, e := range ours {
		if _, present := arcTables[k]; present {
			continue
		}
		if e.params[glueParamPrefix] != g.exp.nsPrefix {
			continue // recorded under another namespace prefix: not this listing's to judge
		}
		if !g.pointerIsArcs(k, e.tbl) {
			continue // another writer committed to it: theirs now
		}
		lctx, cancel := context.WithTimeout(ctx, glueCallTimeout)
		_, found, err := g.exp.LoadPublished(lctx, k.database, k.measurement)
		cancel()
		if err != nil || found {
			continue // not confirmed absent
		}
		seen[k] = struct{}{}
		g.mu.Lock()
		first, ok := g.absentSince[k]
		if !ok {
			g.absentSince[k] = now
		}
		g.mu.Unlock()
		if !ok || now.Sub(first) < glueDeleteGap || !g.canRun() {
			continue
		}
		// Re-read: the listing is a snapshot from before this pass's syncs,
		// and DeleteTable has no version condition.
		dctx, cancel := context.WithTimeout(ctx, glueCallTimeout)
		now, err := g.api.GetTable(dctx, &glue.GetTableInput{CatalogId: g.catalogID, DatabaseName: aws.String(e.database), Name: aws.String(e.table)})
		if err != nil || now.Table == nil || !g.ownsTable(now.Table) || !g.pointerIsArcs(k, now.Table) ||
			now.Table.Parameters[glueParamDatabase] != k.database || now.Table.Parameters[glueParamMeasurement] != k.measurement {
			cancel()
			continue
		}
		_, err = g.api.DeleteTable(dctx, &glue.DeleteTableInput{CatalogId: g.catalogID, DatabaseName: aws.String(e.database), Name: aws.String(e.table)})
		cancel()
		if err != nil && !isGlueNotFound(err) {
			g.logger.Warn().Err(err).Str("glue_table", e.database+"."+e.table).Msg("Glue sync could not delete the Glue table of a dropped Arc table; retried on the next full pass")
			continue
		}
		g.logger.Info().Str("glue_table", e.database+"."+e.table).Str("database", k.database).Str("measurement", k.measurement).
			Msg("Deleted the Glue table of an Arc table that no longer exists (metadata only; Glue never deletes data)")
		g.mu.Lock()
		delete(g.absentSince, k)
		delete(g.synced, k)
		g.mu.Unlock()
	}
	// A table seen again, or no longer Arc's, starts over.
	g.mu.Lock()
	for k := range g.absentSince {
		if _, ok := seen[k]; !ok {
			delete(g.absentSince, k)
		}
	}
	g.mu.Unlock()
}

func glueErrorCode(err error) string {
	var api smithy.APIError
	if errors.As(err, &api) {
		return api.ErrorCode()
	}
	return ""
}

func isGlueNotFound(err error) bool {
	var nf *gluetypes.EntityNotFoundException
	return err != nil && (errors.As(err, &nf) || glueErrorCode(err) == "EntityNotFoundException")
}

func isGlueAlreadyExists(err error) bool {
	var ae *gluetypes.AlreadyExistsException
	return err != nil && (errors.As(err, &ae) || glueErrorCode(err) == "AlreadyExistsException")
}

func isGlueConcurrent(err error) bool {
	var cm *gluetypes.ConcurrentModificationException
	return err != nil && (errors.As(err, &cm) || glueErrorCode(err) == "ConcurrentModificationException")
}

func isGlueThrottle(err error) bool {
	switch glueErrorCode(err) {
	case "ThrottlingException", "Throttling", "TooManyRequestsException", "RequestLimitExceeded":
		return true
	}
	return false
}

// GlueRegion is the region the Glue client uses: region if set, else the S3
// backend's.
func GlueRegion(backend storage.Backend, region string) string {
	if region != "" {
		return region
	}
	if sb, ok := backend.(s3Settings); ok {
		return sb.ClientSettings().Region
	}
	return ""
}

// NewGlueClient builds a Glue client from the S3 backend's credentials (static
// keys or the default chain, renewed through the same cache) in region. Any
// endpoint the AWS config carries (AWS_ENDPOINT_URL, a profile's
// endpoint_url) is dropped: Glue always goes to AWS. Retries use the SDK's
// adaptive mode, which backs off under throttling.
func NewGlueClient(backend storage.Backend, region string) (*glue.Client, error) {
	cfg, _ := awsConfigOf(backend)
	if cfg == nil {
		return nil, fmt.Errorf("Glue sync needs the S3 storage backend")
	}
	c := cfg.Copy()
	c.Region = region
	c.BaseEndpoint = nil
	return glue.NewFromConfig(c, func(o *glue.Options) {
		o.BaseEndpoint = nil
		o.Retryer = retry.NewAdaptiveMode()
	}), nil
}

// uriDir is a location URI's parent, cut at its last '/'. Not path.Dir:
// that cleans the URI as a path and collapses "s3://bucket" into
// "s3:/bucket", which then compares unequal to every directory location.
func uriDir(loc string) string {
	if i := strings.LastIndex(loc, "/"); i >= 0 {
		return loc[:i]
	}
	return ""
}
