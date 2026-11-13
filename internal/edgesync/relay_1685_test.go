package edgesync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

type relayRig struct {
	hub    *importRig
	relay  *storage.LocalBackend
	poller *RelayPoller

	spokeStore *storage.LocalBackend
	ledger     *Ledger
	exporter   *Exporter
	spoke      *RelaySpoke
}

func newRelayRig(t *testing.T) *relayRig {
	t.Helper()
	hub := newImportRig(t, false)
	relay, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	poller, err := NewRelayPoller(RelayPollerConfig{
		Relay: relay, Importer: hub.importer, StagingDir: t.TempDir(), HubID: testHubID, Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}

	storeDir := t.TempDir()
	spokeStore, err := storage.NewLocalBackend(storeDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewBundleWriter(BundleWriterConfig{
		Backend: spokeStore, SpokeID: testSpokeID, HubID: testHubID, Secret: hub.secret, Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := NewDestinationPolicy([]string{t.TempDir()}, storeDir)
	if err != nil {
		t.Fatal(err)
	}
	l := setupTestLedger(t)
	disc, err := NewDiscoverer(l, spokeStore, testHubID, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ex, err := NewExporter(ExporterConfig{
		Ledger: l, Writer: w, Policy: policy, Discoverer: disc, HubID: testHubID, Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	spoke, err := NewRelaySpoke(RelaySpokeConfig{Exporter: ex, Relay: relay, AckTimeout: time.Hour, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	return &relayRig{hub: hub, relay: relay, poller: poller, spokeStore: spokeStore, ledger: l, exporter: ex, spoke: spoke}
}

func (r *relayRig) write(t *testing.T, path, content string) {
	t.Helper()
	if err := r.spokeStore.Write(context.Background(), path, []byte(content)); err != nil {
		t.Fatal(err)
	}
}

func relayPath(i int) string { return fmt.Sprintf("default/cpu/2026/08/07/14/f_%04d.parquet", i) }

func (r *relayRig) spokePass(t *testing.T) *RelaySpokeResult {
	t.Helper()
	res, err := r.spoke.Run(context.Background())
	if err != nil {
		t.Fatalf("spoke pass: %v", err)
	}
	return res
}

func (r *relayRig) poll(t *testing.T) *RelayPollResult {
	t.Helper()
	res, err := r.poller.Run(context.Background())
	if err != nil {
		t.Fatalf("hub poll: %v", err)
	}
	return res
}

func (r *relayRig) state(t *testing.T, path string) SyncState {
	t.Helper()
	e, err := r.ledger.Get(context.Background(), testHubID, path)
	if err != nil {
		t.Fatalf("get %s: %v", path, err)
	}
	return e.State
}

func (r *relayRig) keys(t *testing.T, prefix string) []string {
	t.Helper()
	k, err := r.relay.List(context.Background(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// The whole loop through a bucket: export, import, ack, apply, and the GC
// order (spoke removes the outbox after applying, hub removes the ack after).
func TestRelay_RoundTrip(t *testing.T) {
	r := newRelayRig(t)
	for i := 0; i < 3; i++ {
		r.write(t, relayPath(i), fmt.Sprintf("payload %d", i))
	}
	first := r.spokePass(t)
	if first.Exported == nil || first.Exported.FileCount != 3 {
		t.Fatalf("export = %+v", first.Exported)
	}
	id := first.Exported.BundleID
	if got := r.state(t, relayPath(0)); got != StateExported {
		t.Fatalf("after export: %s", got)
	}

	if p := r.poll(t); p.Imported != 1 {
		t.Fatalf("poll = %+v", p)
	}
	if ok, _ := r.relay.Exists(context.Background(), RelayAckKey(testSpokeID, id)); !ok {
		t.Fatal("no ack in the bucket")
	}
	// A second poll must not re-import: the ack is there.
	if p := r.poll(t); p.Imported != 0 || p.Reacked != 0 {
		t.Fatalf("second poll = %+v", p)
	}

	second := r.spokePass(t)
	if second.AcksApplied != 1 || second.Synced != 3 {
		t.Fatalf("apply = %+v", second)
	}
	for i := 0; i < 3; i++ {
		if got := r.state(t, relayPath(i)); got != StateSynced {
			t.Fatalf("%s: %s", relayPath(i), got)
		}
	}
	if k := r.keys(t, RelayOutboxDir(testSpokeID)); len(k) != 0 {
		t.Fatalf("outbox not removed: %v", k)
	}
	if p := r.poll(t); p.AcksPruned != 1 {
		t.Fatalf("ack not pruned: %+v", p)
	}
	if k := r.keys(t, RelayAckDir(testSpokeID)); len(k) != 0 {
		t.Fatalf("ack left: %v", k)
	}
}

// A partial upload (no manifest) is never imported, and the spoke's next pass
// sweeps it.
func TestRelay_PartialUploadNeverImported(t *testing.T) {
	r := newRelayRig(t)
	ctx := context.Background()
	id, _ := NewBundleID(time.Now())
	prefix := RelayOutboxPrefix(testSpokeID, id)
	if err := r.relay.Write(ctx, prefix+"data/"+relayPath(0), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := r.relay.Write(ctx, prefix+entriesName, []byte("{}\n")); err != nil {
		t.Fatal(err)
	}
	if p := r.poll(t); p.Imported+p.Refused+p.Failed != 0 {
		t.Fatalf("a manifest-less bundle was touched: %+v", p)
	}
	if res := r.spokePass(t); res.Swept != 1 {
		t.Fatalf("sweep = %+v", res)
	}
	if k := r.keys(t, prefix); len(k) != 0 {
		t.Fatalf("leftover: %v", k)
	}
}

// B1: imported, ack lost. The next poll rebuilds the ack from the receipts
// without downloading, and the spoke applies it.
func TestRelay_RebuildsALostAck(t *testing.T) {
	r := newRelayRig(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		r.write(t, relayPath(i), fmt.Sprintf("payload %d", i))
	}
	id := r.spokePass(t).Exported.BundleID
	r.poll(t)
	if err := r.relay.Delete(ctx, RelayAckKey(testSpokeID, id)); err != nil {
		t.Fatal(err)
	}
	if p := r.poll(t); p.Reacked != 1 || p.Imported != 0 {
		t.Fatalf("poll = %+v", p)
	}
	if res := r.spokePass(t); res.Synced != 2 {
		t.Fatalf("apply rebuilt ack = %+v", res)
	}
}

// A bundle whose signature does not verify is refused for good: the hub
// writes a note, the spoke fails the rows (no re-upload loop) and removes the
// bundle, and the hub then prunes the note.
func TestRelay_RefusalFailsRowsAndStopsResending(t *testing.T) {
	r := newRelayRig(t)
	ctx := context.Background()
	r.write(t, relayPath(0), "payload 0")
	id := r.spokePass(t).Exported.BundleID

	// Rewrite the manifest's MAC: the bundle no longer verifies.
	key := RelayOutboxPrefix(testSpokeID, id) + manifestName
	raw, err := r.relay.Read(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	mustUnmarshal(t, raw, &m)
	m.MAC = strings.Repeat("0", len(m.MAC))
	raw, _ = json.Marshal(m)
	if err := r.relay.Write(ctx, key, raw); err != nil {
		t.Fatal(err)
	}

	if p := r.poll(t); p.Refused != 1 {
		t.Fatalf("poll = %+v", p)
	}
	if p := r.poll(t); p.Refused != 0 {
		t.Fatalf("a refused bundle was refused again: %+v", p)
	}
	res := r.spokePass(t)
	if res.Refused != 1 || res.Exported != nil {
		t.Fatalf("spoke = %+v (a refused file must not be re-exported)", res)
	}
	if got := r.state(t, relayPath(0)); got != StateFailed {
		t.Fatalf("state = %s, want failed", got)
	}
	r.poll(t)
	if k := r.keys(t, RelayRejectedDir(testSpokeID)); len(k) != 0 {
		t.Fatalf("note not pruned: %v", k)
	}
}

// Content the spoke did not sign: a data object swapped for one of the same
// size verifies the MAC but not the digest. Nothing is committed, and it is
// NOT a refusal: the spoke re-sending the same files (ack_timeout) cures it,
// so the hub retries instead of failing the spoke's rows (review H1).
func TestRelay_SwappedDataObjectRefused(t *testing.T) {
	r := newRelayRig(t)
	ctx := context.Background()
	r.write(t, relayPath(0), "payload 0")
	id := r.spokePass(t).Exported.BundleID
	if err := r.relay.Write(ctx, RelayOutboxPrefix(testSpokeID, id)+"data/"+relayPath(0), []byte("PAYLOAD 0")); err != nil {
		t.Fatal(err)
	}
	if p := r.poll(t); p.Refused != 0 || p.Failed != 1 || p.Imported != 0 {
		t.Fatalf("poll = %+v", p)
	}
	if k := r.keys(t, RelayRejectedDir(testSpokeID)); len(k) != 0 {
		t.Fatalf("a content error wrote a refusal: %v", k)
	}
	if ok, _ := r.hub.hubStore.Exists(ctx, NamespacedPath(testSpokeID, relayPath(0))); ok {
		t.Fatal("a refused bundle's file was committed")
	}
}

// A data object larger than declared is stopped while downloading, at its
// declared size, not after the hub has stored all of it; transient, as above.
func TestRelay_OversizedDataObjectRefused(t *testing.T) {
	r := newRelayRig(t)
	ctx := context.Background()
	r.write(t, relayPath(0), "payload 0")
	id := r.spokePass(t).Exported.BundleID
	if err := r.relay.Write(ctx, RelayOutboxPrefix(testSpokeID, id)+"data/"+relayPath(0), []byte(strings.Repeat("x", 1<<20))); err != nil {
		t.Fatal(err)
	}
	if p := r.poll(t); p.Refused != 0 || p.Failed != 1 {
		t.Fatalf("poll = %+v", p)
	}
}

// A signed bundle copied under another bundle ID is refused: it would be
// acknowledged where its spoke never looks.
func TestRelay_BundleUnderTheWrongKeyRefused(t *testing.T) {
	r := newRelayRig(t)
	ctx := context.Background()
	r.write(t, relayPath(0), "payload 0")
	id := r.spokePass(t).Exported.BundleID
	other, _ := NewBundleID(time.Now().Add(time.Second))
	for _, k := range r.keys(t, RelayOutboxPrefix(testSpokeID, id)) {
		b, err := r.relay.Read(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.relay.Write(ctx, strings.Replace(k, id, other, 1), b); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.spoke.removeBundleForTest(ctx, id); err != nil {
		t.Fatal(err)
	}
	if p := r.poll(t); p.Refused != 1 {
		t.Fatalf("poll = %+v", p)
	}
}

// A bundle for another hub sharing the bucket is left alone: no import, no
// refusal note that would make its spoke fail the files.
func TestRelay_OtherHubsBundleLeftAlone(t *testing.T) {
	r := newRelayRig(t)
	r.poller.importer.hubID = "another-hub"
	r.write(t, relayPath(0), "payload 0")
	r.spokePass(t)
	if p := r.poll(t); p.Skipped != 1 || p.Refused != 0 {
		t.Fatalf("poll = %+v", p)
	}
	if k := r.keys(t, RelayRejectedDir(testSpokeID)); len(k) != 0 {
		t.Fatalf("note written for another hub's bundle: %v", k)
	}
	// Remembered: the next poll does not fetch it again.
	if p := r.poll(t); p.Skipped != 0 {
		t.Fatalf("a foreign bundle was re-fetched: %+v", p)
	}
}

// B2: no ack within ack_timeout. The spoke takes the bundle back: files to
// pending, bundle removed, re-exported in the same pass.
func TestRelay_AckTimeoutTakesTheBundleBack(t *testing.T) {
	r := newRelayRig(t)
	r.write(t, relayPath(0), "payload 0")
	id := r.spokePass(t).Exported.BundleID
	r.spoke.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	res := r.spokePass(t)
	if res.TimedOut != 1 || res.Reverted != 1 {
		t.Fatalf("pass = %+v", res)
	}
	if res.Exported == nil || res.Exported.BundleID == id {
		t.Fatalf("not re-exported in a new bundle: %+v", res.Exported)
	}
	if k := r.keys(t, RelayOutboxPrefix(testSpokeID, id)); len(k) != 0 {
		t.Fatalf("old bundle left: %v", k)
	}
}

// A conflict reported by the ack is terminal (as on the network path); a
// path the ack neither acknowledged nor reported goes back to pending rather
// than staying exported on a deleted bundle.
func TestRelay_AckSettlesWhatItDidNotAdvance(t *testing.T) {
	r := newRelayRig(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		r.write(t, relayPath(i), fmt.Sprintf("payload %d", i))
	}
	id := r.spokePass(t).Exported.BundleID
	ack := &Ack{
		BundleID: id, SpokeID: testSpokeID, HubID: testHubID, ImportedAt: time.Now().Unix(),
		Paths:     []string{relayPath(0)},
		Conflicts: []Conflict{{Path: relayPath(1), TheirSHA256: strings.Repeat("a", 64)}},
	}
	raw, err := EncodeAck(r.hub.secret, ack)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.relay.Write(ctx, RelayAckKey(testSpokeID, id), raw); err != nil {
		t.Fatal(err)
	}
	res := r.spokePass(t)
	if res.Synced != 1 || res.Conflicts != 1 || res.Reverted != 1 {
		t.Fatalf("pass = %+v", res)
	}
	want := map[int]SyncState{0: StateSynced, 1: StateFailed}
	for i, s := range want {
		if got := r.state(t, relayPath(i)); got != s {
			t.Fatalf("%s: %s, want %s", relayPath(i), got, s)
		}
	}
	// The reverted file was re-exported in the same pass.
	if got := r.state(t, relayPath(2)); got != StateExported {
		t.Fatalf("%s: %s, want exported again", relayPath(2), got)
	}
}

// An ack that fails verification is not acted on.
func TestRelay_ForgedAckIgnored(t *testing.T) {
	r := newRelayRig(t)
	ctx := context.Background()
	r.write(t, relayPath(0), "payload 0")
	id := r.spokePass(t).Exported.BundleID
	raw, err := EncodeAck("not-the-secret", &Ack{BundleID: id, SpokeID: testSpokeID, HubID: testHubID, ImportedAt: 1, Paths: []string{relayPath(0)}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.relay.Write(ctx, RelayAckKey(testSpokeID, id), raw); err != nil {
		t.Fatal(err)
	}
	if res := r.spokePass(t); res.AcksApplied != 0 {
		t.Fatalf("forged ack applied: %+v", res)
	}
	if got := r.state(t, relayPath(0)); got != StateExported {
		t.Fatalf("state = %s", got)
	}
}

func TestBundleIDTime(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond).UTC()
	id, _ := NewBundleID(now)
	got, err := bundleIDTime(id)
	if err != nil || !got.Equal(now) {
		t.Fatalf("got %v err %v, want %v", got, err, now)
	}
}

func (r *RelaySpoke) removeBundleForTest(ctx context.Context, id string) error {
	if !r.removeBundle(ctx, r.exporter.spokeID, id) {
		return fmt.Errorf("nothing removed")
	}
	return nil
}

// A file that changes between discovery and export is not signed, and the
// half-uploaded bundle is removed: no outbox objects, the row still pending.
func TestRelay_ExportAbortsAndCleansUpWhenASourceChanged(t *testing.T) {
	r := newRelayRig(t)
	ctx := context.Background()
	r.write(t, relayPath(0), "payload 0")
	r.write(t, relayPath(1), "payload 1")
	if _, err := r.exporter.discoverer.Discover(ctx); err != nil {
		t.Fatal(err)
	}
	r.write(t, relayPath(1), "PAYLOAD 1") // same size, different digest
	if _, err := r.spoke.Run(ctx); err == nil || !strings.Contains(err.Error(), "changed while exporting") {
		t.Fatalf("err = %v, want a changed-while-exporting refusal", err)
	}
	if k := r.keys(t, RelayOutboxDir(testSpokeID)); len(k) != 0 {
		t.Fatalf("partial bundle left in the bucket: %v", k)
	}
	if got := r.state(t, relayPath(0)); got != StatePending {
		t.Fatalf("state = %s, want pending", got)
	}
}

type flakyConditional struct {
	storage.Backend
	plainWrites int
}

func (f *flakyConditional) WriteIfAbsent(context.Context, string, []byte) (bool, error) {
	return false, fmt.Errorf("NoSuchKey: conditional put not understood")
}

func (f *flakyConditional) Write(ctx context.Context, key string, data []byte) error {
	f.plainWrites++
	return f.Backend.Write(ctx, key, data)
}

// A store that cannot honour If-None-Match (MinIO 2025-09 answers 404) must
// not stop acks: the conditional write falls back to a plain one.
func TestWriteIfAbsentFallsBackWhenTheConditionErrors(t *testing.T) {
	lb, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	f := &flakyConditional{Backend: lb}
	created, err := writeIfAbsent(context.Background(), f, "s/acks/x.json", []byte("ack"))
	if err != nil || !created || f.plainWrites != 1 {
		t.Fatalf("created=%v err=%v plain=%d", created, err, f.plainWrites)
	}
}

// A refusal note without the hub's signature is ignored: bucket write access
// alone must not fail a spoke's rows (review H2).
func TestRelay_ForgedRefusalIgnored(t *testing.T) {
	r := newRelayRig(t)
	ctx := context.Background()
	r.write(t, relayPath(0), "payload 0")
	id := r.spokePass(t).Exported.BundleID
	raw, _ := json.Marshal(RelayRefusal{BundleID: id, SpokeID: testSpokeID, HubID: testHubID, Reason: "forged", RefusedAt: 1, MAC: strings.Repeat("0", 64)})
	if err := r.relay.Write(ctx, RelayRejectedKey(testSpokeID, id), raw); err != nil {
		t.Fatal(err)
	}
	if res := r.spokePass(t); res.Refused != 0 {
		t.Fatalf("forged refusal acted on: %+v", res)
	}
	if got := r.state(t, relayPath(0)); got != StateExported {
		t.Fatalf("state = %s", got)
	}
}

// A replayed genuine ack, put back under its own key beside a planted
// manifest, advances only rows still exported on that bundle: a row that
// was re-queued since (pending) stays pending (review M4).
func TestRelay_ReplayedAckAdvancesOnlyThatBundlesRows(t *testing.T) {
	r := newRelayRig(t)
	ctx := context.Background()
	r.write(t, relayPath(0), "payload 0")
	id := r.spokePass(t).Exported.BundleID
	r.poll(t)
	ack, err := r.relay.Read(ctx, RelayAckKey(testSpokeID, id))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := r.relay.Read(ctx, RelayOutboxPrefix(testSpokeID, id)+manifestName)
	if err != nil {
		t.Fatal(err)
	}
	r.spokePass(t) // applies, removes the bundle
	if err := r.ledger.RequeueSynced(ctx, testHubID, relayPath(0)); err != nil {
		t.Fatal(err)
	}
	// The replay: the old ack and a manifest at the bundle's own keys.
	if err := r.relay.Write(ctx, RelayAckKey(testSpokeID, id), ack); err != nil {
		t.Fatal(err)
	}
	if err := r.relay.Write(ctx, RelayOutboxPrefix(testSpokeID, id)+manifestName, manifest); err != nil {
		t.Fatal(err)
	}
	res := &RelaySpokeResult{}
	if !r.spoke.applyAck(ctx, testSpokeID, id, res) {
		t.Fatal("the replayed ack verifies and should be processed")
	}
	if res.Synced != 0 {
		t.Fatalf("replay advanced %d rows", res.Synced)
	}
	if got := r.state(t, relayPath(0)); got != StatePending {
		t.Fatalf("state = %s, want pending", got)
	}
}

// A relay bundle that vanished from the bucket is taken back from the
// ledger; a drive bundle in transit is not touched (review M3).
func TestRelay_VanishedBundleIsTakenBack(t *testing.T) {
	r := newRelayRig(t)
	ctx := context.Background()
	r.write(t, relayPath(0), "payload 0")
	r.write(t, relayPath(1), "payload 1")
	if _, err := r.exporter.discoverer.Discover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.ledger.MarkExported(ctx, testHubID, relayPath(1), "DRIVEBUNDLE0000000000000000"); err != nil {
		t.Fatal(err)
	}
	id := r.spokePass(t).Exported.BundleID
	for _, k := range r.keys(t, RelayOutboxPrefix(testSpokeID, id)) {
		_ = r.relay.Delete(ctx, k)
	}
	pruneEmptyLocalDirs(r.relay, RelayOutboxPrefix(testSpokeID, id))
	res := r.spokePass(t)
	if res.Reverted != 1 || res.Exported == nil {
		t.Fatalf("pass = %+v", res)
	}
	if got := r.state(t, relayPath(1)); got != StateExported {
		t.Fatalf("drive bundle row = %s, want still exported", got)
	}
}

// A prefix naming no registered spoke costs no fetch at all.
func TestRelay_UnregisteredPrefixNotFetched(t *testing.T) {
	r := newRelayRig(t)
	ctx := context.Background()
	id, _ := NewBundleID(time.Now())
	if err := r.relay.Write(ctx, RelayOutboxPrefix("stranger", id)+manifestName, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if p := r.poll(t); p.Skipped != 1 || p.Refused+p.Failed != 0 {
		t.Fatalf("poll = %+v", p)
	}
}

// A drive import holding the importer is found before any data download,
// and is not a failure.
func TestRelay_DriveImportInProgressWaits(t *testing.T) {
	r := newRelayRig(t)
	r.write(t, relayPath(0), "payload 0")
	r.spokePass(t)
	unlock, ok := r.hub.importer.tryLock()
	if !ok {
		t.Fatal("lock")
	}
	p := r.poll(t)
	unlock()
	if p.Failed != 0 || p.Imported != 0 {
		t.Fatalf("poll = %+v", p)
	}
	if p := r.poll(t); p.Imported != 1 {
		t.Fatalf("after the drive import: %+v", p)
	}
}
