//go:build duckdb_arrow

package api

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/rehome"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
)

// coldTiering1825 is a tiering manager with a cold tier: a second local
// directory. It keeps tier rows as the fake without one does.
type coldTiering1825 struct {
	*fakeTiering1825
	cold    storage.Backend
	store   string // this node's cold store identity
	cmu     sync.Mutex
	coldRow map[string]bool
	// checkCold fails the source check; removeErr the removal.
	checkCold error
	removeErr error
	removed   []string
}

func (c *coldTiering1825) ColdBackend() storage.Backend { return c.cold }

func (c *coldTiering1825) CheckRehomeColdSource(ctx context.Context, path string) (int64, string, error) {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	if c.owns != nil && !c.owns(path) {
		c.unowned = append(c.unowned, path)
	}
	if c.checkCold != nil {
		return 0, "", c.checkCold
	}
	n, err := c.cold.StatFile(ctx, path)
	if err != nil || n < 0 || !c.coldRow[path] {
		return 0, "", fmt.Errorf("%w: %s", tiering.ErrRehomeNotCold, path)
	}
	return n, c.store, nil
}

func (c *coldTiering1825) ColdSourceStat(ctx context.Context, path, store string) (int64, error) {
	if store != c.store {
		return 0, fmt.Errorf("%w: %s", tiering.ErrColdStoreChanged, path)
	}
	return c.cold.StatFile(ctx, path)
}

func (c *coldTiering1825) RemoveRehomedColdFile(ctx context.Context, path, store string) error {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	if store != c.store {
		return fmt.Errorf("%w: %s", tiering.ErrColdStoreChanged, path)
	}
	if c.removeErr != nil {
		return c.removeErr
	}
	if err := c.cold.Delete(ctx, path); err != nil && !storage.IsNotFound(err) {
		return err
	}
	delete(c.coldRow, path)
	c.removed = append(c.removed, path)
	return nil
}

// coldSwapRig1825 is the swap rig with its file moved to cold storage.
type coldSwapRig1825 struct {
	*swapRig
	coldRoot string
	tier     *coldTiering1825
}

func newColdSwapRig1825(t *testing.T) *coldSwapRig1825 {
	t.Helper()
	r := newSwapRig(t)
	coldRoot := t.TempDir()
	cold, err := storage.NewLocalBackend(coldRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cold.Close() })
	c := &coldSwapRig1825{swapRig: r, coldRoot: coldRoot}
	if err := os.MkdirAll(filepath.Dir(filepath.Join(coldRoot, swapKey)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(r.root, swapKey), filepath.Join(coldRoot, swapKey)); err != nil {
		t.Fatal(err)
	}
	c.bind(cold)
	return c
}

// bind gives the rig's current handler a tiering manager over cold.
func (c *coldSwapRig1825) bind(cold storage.Backend) {
	c.tier = &coldTiering1825{
		fakeTiering1825: &fakeTiering1825{rows: map[string]bool{}, owns: c.reg.Owns},
		cold:            cold, store: "cold-store-A", coldRow: map[string]bool{swapKey: true},
	}
	c.swap.TieringExpected, c.swap.ColdConfigured = true, true
	c.swap.ColdFileNames = func(_ context.Context, glob string) ([]string, error) {
		// As the engine does: the name is the path it was given.
		uri, err := storage.ObjectURI(cold, swapKey)
		return []string{uri}, err
	}
	c.h.SetTiering(c.tier)
	c.h.Wired()
}

func (c *coldSwapRig1825) coldThere() bool {
	_, err := os.Stat(filepath.Join(c.coldRoot, swapKey))
	return err == nil
}

func (c *coldSwapRig1825) hotFiles() []string {
	keys, err := c.backend.List(context.Background(), "db/cpu/")
	if err != nil {
		c.t.Fatal(err)
	}
	return keys
}

// A file in cold storage: read from there, replaced by files on the hot
// tier, left out of reads by its cold key once they are in place, and removed
// from cold storage with its row.
func TestColdSwap1825(t *testing.T) {
	c := newColdSwapRig1825(t)
	type excl struct{ keys, cold []string }
	at := map[string]excl{}
	var steps []string
	c.h.afterStep = func(s string) {
		steps = append(steps, s)
		k, cold := c.reg.ExcludedTiered(context.Background(), "db/cpu/")
		at[s] = excl{k, cold}
		if s == "latched" && !c.coldThere() {
			t.Error("the old file left cold storage before reads from before the switch had ended")
		}
	}
	id, code, why := c.start()
	if code != 202 {
		t.Fatalf("start: %d %s", code, why)
	}
	job := c.wait(id)
	if job["state"] != "done" || job["tier"] != "cold" {
		t.Fatalf("job: %v", job)
	}
	if c.coldThere() || len(c.tier.removed) != 1 || c.tier.removed[0] != swapKey || c.tier.coldRow[swapKey] {
		t.Fatalf("the old file or its row is still in cold storage (removed %v)", c.tier.removed)
	}
	if files := c.hotFiles(); len(files) != 3 {
		t.Fatalf("hot files %v; want the three new files", files)
	}
	if rows := c.tier.hot(); len(rows) != 3 {
		t.Fatalf("hot rows %v; want the three new files'", rows)
	}
	if len(c.tier.unowned) != 0 {
		t.Fatalf("tiering was asked about %v before the job owned it", c.tier.unowned)
	}
	if e := at["committed"]; len(e.keys) != 3 || len(e.cold) != 0 {
		t.Fatalf("committed: %+v; want the three outputs, none of them cold", e)
	}
	if e := at["latched"]; len(e.keys) != 1 || e.keys[0] != swapKey || len(e.cold) != 1 || e.cold[0] != swapKey {
		t.Fatalf("latched: %+v; want the old file, named as a cold key", e)
	}
	if got := strings.Join(steps, " "); !strings.HasPrefix(got, "staged cold-name committed ") {
		t.Fatalf("steps: %s; the name is asked about before reads leave anything out", got)
	}
	if c.reg.Active() {
		t.Fatal("the job's record was kept")
	}
}

// The engine names the file otherwise than Arc would leave it out by: the
// job is refused before anything changes.
func TestColdSwapRefusedWhenTheEngineNamesTheFileOtherwise1825(t *testing.T) {
	wantWhy := map[string]string{
		"another spelling": "names the file",
		"not listed":       "does not list",
		"cannot be read":   "could not read",
	}
	for name, names := range map[string]func(want string) ([]string, error){
		"another spelling": func(want string) ([]string, error) {
			return []string{strings.Replace(want, "/db/", "//db/", 1)}, nil
		},
		"not listed":     func(string) ([]string, error) { return nil, nil },
		"cannot be read": func(string) ([]string, error) { return nil, errors.New("HTTP 403") },
	} {
		t.Run(name, func(t *testing.T) {
			c := newColdSwapRig1825(t)
			want, _ := storage.ObjectURI(c.tier.cold, swapKey)
			c.swap.ColdFileNames = func(context.Context, string) ([]string, error) { return names(want) }
			committed := false
			c.h.afterStep = func(s string) {
				if s == "committed" {
					committed = true
				}
			}
			id, _, _ := c.start()
			job := c.wait(id)
			if job["state"] != "refused" || committed || !strings.Contains(fmt.Sprint(job["reason"]), wantWhy[name]) {
				t.Fatalf("job: %v (reads changed: %v)", job, committed)
			}
			if !c.coldThere() || c.reg.Active() || len(c.hotFiles()) != 0 || len(c.tier.hot()) != 0 {
				t.Fatalf("a refused job left something: cold file %v, hot files %v, rows %v", c.coldThere(), c.hotFiles(), c.tier.hot())
			}
			if j, s := c.leftovers(); j != 0 || s != 0 {
				t.Fatalf("left %d journal and %d staged file(s)", j, s)
			}
		})
	}
}

func TestColdSwapRefusedByTiering1825(t *testing.T) {
	c := newColdSwapRig1825(t)
	c.tier.checkCold = fmt.Errorf("%w: it is on both tiers", tiering.ErrRehomeNotCold)
	id, _, _ := c.start()
	if job := c.wait(id); job["state"] != "refused" || !strings.Contains(fmt.Sprint(job["reason"]), "on both tiers") {
		t.Fatalf("job: %v", job)
	}
	if !c.coldThere() || c.reg.Active() {
		t.Fatal("a refused job changed something")
	}
	// A dry run says the same, and changes nothing either.
	code, out := rehomeDo(t, c.app, "POST", "/api/v1/rehome", "", fmt.Sprintf(`{"path":%q,"dry_run":true}`, swapKey))
	if code != 202 {
		t.Fatalf("dry run: %d %v", code, out)
	}
	if job := c.wait(out["id"].(string)); job["state"] != "refused" || !strings.Contains(fmt.Sprint(job["reason"]), "on both tiers") {
		t.Fatalf("dry run: %v", job)
	}
}

// A dry run of a cold file reads it from cold storage and reports the plan.
func TestColdDryRun1825(t *testing.T) {
	c := newColdSwapRig1825(t)
	code, out := rehomeDo(t, c.app, "POST", "/api/v1/rehome", "", fmt.Sprintf(`{"path":%q,"dry_run":true}`, swapKey))
	if code != 202 {
		t.Fatalf("dry run: %d %v", code, out)
	}
	job := c.wait(out["id"].(string))
	report, _ := job["report"].(map[string]interface{})
	if job["state"] != "done" || job["tier"] != "cold" || report["rows"] != float64(6) || report["misfiled"] != true {
		t.Fatalf("dry run: %v", job)
	}
	if !c.coldThere() || len(c.hotFiles()) != 0 || c.reg.Active() {
		t.Fatal("a dry run changed something")
	}
}

// The removal from cold storage fails: the job stays where it is, the old
// file left out of reads, and a resume finishes it.
func TestColdSwapRemovalFailsThenResumes1825(t *testing.T) {
	c := newColdSwapRig1825(t)
	c.tier.removeErr = errors.New("HTTP 503 from cold storage")
	id, _, _ := c.start()
	if job := c.wait(id); job["state"] != "stopped" {
		t.Fatalf("job: %v", job)
	}
	k, cold := c.reg.ExcludedTiered(context.Background(), "db/cpu/")
	if !c.coldThere() || !c.reg.Active() || len(k) != 1 || len(cold) != 1 {
		t.Fatalf("after a failed removal: cold file %v, active %v, excluded %v cold %v; want the job latched", c.coldThere(), c.reg.Active(), k, cold)
	}
	c.tier.cmu.Lock()
	c.tier.removeErr = nil
	c.tier.cmu.Unlock()
	if code, out := rehomeDo(t, c.app, "POST", "/api/v1/rehome/"+id+"/resume", "", ""); code != 202 {
		t.Fatalf("resume: %d %v", code, out)
	}
	if job := c.wait(id); job["state"] != "done" || c.coldThere() || c.reg.Active() {
		t.Fatalf("after resume: %v, cold file %v", job, c.coldThere())
	}
}

// A job on a cold file found at start, in each state, with the cold store it
// recorded: taken up and finished.
func TestColdRecovery1825(t *testing.T) {
	for _, tc := range []struct {
		step string
		nth  int
	}{{"moved", 2}, {"latched", 1}, {"removed", 1}} {
		t.Run(tc.step, func(t *testing.T) {
			c := newColdSwapRig1825(t)
			cold := c.tier.cold
			rowsBefore := c.tier.coldRow
			stopAt(t, c.swapRig, tc.step, tc.nth)
			c.bind(cold)
			if tc.step != "removed" {
				c.tier.coldRow = rowsBefore
			} else {
				c.tier.coldRow = map[string]bool{}
			}
			// The id before Recover: a job it finishes at once leaves the
			// registry before Recover returns.
			jobs := c.reg.Jobs()
			if len(jobs) != 1 {
				t.Fatalf("recorded jobs before recovery: %d", len(jobs))
			}
			id := jobs[0].ID
			if err := c.h.Recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			if job := c.wait(id); job["state"] != "done" {
				t.Fatalf("job: %v", job)
			}
			if c.coldThere() || c.reg.Active() || len(c.hotFiles()) != 3 || len(c.tier.hot()) != 3 {
				t.Fatalf("after recovery: cold file %v, active %v, hot files %v, rows %v", c.coldThere(), c.reg.Active(), c.hotFiles(), c.tier.hot())
			}
		})
	}
}

// Taken up with no tiering, with no cold tier, or against another cold store
// than the one recorded: the job is kept as it is. "Not there" is not
// "removed": ending the job would leave the old file to be read beside the
// new ones once cold storage is back.
func TestColdRecoveryKeepsAJobItCannotAskColdStorageAbout1825(t *testing.T) {
	for name, rebind := range map[string]func(c *coldSwapRig1825, cold storage.Backend){
		"no tiering configured": func(c *coldSwapRig1825, _ storage.Backend) {},
		"tiering not running": func(c *coldSwapRig1825, _ storage.Backend) {
			c.swap.TieringExpected, c.swap.ColdConfigured = true, true
			c.h.Wired()
		},
		"another cold store": func(c *coldSwapRig1825, cold storage.Backend) {
			c.bind(cold)
			c.tier.store = "cold-store-B"
		},
	} {
		for _, step := range []string{"moved", "latched"} {
			t.Run(name+"/"+step, func(t *testing.T) {
				c := newColdSwapRig1825(t)
				cold := c.tier.cold
				stopAt(t, c.swapRig, step, 1)
				state := c.reg.Jobs()[0].State
				rebind(c, cold)
				if err := c.h.Recover(context.Background()); err != nil {
					t.Fatal(err)
				}
				id := c.reg.Jobs()[0].ID
				if c.swap.TieringExpected {
					if job := c.wait(id); job["state"] != "stopped" {
						t.Fatalf("job: %v", job)
					}
				}
				if !c.reg.Active() || !c.coldThere() || c.reg.Jobs()[0].State != state {
					t.Fatalf("active %v, cold file %v, state %s (was %s); want the job kept as recorded", c.reg.Active(), c.coldThere(), c.reg.Jobs()[0].State, state)
				}
			})
		}
	}
}

// The record says where the source is, and survives a restart.
func TestColdJobRecord1825(t *testing.T) {
	c := newColdSwapRig1825(t)
	stopAt(t, c.swapRig, "committed", 1)
	job := c.reg.Jobs()[0]
	if !job.Source.Cold() || job.Source.Tier != rehome.TierCold || job.Source.ColdStore != "cold-store-A" || job.Source.Size <= 0 {
		t.Fatalf("record: %+v", job.Source)
	}
}

// A file of the same name in a subdirectory, which the recursive glob also
// matches, is another file: it does not make the job's file misnamed.
func TestColdNameCheckIgnoresAFileOfTheSameNameBelow1825(t *testing.T) {
	c := newColdSwapRig1825(t)
	want, _ := storage.ObjectURI(c.tier.cold, swapKey)
	slash := strings.LastIndex(want, "/")
	c.swap.ColdFileNames = func(_ context.Context, glob string) ([]string, error) {
		return []string{want[:slash] + "/sub" + want[slash:], want}, nil
	}
	id, _, _ := c.start()
	if job := c.wait(id); job["state"] != "done" {
		t.Fatalf("job: %v", job)
	}
}

// deadCold1825 is a cold store that cannot be asked.
type deadCold1825 struct{ storage.Backend }

func (deadCold1825) StatFile(context.Context, string) (int64, error) {
	return 0, errors.New("dial tcp: connection refused")
}

// A file that is not on the hot tier, with cold storage unreachable: the
// request is answered 502, not "no such file".
func TestColdStartWhenColdStorageCannotBeAsked1825(t *testing.T) {
	c := newColdSwapRig1825(t)
	c.tier.cold = deadCold1825{c.tier.cold}
	for _, body := range []string{fmt.Sprintf(`{"path":%q}`, swapKey), fmt.Sprintf(`{"path":%q,"dry_run":true}`, swapKey)} {
		code, out := rehomeDo(t, c.app, "POST", "/api/v1/rehome", "", body)
		if code != 502 || !strings.Contains(fmt.Sprint(out["error"]), "cold storage could not be asked") {
			t.Fatalf("%s: %d %v, want 502", body, code, out)
		}
	}
	// A file that is nowhere is still 404.
	c2 := newColdSwapRig1825(t)
	if code, _ := rehomeDo(t, c2.app, "POST", "/api/v1/rehome", "", `{"path":"db/cpu/2026/11/18/23/nosuch.parquet"}`); code != 404 {
		t.Fatalf("a file in neither tier: %d, want 404", code)
	}
}

// A cold store with no identity: the job's record could not say where its
// file is, and would not be readable at the next start.
func TestColdSwapRefusedWithoutAColdStoreIdentity1825(t *testing.T) {
	c := newColdSwapRig1825(t)
	c.tier.store = ""
	id, _, _ := c.start()
	if job := c.wait(id); job["state"] != "refused" || !strings.Contains(fmt.Sprint(job["reason"]), "no identity") {
		t.Fatalf("job: %v", job)
	}
	if !c.coldThere() || c.reg.Active() {
		t.Fatal("a refused job changed something")
	}
}
