package api

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/ingest"
)

// importPacer holds a node's import write slots and the limit its write
// passes pace against (#1429). Until #1429 one pass wrote at a time, which
// on a large writer capped bulk loads at one core's worth of work.
//
// Every active pass paces against the same limit: where the oldest active
// pass started (the lowest BufferedBytes any of them saw at its start) plus
// every active pass's allowance. With per-pass limits, the pass with the
// lowest one would wait on rows the others wrote and it cannot flush, while
// they kept writing under theirs, and could run out its whole flush wait into
// a failure part-way. With one pass the limit is the pre-#1429 one: its start
// plus its allowance.
//
// A pass is counted from its slot until close, after its final flush: its
// rows are buffered until then, though it gives its slot back before that
// flush.
type importPacer struct {
	slots        chan struct{}
	queueTimeout time.Duration // 0: wait as long as the request lives

	mu     sync.Mutex
	active map[*importWriter]importPass
}

// importPass is one counted write pass: BufferedBytes when it started, and
// the bytes it may add above the shared baseline.
type importPass struct {
	baseline  int64
	allowance int64
}

func newImportPacer(slots int, queueTimeout time.Duration) *importPacer {
	return &importPacer{
		slots:        make(chan struct{}, max(slots, 1)),
		queueTimeout: queueTimeout,
		active:       map[*importWriter]importPass{},
	}
}

// importPacerCurrent is the node's pacer: one slot and no queue timeout (the
// pre-#1429 behaviour) until ConfigureImports runs at startup.
var importPacerCurrent atomic.Pointer[importPacer]

func init() { importPacerCurrent.Store(newImportPacer(1, 0)) }

func currentImportPacer() *importPacer { return importPacerCurrent.Load() }

// ConfigureImports sets this node's import write concurrency and queue
// timeout (ingest.import_write_concurrency, ingest.import_queue_timeout_ms).
// concurrency 0 derives it from buf (importAutoConcurrency). Call once at
// startup, before the import routes serve. Returns the concurrency in use.
func ConfigureImports(concurrency int, queueTimeout time.Duration, buf *ingest.ArrowBuffer) int {
	n := concurrency
	if n <= 0 {
		n = importAutoConcurrency(buf.MemoryLimitBytes(), importBufferedAllowance(buf), runtime.GOMAXPROCS(0))
	}
	importPacerCurrent.Store(newImportPacer(n, queueTimeout))
	return n
}

// importAutoConcurrency is the default number of import write slots: as many
// passes as fit an eighth of the memory limit at one allowance each (a pass
// holds about that much: half grouped rows, half buffered rows), no more
// than the cores Arc runs on, at least one. Memory unknown: one. A 3 GiB
// writer (the #1296 out-of-memory case) keeps one.
func importAutoConcurrency(memoryLimit uint64, allowance int64, procs int) int {
	if memoryLimit == 0 || allowance <= 0 {
		return 1
	}
	n := int(memoryLimit / 8 / uint64(allowance))
	return min(max(n, 1), max(procs, 1))
}

// acquire waits for a slot, for as long as ctx lives and at most the queue
// timeout, then counts w as active from buffered() and allowance.
func (p *importPacer) acquire(ctx context.Context, w *importWriter, buffered func() int64, allowance int64) error {
	var timeout <-chan time.Time
	if p.queueTimeout > 0 {
		t := time.NewTimer(p.queueTimeout)
		defer t.Stop()
		timeout = t.C
	}
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-timeout:
		return ingest.ErrImportQueueTimeout
	}
	p.mu.Lock()
	p.active[w] = importPass{baseline: buffered(), allowance: allowance}
	p.mu.Unlock()
	return nil
}

func (p *importPacer) releaseSlot() { <-p.slots }

// leave stops counting w.
func (p *importPacer) leave(w *importWriter) {
	p.mu.Lock()
	delete(p.active, w)
	p.mu.Unlock()
}

// limit is the shared limit of the active passes (see importPacer).
func (p *importPacer) limit() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	first := true
	var base, sum int64
	for _, a := range p.active {
		if first || a.baseline < base {
			base = a.baseline
		}
		first = false
		sum += a.allowance
	}
	return base + sum
}
