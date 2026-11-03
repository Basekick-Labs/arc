package api

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
)

// Parallel encoding of a query response: the stream goroutine hands pieces of
// the result (JSON: a record batch's rows; MessagePack: a run of batches of one
// column) to encoder goroutines when one is free, and writes the encoded bytes
// in order. Encoding (floats and timestamps above all) was nearly the whole
// cost of a large JSON or MessagePack response; DuckDB produces batches far
// faster than one core encodes them.

// parallelEncodeAfterBatches is how many batches a stream encodes itself
// before handing work out: small results (dashboards, a few thousand rows)
// never pay for the hand-off.
const parallelEncodeAfterBatches = 2

// encodeMaxInFlight bounds a query's jobs handed out and not yet written, so
// one huge result cannot take every core from concurrent queries, and its
// memory stays a few encoded pieces.
const encodeMaxInFlight = 4

// encodersInUse counts encoder goroutines across the process. A job is handed
// out only while fewer than GOMAXPROCS-1 are running (read at each hand-off:
// a licence re-pins GOMAXPROCS at runtime); otherwise the stream goroutine
// encodes it itself. A slot is held only while a job is being encoded, never
// while a query waits on its client.
var encodersInUse atomic.Int64

// encodeHandoffs counts jobs encoded off the stream goroutine.
var encodeHandoffs atomic.Int64

// encoderLimit is the most encoder goroutines that may run at once. Tests
// replace it to force the parallel path regardless of the machine.
var encoderLimit = func() int64 { return int64(runtime.GOMAXPROCS(0) - 1) }

func tryAcquireEncoder() bool {
	limit := encoderLimit()
	for {
		cur := encodersInUse.Load()
		if cur >= limit {
			return false
		}
		if encodersInUse.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// encodeJob is one piece of a response to encode. The stream goroutine owns
// it from submit to finish. fill appends the piece's bytes to buf; it must
// only read data that stays valid until the job is finished (a JSON job holds
// its record; MessagePack's batches belong to the caller for the whole call).
// A batch's arrays are never read by two jobs at once when that could build
// lazily-built state (a dictionary) concurrently: JSON jobs are whole
// batches, MessagePack jobs are one column over a run of whole batches.
type encodeJob struct {
	fill   func(buf []byte) ([]byte, error)
	rows   int          // rows the piece carries (JSON; MessagePack leaves it 0)
	record arrow.Record // released at finish; nil when the job holds none

	buf  []byte
	err  error
	done chan struct{}
}

// encodeBufMax is the largest buffer put back in the pool: an unusually wide
// or long piece's buffer is left to the GC.
const encodeBufMax = 4 << 20

var encodeBufPool = sync.Pool{New: func() any { b := make([]byte, 0, 256<<10); return &b }}

// runEncodeJob encodes a job and closes done. A panic is the job's error, not
// the process's (on an encoder goroutine nothing else would recover it): the
// stream ends truncated, and the partial buffer is dropped.
func runEncodeJob(job *encodeJob) {
	defer close(job.done)
	defer func() {
		if r := recover(); r != nil {
			job.buf = nil
			job.err = fmt.Errorf("encoder panicked: %v", r)
		}
	}()
	buf := (*encodeBufPool.Get().(*[]byte))[:0]
	job.buf, job.err = job.fill(buf)
}

// encodePipeline is a stream's jobs handed out and not yet written, in order.
type encodePipeline struct {
	pending []*encodeJob
	wg      sync.WaitGroup
}

// submit hands a job to an encoder goroutine, or encodes it here when none is
// free.
func (p *encodePipeline) submit(job *encodeJob) {
	job.done = make(chan struct{})
	p.pending = append(p.pending, job)
	if !tryAcquireEncoder() {
		runEncodeJob(job)
		return
	}
	encodeHandoffs.Add(1)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer encodersInUse.Add(-1)
		runEncodeJob(job)
	}()
}

// full reports whether the oldest job must be written before another is
// submitted.
func (p *encodePipeline) full() bool { return len(p.pending) >= encodeMaxInFlight }

// ready reports whether the oldest job is encoded.
func (p *encodePipeline) ready() bool {
	if len(p.pending) == 0 {
		return false
	}
	select {
	case <-p.pending[0].done:
		return true
	default:
		return false
	}
}

// next waits for the oldest job and takes it back.
func (p *encodePipeline) next() *encodeJob {
	job := p.pending[0]
	p.pending = p.pending[1:]
	<-job.done
	return job
}

// drain waits for every job still out and releases it without writing, then
// for the goroutines themselves.
func (p *encodePipeline) drain() {
	for len(p.pending) > 0 {
		p.next().finish()
	}
	p.wg.Wait()
}

// finish releases a job's record and returns its buffer to the pool.
func (job *encodeJob) finish() {
	if job.record != nil {
		job.record.Release()
		job.record = nil
	}
	if job.buf != nil && cap(job.buf) <= encodeBufMax {
		b := job.buf[:0]
		encodeBufPool.Put(&b)
	}
	job.buf = nil
}

// jsonRowsFill returns a JSON job's fill: rows [0,n) of cols as JSON arrays,
// each after a comma unless it is the first row of the response.
func jsonRowsFill(cols []arrow.Array, n int, comma bool) func([]byte) ([]byte, error) {
	return func(buf []byte) ([]byte, error) {
		enc := &jsonValueEncoder{}
		for row := 0; row < n; row++ {
			if row > 0 || comma {
				buf = append(buf, ',')
			}
			buf = enc.appendRow(buf, cols, row)
		}
		return buf, nil
	}
}
