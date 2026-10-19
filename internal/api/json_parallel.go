package api

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
)

// Parallel JSON encoding of a query result: the stream goroutine reads record
// batches in order and hands each to an encoder goroutine when one is free;
// the encoded bytes are written in order. JSON encoding (floats and
// timestamps above all) was the whole cost of a large JSON response; DuckDB
// produces batches far faster than one core encodes them.

// jsonParallelAfterBatches is how many batches a stream encodes itself before
// handing batches out: small results (dashboards, a few thousand rows) never
// pay for the hand-off.
const jsonParallelAfterBatches = 2

// jsonMaxInFlight bounds a query's batches handed out and not yet written,
// so one huge result cannot take every core from concurrent queries, and its
// memory stays a few encoded batches.
const jsonMaxInFlight = 4

// jsonEncodersInUse counts encoder goroutines across the process. A batch is
// handed out only while fewer than GOMAXPROCS-1 are running (read at each
// hand-off: a licence re-pins GOMAXPROCS at runtime); otherwise the stream
// goroutine encodes it itself. A slot is held only while a batch is being
// encoded, never while a query waits on its client.
var jsonEncodersInUse atomic.Int64

// jsonEncodeHandoffs counts batches encoded off the stream goroutine.
var jsonEncodeHandoffs atomic.Int64

// jsonEncoderLimit is the most encoder goroutines that may run at once.
// Tests replace it to force the parallel path regardless of the machine.
var jsonEncoderLimit = func() int64 { return int64(runtime.GOMAXPROCS(0) - 1) }

func tryAcquireJSONEncoder() bool {
	limit := jsonEncoderLimit()
	for {
		cur := jsonEncodersInUse.Load()
		if cur >= limit {
			return false
		}
		if jsonEncodersInUse.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// jsonEncodeJob is one batch's rows to encode. The stream goroutine owns it
// from submit to finish: it retains the record before handing the job out
// and releases it after the job is done, whether the bytes are written or
// not. A batch is never split across goroutines (an array's lazily built
// parts, e.g. a dictionary, are not safe to build concurrently).
type jsonEncodeJob struct {
	cols   []arrow.Array
	rows   int
	comma  bool // a row was dispatched before this batch
	record arrow.Record

	buf  []byte
	err  error
	done chan struct{}
}

// jsonEncodeBufMax is the largest buffer put back in the pool: an unusually
// wide or long batch's buffer is left to the GC.
const jsonEncodeBufMax = 4 << 20

var jsonEncodeBufPool = sync.Pool{New: func() any { b := make([]byte, 0, 256<<10); return &b }}

// encodeJSONJob turns a job into bytes and closes done. A panic is the job's
// error, not the process's (on an encoder goroutine nothing else would
// recover it): the stream ends truncated, and the partial buffer is dropped.
func encodeJSONJob(job *jsonEncodeJob) {
	defer close(job.done)
	defer func() {
		if r := recover(); r != nil {
			job.buf = nil
			job.err = fmt.Errorf("JSON encoder panicked: %v", r)
		}
	}()
	enc := &jsonValueEncoder{}
	buf := (*jsonEncodeBufPool.Get().(*[]byte))[:0]
	for row := 0; row < job.rows; row++ {
		if row > 0 || job.comma {
			buf = append(buf, ',')
		}
		buf = enc.appendRow(buf, job.cols, row)
	}
	job.buf = buf
}

// jsonEncodePipeline is a stream's batches handed out and not yet written,
// in order.
type jsonEncodePipeline struct {
	pending []*jsonEncodeJob
	wg      sync.WaitGroup
}

// submit hands a job to an encoder goroutine, or encodes it here when none
// is free.
func (p *jsonEncodePipeline) submit(job *jsonEncodeJob) {
	job.done = make(chan struct{})
	p.pending = append(p.pending, job)
	if !tryAcquireJSONEncoder() {
		encodeJSONJob(job)
		return
	}
	jsonEncodeHandoffs.Add(1)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer jsonEncodersInUse.Add(-1)
		encodeJSONJob(job)
	}()
}

// full reports whether the oldest job must be written before another is
// submitted.
func (p *jsonEncodePipeline) full() bool { return len(p.pending) >= jsonMaxInFlight }

// ready reports whether the oldest job is encoded.
func (p *jsonEncodePipeline) ready() bool {
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
func (p *jsonEncodePipeline) next() *jsonEncodeJob {
	job := p.pending[0]
	p.pending = p.pending[1:]
	<-job.done
	return job
}

// drain waits for every job still out and releases it without writing, then
// for the goroutines themselves.
func (p *jsonEncodePipeline) drain() {
	for len(p.pending) > 0 {
		p.next().finish()
	}
	p.wg.Wait()
}

// finish releases a job's record and returns its buffer to the pool.
func (job *jsonEncodeJob) finish() {
	if job.record != nil {
		job.record.Release()
		job.record = nil
	}
	if job.buf != nil && cap(job.buf) <= jsonEncodeBufMax {
		b := job.buf[:0]
		jsonEncodeBufPool.Put(&b)
	}
	job.buf = nil
}
