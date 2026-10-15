package api

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/gofiber/fiber/v2"
)

// An import reads its file part straight out of the request body, twice: a
// validation pass, then the write pass (#1070 follow-up). c.FormFile would
// copy the part into a second in-memory buffer (fasthttp parses a buffered
// body with an in-memory limit of the whole body, so nothing spills to disk),
// and an io.ReadAll on top of it made a third copy. multipart.Reader over the
// body bytes copies nothing; reopening it for the second pass is a re-scan of
// the same bytes.

// importUpload is the multipart body of an import request.
type importUpload struct {
	body     []byte
	boundary string
}

// importUploadFrom checks an import request's body and finds its file part.
// The body stays owned by fasthttp; the upload is valid until the handler
// returns.
func importUploadFrom(c *fiber.Ctx) (*importUpload, *importError) {
	// fasthttp would decode a Content-Encoding'd multipart body into a new
	// buffer with no size limit. Imports take a compressed FILE (gzip LP)
	// instead.
	if ce := c.Get(fiber.HeaderContentEncoding); ce != "" && !strings.EqualFold(ce, "identity") {
		return nil, &importError{StatusCode: fiber.StatusUnsupportedMediaType, Message: "Content-Encoding is not supported on import requests; compress the file itself (gzip) instead"}
	}
	mediaType, params, err := mime.ParseMediaType(c.Get(fiber.HeaderContentType))
	if err != nil || mediaType != "multipart/form-data" || params["boundary"] == "" {
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "no file uploaded: use multipart/form-data with field name 'file'"}
	}
	u := &importUpload{body: c.Request().Body(), boundary: params["boundary"]}
	if _, err := u.open(); err != nil {
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "no file uploaded: use multipart/form-data with field name 'file'"}
	}
	return u, nil
}

// open returns a reader of the file part ("file"), capped at maxImportSize:
// past it a read fails with errImportTooLarge.
func (u *importUpload) open() (io.Reader, error) {
	mr := multipart.NewReader(bytes.NewReader(u.body), u.boundary)
	for {
		p, err := mr.NextPart()
		if err != nil {
			return nil, err
		}
		if isImportFilePart(p) {
			return &limitedReader{r: p, limit: maxImportSize}, nil
		}
	}
}

// isImportFilePart reports whether p is the uploaded file: the "file" field,
// sent as a file (with a filename), as c.FormFile required.
func isImportFilePart(p *multipart.Part) bool {
	return p.FormName() == "file" && p.FileName() != ""
}

// openMaybeGzip is open with a gzip file decompressed on the fly, the
// decompressed bytes capped at maxImportSize too.
func (u *importUpload) openMaybeGzip() (io.Reader, error) {
	r, err := u.open()
	if err != nil {
		return nil, err
	}
	br := bufio.NewReader(r)
	if magic, _ := br.Peek(2); len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		zr, err := gzip.NewReader(br)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress gzip file: %w", err)
		}
		return &limitedReader{r: zr, limit: maxImportSize}, nil
	}
	return br, nil
}

// fileBytes is the file part's content as a slice of the request body, no
// copy: Parquet needs random access over the whole file. It finds the part
// by its delimiters and checks the slice byte for byte against what the
// multipart reader reads; when they differ (a transfer encoding, unusual
// line endings) it falls back to a copy. Valid until the handler returns.
func (u *importUpload) fileBytes() ([]byte, error) {
	if b, ok := u.fileSlice(); ok {
		if len(b) > maxImportSize {
			return nil, errImportTooLarge
		}
		return b, nil
	}
	return u.readAll()
}

func (u *importUpload) fileSlice() ([]byte, bool) {
	// Which part is the file, counting from 0.
	mr := multipart.NewReader(bytes.NewReader(u.body), u.boundary)
	index := 0
	for ; ; index++ {
		p, err := mr.NextPart()
		if err != nil {
			return nil, false
		}
		if isImportFilePart(p) {
			break
		}
	}
	body := u.body
	dash := []byte("--" + u.boundary)
	crlfDash := []byte("\r\n--" + u.boundary)
	crlf := []byte("\r\n")
	// at: where the current delimiter's "--" starts.
	at := 0
	if !bytes.HasPrefix(body, dash) {
		i := bytes.Index(body, crlfDash)
		if i < 0 {
			return nil, false
		}
		at = i + 2
	}
	for k := 0; ; k++ {
		lineEnd := bytes.Index(body[at:], crlf) // the CRLF ending the delimiter line
		if lineEnd < 0 {
			return nil, false
		}
		lineEnd += at
		headersEnd := bytes.Index(body[lineEnd:], []byte("\r\n\r\n"))
		if headersEnd < 0 {
			return nil, false
		}
		start := lineEnd + headersEnd + 4
		next := bytes.Index(body[start:], crlfDash)
		if next < 0 {
			return nil, false
		}
		end := start + next
		if k == index {
			content := body[start:end:end]
			if len(content) > maxImportSize {
				return content, true // refused by size, no need to verify it
			}
			r, err := u.open()
			if err != nil || !readerEquals(r, content) {
				return nil, false
			}
			return content, true
		}
		at = end + 2
	}
}

// readerEquals reports whether r yields exactly b.
func readerEquals(r io.Reader, b []byte) bool {
	buf := make([]byte, 64<<10)
	for {
		n, err := r.Read(buf)
		if n > len(b) || !bytes.Equal(buf[:n], b[:n]) {
			return false
		}
		b = b[n:]
		if err == io.EOF {
			return len(b) == 0
		}
		if err != nil {
			return false
		}
	}
}

// readAll reads the whole file part into a copy: for a format that needs
// the file at once (TLE records span lines).
func (u *importUpload) readAll() ([]byte, error) {
	r, err := u.open()
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

// maxImportLineBytes caps one line of a line-chunked import: a chunk holds
// at least one whole line, so the cap bounds a chunk. A var for tests.
var maxImportLineBytes = 64 << 20

// errImportLineTooLong is a line longer than maxImportLineBytes.
var errImportLineTooLong = errors.New("a line is longer than the import limit for one line")

// forEachLineChunk calls fn with consecutive slices of r, each about target
// bytes and ending on a line boundary (the last one may not). The slice is
// reused: fn must not keep it. A line longer than target makes a longer
// chunk, up to maxImportLineBytes. Each byte is searched for a newline once.
func forEachLineChunk(r io.Reader, target int, fn func(chunk []byte) error) error {
	buf := make([]byte, 0, target)
	lastNL := -1 // the last newline in buf
	for {
		if len(buf) == cap(buf) {
			if len(buf) >= maxImportLineBytes && lastNL < 0 {
				return errImportLineTooLong
			}
			grown := make([]byte, len(buf), min(2*cap(buf), maxImportLineBytes+target))
			copy(grown, buf)
			buf = grown
		}
		n, err := r.Read(buf[len(buf):cap(buf)])
		if i := bytes.LastIndexByte(buf[len(buf):len(buf)+n], '\n'); i >= 0 {
			lastNL = len(buf) + i
		}
		buf = buf[:len(buf)+n]
		if len(buf) >= target && lastNL >= 0 {
			if ferr := fn(buf[:lastNL+1]); ferr != nil {
				return ferr
			}
			buf = buf[:copy(buf, buf[lastNL+1:])]
			lastNL = -1 // the rest holds no newline: lastNL was the last one
		}
		if err == io.EOF {
			if len(buf) > 0 {
				if len(buf) > maxImportLineBytes {
					return errImportLineTooLong
				}
				return fn(buf)
			}
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// importWriter paces an import's writes to the speed of its flushes and
// keeps what it wrote, for the answer to a failure part-way.
type importWriter struct {
	buf      *ingest.ArrowBuffer
	database string
	limit    int64
	maxWait  time.Duration
	rows     int64
	measRows map[string]int64
	order    []string
	release  func()
}

// importBufferedAllowance is how many bytes an import may have written and
// not yet flushed, above what the buffer held when its write pass started:
// an eighth of the memory limit, between 64 MB and 512 MB (256 MB when the
// limit is unknown). The upload itself and one chunk come on top of it.
func importBufferedAllowance(buf *ingest.ArrowBuffer) int64 {
	const lo, hi, unknown = 64 << 20, 512 << 20, 256 << 20
	limit := buf.MemoryLimitBytes()
	if limit == 0 {
		return unknown
	}
	return min(max(int64(limit/8), lo), hi)
}

// importFlushWait bounds one wait for flushes between chunks.
var importFlushWait = 2 * time.Minute

// importAllowanceOverride replaces importBufferedAllowance when > 0. Tests.
var importAllowanceOverride int64

// importWriteHook, when set, sees the row count of every write an import
// makes. Tests.
var importWriteHook func(rows int64)

// importWriteGate lets one import write at a time on this node; validation
// passes run concurrently. Concurrent write passes would each pace against
// a buffer level the others raise: their allowances would add up, and one
// could wait out its whole flush wait behind another.
var importWriteGate = make(chan struct{}, 1)

// newImportWriter waits for this node's import write gate (as long as ctx
// lives) and starts the pacing for an import into database. Call close.
func newImportWriter(ctx context.Context, buf *ingest.ArrowBuffer, database string) (*importWriter, error) {
	select {
	case importWriteGate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	allowance := importBufferedAllowance(buf)
	if importAllowanceOverride > 0 {
		allowance = importAllowanceOverride
	}
	return &importWriter{
		buf:      buf,
		database: database,
		limit:    buf.BufferedBytes() + allowance,
		maxWait:  importFlushWait,
		measRows: map[string]int64{},
		release:  func() { <-importWriteGate },
	}, nil
}

// close releases the write gate.
func (w *importWriter) close() {
	if w.release != nil {
		w.release()
		w.release = nil
	}
}

// wrote records rows written for measurement.
func (w *importWriter) wrote(measurement string, rows int64) {
	if importWriteHook != nil {
		importWriteHook(rows)
	}
	if _, ok := w.measRows[measurement]; !ok {
		w.order = append(w.order, measurement)
	}
	w.measRows[measurement] += rows
	w.rows += rows
}

// pace waits until the import's unflushed rows are back under its allowance.
// Over it, it first flushes what the database's buffers hold: rows below the
// buffer's size threshold would otherwise wait for its age threshold.
func (w *importWriter) pace(ctx context.Context) error {
	if w.buf.BufferedBytes() <= w.limit {
		return nil
	}
	if err := w.buf.FlushScope(ctx, w.database); err != nil {
		return err
	}
	return w.buf.WaitBufferedBelow(ctx, w.limit, w.maxWait)
}

// finish turns an error of the write pass into a failure part-way once a
// row was written, whatever it is (a decode error a validation pass cannot
// see, a read error): any answer but the partial one would invite a re-run
// that writes those rows again.
func (w *importWriter) finish(err error) error {
	if err == nil || !w.started() {
		return err
	}
	if _, ok := asImportPartial(err); ok {
		return err
	}
	return w.partial(err, "")
}

// started reports whether any row of the import was written.
func (w *importWriter) started() bool { return w.rows > 0 }

// importPartialError is a failure after some of the import was written: not
// retryable as a whole, the answer says what was written (#1070).
type importPartialError struct {
	Err          error
	RowsWritten  int64
	Measurements []string
	StoppedAt    string
}

func (e *importPartialError) Error() string {
	return fmt.Sprintf("import stopped after writing %d rows; running the whole file again writes them again: %v", e.RowsWritten, e.Err)
}

func (e *importPartialError) Unwrap() error { return e.Err }

func (w *importWriter) partial(err error, stoppedAt string) *importPartialError {
	return &importPartialError{Err: err, RowsWritten: w.rows, Measurements: append([]string(nil), w.order...), StoppedAt: stoppedAt}
}

// respondImportPartial answers a failure part-way: 500, without the refusal
// headers and Retry-After, which promise nothing was written.
func respondImportPartial(c *fiber.Ctx, pe *importPartialError) error {
	body := fiber.Map{
		"error":                pe.Error(),
		"rows_written":         pe.RowsWritten,
		"written_measurements": pe.Measurements,
	}
	if pe.StoppedAt != "" {
		body["stopped_at_measurement"] = pe.StoppedAt
	}
	return c.Status(fiber.StatusInternalServerError).JSON(body)
}

// asImportPartial reports whether err is a failure part-way.
func asImportPartial(err error) (*importPartialError, bool) {
	var pe *importPartialError
	return pe, errors.As(err, &pe)
}
