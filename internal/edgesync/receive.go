package edgesync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"path"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// StagingPrefix is where partially-received and unverified files live.
//
// It sits outside every database's namespace so a staged file can never be
// mistaken for queryable data: Arc's storage layout is
// {database}/{measurement}/{y}/{m}/{d}/{h}/file.parquet, and a leading dot
// cannot be a database name (validateSyncPath rejects it).
const StagingPrefix = ".sync-staging"

// ErrReceiveInternal wraps a failure that is the HUB's fault rather than the
// spoke's — storage I/O, a manifest write during a Raft election, a promote
// that could not complete.
//
// The distinction is not cosmetic: a spoke told "bad request" has no reason to
// retry, while a transient hub-side failure is exactly what it should retry.
// Handlers map this to 503 rather than 400.
var ErrReceiveInternal = errors.New("edgesync: receive failed for a hub-side reason")

// Receiver implements the hub side of a file transfer: it accepts bytes from a
// spoke, verifies them, and only then makes them visible.
//
// The ordering is the point. Bytes stream into a staging path while a SHA-256
// runs over them; the digest is compared against what the spoke declared; and
// only on a match is the file promoted into its final namespaced location.
// A mismatch never produces a byte at the path a reader would look at.
type Receiver struct {
	backend storage.Backend
	logger  zerolog.Logger

	// index records what the hub holds so reconcile can answer without
	// reading parquet bytes. Nil disables recording — reconcile then reports
	// every file as missing, which is safe (the spoke re-sends and gets
	// AlreadyPresent) but wasteful, so the hub always wires one.
	index *HubIndex

	// registerFile publishes a committed file so readers can see it. In
	// cluster mode this proposes to the Raft manifest; standalone it is nil,
	// because a local backend's directory listing IS the manifest.
	//
	// Nil is a supported configuration, not an error — the hub must work in
	// OSS standalone, where no coordinator exists.
	registerFile func(ctx context.Context, f *ReceivedFile) error
}

// ReceivedFile describes a file that has been verified and promoted.
type ReceivedFile struct {
	// SpokeID is the sender. The hub namespaces by it, so this is what keeps
	// two edges writing the same measurement from colliding.
	SpokeID string

	// Path is the final storage-relative path, already namespaced.
	Path string

	// SourcePath is the path as the spoke knows it, before namespacing —
	// carried so logs and manifests can be correlated with the spoke's ledger.
	SourcePath string

	SHA256    string
	SizeBytes int64
}

// Database and Measurement extract the Arc namespace a received file belongs
// to, from the spoke's own path.
//
// Arc's layout is {database}/{measurement}/{y}/{m}/{d}/{h}/file.parquet, and
// SourcePath is the spoke's path BEFORE hub namespacing — so the database and
// measurement are its first two segments. Deriving them from SourcePath rather
// than the namespaced Path is deliberate: the hub prepends the spoke ID, which
// would otherwise be read as the database name.
//
// Both return "" for a path too short to carry them. validateSyncPath has
// already rejected traversal and absolute paths by the time these are called.
func (f *ReceivedFile) Database() string {
	if db, _, ok := splitFirstTwo(f.SourcePath); ok {
		return db
	}
	return ""
}

func (f *ReceivedFile) Measurement() string {
	if _, m, ok := splitFirstTwo(f.SourcePath); ok {
		return m
	}
	return ""
}

// PartitionTime is the hour partition the file belongs to, parsed from the
// spoke's path.
//
// This is not cosmetic metadata. raft.FileEntry.PartitionTime drives hot/cold
// routing: tiering/router.go filters files by it when a query carries a time
// range, and tiering/migrator.go computes file age from it to decide what to
// migrate. A zero value would make every synced file look infinitely old to
// the migrator and drop it from time-ranged queries entirely.
//
// Returns the zero time when the path does not carry a parseable partition —
// callers should treat that as "unknown" rather than "epoch".
func (f *ReceivedFile) PartitionTime() time.Time {
	// {database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/file.parquet
	parts := strings.Split(f.SourcePath, "/")
	if len(parts) < 7 {
		return time.Time{}
	}
	year, month, day, hour := parts[2], parts[3], parts[4], parts[5]
	t, err := time.Parse("2006/01/02/15", year+"/"+month+"/"+day+"/"+hour)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

func splitFirstTwo(p string) (first, second string, ok bool) {
	parts := strings.SplitN(p, "/", 3)
	if len(parts) < 3 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// ReceiverConfig configures a Receiver.
type ReceiverConfig struct {
	Backend storage.Backend
	Logger  zerolog.Logger

	// Index records received files for reconcile. Optional but strongly
	// recommended; without it reconcile cannot report anything as present.
	Index *HubIndex

	// RegisterFile is called after a file is verified and promoted. Leave nil
	// in standalone mode.
	RegisterFile func(ctx context.Context, f *ReceivedFile) error
}

// NewReceiver validates configuration and returns a ready Receiver.
func NewReceiver(cfg ReceiverConfig) (*Receiver, error) {
	if cfg.Backend == nil {
		return nil, errors.New("edgesync: receiver requires a storage backend")
	}
	return &Receiver{
		backend: cfg.Backend,
		index:   cfg.Index,
		// No .Str("component", ...) here: logger.Get already sets it, and a
		// second one emits a duplicate JSON key that strict parsers and log
		// aggregators mishandle.
		logger:       cfg.Logger,
		registerFile: cfg.RegisterFile,
	}, nil
}

// SupportsResume reports whether this hub can accept a partial transfer and
// continue it later.
//
// Only backends implementing storage.AppendingBackend can — S3 and Azure
// cannot append to a block object. On those, a dropped transfer restarts from
// zero rather than resuming, which is a throughput cost on intermittent links,
// not a correctness problem. Handlers surface this so a spoke does not send an
// offset the hub cannot honor.
func (r *Receiver) SupportsResume() bool {
	_, ok := r.backend.(storage.AppendingBackend)
	return ok
}

// Receive streams one file from a spoke, verifies it, and promotes it.
//
// offset resumes a previous partial transfer: body must carry only the bytes
// from offset onward. declaredSHA256 is the digest of the WHOLE file, so a
// resumed transfer is still verified end to end — the staged prefix is hashed
// before the tail is appended.
//
// The returned PutResult mirrors what the spoke's transport expects, so an
// HTTP handler maps it to a status code without re-deriving the semantics.
func (r *Receiver) Receive(ctx context.Context, spokeID, sourcePath, declaredSHA256 string, declaredSize, offset int64, body io.Reader) (*PutResult, error) {
	if err := validateSpokeID(spokeID); err != nil {
		return nil, err
	}
	if err := validateSyncPath(sourcePath); err != nil {
		return nil, err
	}
	if declaredSize < 0 {
		return nil, fmt.Errorf("edgesync: negative declared size %d", declaredSize)
	}
	if offset < 0 || offset > declaredSize {
		return nil, fmt.Errorf("edgesync: offset %d out of range for a %d-byte file", offset, declaredSize)
	}
	if !isHexSHA256(declaredSHA256) {
		return nil, fmt.Errorf("edgesync: declared sha256 %q is not a 64-character hex digest", declaredSHA256)
	}

	finalPath := NamespacedPath(spokeID, sourcePath)
	stagingPath := stagingPathFor(spokeID, sourcePath)

	// §6.1 identity check, before reading a single byte of the body. A
	// duplicate costs no transfer, and a conflict must not consume bytes it
	// will discard.
	//
	// Exists, not StatFile. LocalBackend.StatFile deliberately falls back to
	// the "{path}.part" staging file when the final file is absent, which is
	// right for the peer-fetch puller (it wants to know how much of a partial
	// it already holds) and wrong here: a stale .part left by an interrupted
	// promote would be read as "the file exists", and the resolveExisting
	// branch would then fail forever because ReadTo is NOT .part-aware. That
	// wedges the path permanently — no retry could ever clear it. Exists()
	// checks only the real file.
	exists, err := r.backend.Exists(ctx, finalPath)
	if err != nil {
		return nil, fmt.Errorf("%w: stat %q: %w", ErrReceiveInternal, finalPath, err)
	}
	if exists {
		size, err := r.backend.StatFile(ctx, finalPath)
		if err != nil {
			return nil, fmt.Errorf("%w: stat %q: %w", ErrReceiveInternal, finalPath, err)
		}
		return r.resolveExisting(ctx, spokeID, sourcePath, finalPath, declaredSHA256, size)
	}

	// Hash any already-staged prefix so the final digest covers the whole
	// file, not just the tail this request carries.
	hasher := sha256.New()
	if offset > 0 {
		if !r.SupportsResume() {
			// Discard the stale staging object: this backend cannot append,
			// so the spoke must restart from zero and a leftover prefix would
			// only mislead the next attempt.
			_ = r.backend.Delete(ctx, stagingPath)
			return nil, storage.ErrResumeNotSupported
		}
		staged, err := r.stagedSize(ctx, stagingPath)
		if err != nil {
			return nil, fmt.Errorf("%w: stat staging %q: %w", ErrReceiveInternal, stagingPath, err)
		}
		if staged != offset {
			// The spoke's checkpoint disagrees with what the hub holds.
			// Answering with the truth lets it resume correctly instead of
			// appending at the wrong place and corrupting the file.
			if staged < 0 {
				staged = 0
			}
			// A staged prefix at least as long as the newly-declared size
			// means the two sides disagree about the FILE, not just the
			// offset — the spoke has re-declared the same path at a smaller
			// size, so the staged bytes belong to a different version. Report
			// no progress and discard them: reporting `staged` here would
			// return BytesAccepted > SizeBytes, which PutResult.Validate
			// rejects, so the spoke would hard-error on the hub's own reply.
			if staged >= declaredSize {
				_ = r.backend.Delete(ctx, stagingPath)
				_ = r.backend.Delete(ctx, partSuffix(stagingPath))
				return &PutResult{Outcome: OutcomePartial, BytesAccepted: 0}, nil
			}
			return &PutResult{Outcome: OutcomePartial, BytesAccepted: staged}, nil
		}
		if err := r.backend.ReadTo(ctx, partSuffix(stagingPath), hasherWriter{hasher}); err != nil {
			return nil, fmt.Errorf("%w: rehash staged prefix %q: %w", ErrReceiveInternal, stagingPath, err)
		}
	}

	written, err := r.stage(ctx, stagingPath, body, hasher, offset, declaredSize)
	if err != nil {
		return nil, err
	}

	// A short body means the link dropped mid-stream. Keep what arrived —
	// that prefix is the spoke's resume checkpoint — but only where the
	// backend can actually append to it later.
	if total := offset + written; total < declaredSize {
		if !r.SupportsResume() {
			_ = r.backend.Delete(ctx, stagingPath)
			return &PutResult{Outcome: OutcomePartial, BytesAccepted: 0}, nil
		}
		return &PutResult{Outcome: OutcomePartial, BytesAccepted: total}, nil
	}

	// Verify BEFORE promoting. This is the whole reason for a staging path:
	// a mismatch is discarded here, so corrupt bytes never appear at the
	// path a reader or a later reconcile would consult.
	if got := hex.EncodeToString(hasher.Sum(nil)); got != declaredSHA256 {
		r.logger.Warn().
			Str("spoke_id", spokeID).
			Str("path", sourcePath).
			Str("declared", declaredSHA256).
			Str("computed", got).
			Msg("Discarding sync upload: checksum mismatch")
		_ = r.backend.Delete(ctx, stagingPath)
		return &PutResult{Outcome: OutcomeChecksumMismatch}, nil
	}

	if err := r.promote(ctx, stagingPath, finalPath, declaredSize); err != nil {
		return nil, err
	}

	if r.registerFile != nil {
		if err := r.register(ctx, spokeID, sourcePath, finalPath, declaredSHA256, declaredSize); err != nil {
			// The bytes are committed but invisible to readers. Reporting an
			// error is right: deleting a verified transfer over a transient
			// manifest failure would be worse. The spoke retries, and
			// resolveExisting re-attempts registration on that retry — see
			// the note there.
			return nil, err
		}
	}

	// Recorded AFTER registration so the index never claims a file that
	// readers cannot see. The reverse order would make reconcile report a file
	// as present while it is still invisible, and the spoke would mark it
	// synced and stop re-sending.
	if err := r.recordReceived(ctx, spokeID, sourcePath, finalPath, declaredSHA256, declaredSize); err != nil {
		return nil, err
	}

	return &PutResult{Outcome: OutcomeCommitted, BytesAccepted: declaredSize}, nil
}

// recordReceived notes a committed file in the hub index.
func (r *Receiver) recordReceived(ctx context.Context, spokeID, sourcePath, finalPath, sha string, size int64) error {
	if r.index == nil {
		return nil
	}
	err := r.index.Record(ctx, &ReceivedRecord{
		SpokeID:    spokeID,
		SourcePath: sourcePath,
		HubPath:    finalPath,
		SHA256:     sha,
		SizeBytes:  size,
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrReceiveInternal, err)
	}
	return nil
}

// register publishes a committed file to the manifest.
func (r *Receiver) register(ctx context.Context, spokeID, sourcePath, finalPath, sha string, size int64) error {
	f := &ReceivedFile{
		SpokeID:    spokeID,
		Path:       finalPath,
		SourcePath: sourcePath,
		SHA256:     sha,
		SizeBytes:  size,
	}
	if err := r.registerFile(ctx, f); err != nil {
		return fmt.Errorf("%w: register %q: %w", ErrReceiveInternal, finalPath, err)
	}
	return nil
}

// resolveExisting applies §6.1 to a path the hub already holds.
func (r *Receiver) resolveExisting(ctx context.Context, spokeID, sourcePath, finalPath, declaredSHA256 string, size int64) (*PutResult, error) {
	existingSHA, err := r.hashStored(ctx, finalPath)
	if err != nil {
		return nil, fmt.Errorf("%w: hash existing %q: %w", ErrReceiveInternal, finalPath, err)
	}

	if existingSHA == declaredSHA256 {
		// Redelivery of identical content — the lost-ack case. The bytes are a
		// no-op, which is what turns at-least-once delivery into exactly-once
		// effect.
		//
		// Registration is NOT a no-op, though. If a previous attempt committed
		// the bytes but failed its manifest write (a Raft election, a quorum
		// blip), the file is on disk and invisible to every reader. Returning
		// AlreadyPresent without retrying would make that permanent: the spoke
		// marks the file synced and never sends it again, so nothing would
		// ever register it. Re-attempting is idempotent — registering a file
		// the manifest already has is a no-op — so the only cost is one extra
		// proposal on a path that is already rare.
		if r.registerFile != nil {
			if err := r.register(ctx, spokeID, sourcePath, finalPath, declaredSHA256, size); err != nil {
				return nil, err
			}
		}
		// Re-record for the same reason registration is re-attempted: if a
		// previous attempt committed the bytes but failed to index them, the
		// hub would keep reporting this file as missing and the spoke would
		// keep re-sending it forever.
		if err := r.recordReceived(ctx, spokeID, sourcePath, finalPath, declaredSHA256, size); err != nil {
			return nil, err
		}
		return &PutResult{Outcome: OutcomeAlreadyPresent, BytesAccepted: size}, nil
	}

	// Same path, different content. Never overwrite: one of the two copies is
	// wrong and silently replacing either destroys evidence. This is an
	// operator alarm, not a retry.
	return &PutResult{Outcome: OutcomeConflict, TheirSHA256: existingSHA}, nil
}

// stage writes the incoming bytes to the staging path, tee'd through hasher.
//
// A short body must leave a RESUMABLE partial rather than a promoted short
// file, and that constrains how the backend is called. LocalBackend writes
// through a "{path}.part" staging file and renames it to the final name when
// the call returns cleanly — so handing WriteReader a short reader would
// promote a truncated file. Returning an error from the reader instead leaves
// the .part in place, which is exactly the state AppendReader later extends.
// This mirrors how the peer-fetch puller resumes (filereplication/puller.go).
func (r *Receiver) stage(ctx context.Context, stagingPath string, body io.Reader, hasher hash.Hash, offset, declaredSize int64) (int64, error) {
	// Cap the reader at what the spoke declared. Without this a spoke could
	// stream unbounded bytes into hub storage under a small declared size.
	expected := declaredSize - offset
	counted := &countingReader{r: io.TeeReader(io.LimitReader(body, expected), hasher)}
	guarded := &shortBodyGuard{r: counted, expected: expected}

	if offset > 0 {
		ab, ok := r.backend.(storage.AppendingBackend)
		if !ok {
			return 0, storage.ErrResumeNotSupported
		}
		err := ab.AppendReader(ctx, stagingPath, guarded, expected)
		if errors.Is(err, errShortBody) {
			return counted.n, nil
		}
		if err != nil {
			return counted.n, fmt.Errorf("%w: append staging %q: %w", ErrReceiveInternal, stagingPath, err)
		}
		return counted.n, nil
	}

	err := r.backend.WriteReader(ctx, stagingPath, guarded, expected)
	if errors.Is(err, errShortBody) {
		// Deliberate: the .part survives for a later resume.
		return counted.n, nil
	}
	if err != nil {
		return counted.n, fmt.Errorf("%w: write staging %q: %w", ErrReceiveInternal, stagingPath, err)
	}
	return counted.n, nil
}

// stagedSize reports how many bytes of a staged file the hub holds, whether it
// sits in the backend's partial (".part") state or was fully written.
func (r *Receiver) stagedSize(ctx context.Context, stagingPath string) (int64, error) {
	if n, err := r.backend.StatFile(ctx, partSuffix(stagingPath)); err != nil {
		return -1, err
	} else if n >= 0 {
		return n, nil
	}
	return r.backend.StatFile(ctx, stagingPath)
}

// partSuffix names the backend's in-progress staging file. LocalBackend writes
// through "{path}.part" and renames on completion; sizing that file is how the
// hub learns where a dropped transfer stopped.
func partSuffix(p string) string { return p + ".part" }

// errShortBody signals that the spoke sent fewer bytes than it declared. It is
// returned from the reader so the backend abandons its write with the partial
// staging file intact, instead of promoting a truncated file.
var errShortBody = errors.New("edgesync: body ended before the declared size")

type shortBodyGuard struct {
	r        io.Reader
	expected int64
	read     int64
}

func (g *shortBodyGuard) Read(p []byte) (int, error) {
	n, err := g.r.Read(p)
	g.read += int64(n)
	if errors.Is(err, io.EOF) && g.read < g.expected {
		return n, errShortBody
	}
	return n, err
}

// promote moves a verified file from staging to its final path.
//
// A copy, not a rename: storage.Backend has no move operation, and inventing
// one would mean touching every backend. Streaming staging -> final uses the
// same pipe pattern as the tiering migrator, so it works identically on local
// disk and object storage.
//
// The staging object is deleted only after the final write succeeds. A crash
// between them leaves a staging orphan, which is harmless — the next transfer
// of the same file overwrites it, and the final path already holds verified
// content. The reverse order could lose the file entirely.
//
// BACKEND REQUIREMENT: the write to finalPath must be atomic — a reader must
// see either the whole file or none of it. Every backend in the tree satisfies
// this (LocalBackend writes to "{path}.part" and renames; S3 and Azure PUTs
// are atomic), but storage.Backend does not state it as a contract. A backend
// that could expose a partial write would break the guarantee this whole
// function exists to provide: the next attempt would find a truncated file,
// hash it, and return a conflict against content the hub itself produced —
// wedging that path the same way a stale ".part" once did.
func (r *Receiver) promote(ctx context.Context, stagingPath, finalPath string, size int64) error {
	// Clear any stale staging file at the DESTINATION before writing. A
	// previous promote that died between WriteReader's staging write and its
	// rename leaves "{finalPath}.part" behind; left in place it makes
	// StatFile report the final file as present (see the note in Receive) and
	// would defeat the Exists() guard for any code that still uses StatFile.
	if err := r.backend.Delete(ctx, partSuffix(finalPath)); err != nil {
		r.logger.Debug().Err(err).Str("path", finalPath).Msg("No stale promote staging file to clear")
	}

	pr, pw := io.Pipe()
	errCh := make(chan error, 2)

	go func() {
		err := r.backend.ReadTo(ctx, stagingPath, pw)
		_ = pw.CloseWithError(err)
		errCh <- err
	}()
	go func() {
		err := r.backend.WriteReader(ctx, finalPath, pr, size)
		_ = pr.CloseWithError(err)
		errCh <- err
	}()

	var firstErr error
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return fmt.Errorf("%w: promote %q -> %q: %w", ErrReceiveInternal, stagingPath, finalPath, firstErr)
	}

	if err := r.backend.Delete(ctx, stagingPath); err != nil {
		// Non-fatal: the file is committed and visible. A leftover staging
		// object costs disk, not correctness.
		r.logger.Warn().Err(err).Str("staging", stagingPath).Msg("Failed to remove staging object after promote")
	}
	return nil
}

// hashStored computes the SHA-256 of a file already in storage, streaming so a
// large file does not have to be buffered.
func (r *Receiver) hashStored(ctx context.Context, p string) (string, error) {
	h := sha256.New()
	if err := r.backend.ReadTo(ctx, p, hasherWriter{h}); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// NamespacedPath rewrites a spoke's path into the hub's namespace.
//
// This is a HUB-SIDE rewrite by design: the spoke sends its own native paths
// and stays unaware of namespacing, so the same spoke can sync to several hubs
// unmodified. Because spoke_id is bound into the request HMAC, a spoke cannot
// claim another's namespace.
func NamespacedPath(spokeID, sourcePath string) string {
	return path.Join(spokeID, sourcePath)
}

func stagingPathFor(spokeID, sourcePath string) string {
	return path.Join(StagingPrefix, spokeID, sourcePath)
}

// validateSpokeID rejects identifiers that could escape their namespace.
//
// The spoke ID becomes the first path segment of everything that spoke writes,
// so a value containing a separator or a traversal element would let it write
// outside its own namespace — defeating §6.3 even though the ID is
// HMAC-bound, because the binding proves WHO is asking, not WHERE they may
// write.
func validateSpokeID(spokeID string) error {
	if spokeID == "" {
		return errors.New("edgesync: spoke ID is required")
	}
	if strings.ContainsAny(spokeID, "/\\") {
		return fmt.Errorf("edgesync: spoke ID %q contains a path separator", spokeID)
	}
	if spokeID == "." || spokeID == ".." || strings.HasPrefix(spokeID, ".") {
		return fmt.Errorf("edgesync: spoke ID %q may not start with a dot", spokeID)
	}
	if strings.ContainsRune(spokeID, 0) {
		return fmt.Errorf("edgesync: spoke ID contains a NUL byte")
	}
	return nil
}

// validateSyncPath rejects paths that would escape the spoke's namespace or
// collide with the staging area.
//
// A spoke is a remote, semi-trusted party — it may be a compromised edge box —
// so its declared path is untrusted input that becomes a filesystem location.
func validateSyncPath(p string) error {
	if p == "" {
		return errors.New("edgesync: path is required")
	}
	if strings.ContainsRune(p, 0) {
		return errors.New("edgesync: path contains a NUL byte")
	}
	if strings.HasPrefix(p, "/") {
		return fmt.Errorf("edgesync: path %q must be relative", p)
	}
	if strings.Contains(p, "\\") {
		return fmt.Errorf("edgesync: path %q contains a backslash", p)
	}
	// Reject ".." ANYWHERE, not only as a whole segment. LocalBackend
	// sanitizes by replacing every ".." substring with "_" (storage/local.go),
	// which is many-to-one: "a..b.parquet" and "a_b.parquet" both land on
	// "a_b.parquet". A spoke sending the former would then collide with an
	// unrelated file, get an unresolvable 409 naming a digest it never
	// uploaded, and leave that path permanently unsyncable.
	//
	// Checked before cleaning: path.Clean would resolve "a/../../b" into
	// "../b", and checking only the cleaned form invites the reader to assume
	// the raw form was safe.
	if strings.Contains(p, "..") {
		return fmt.Errorf("edgesync: path %q contains a parent-directory sequence", p)
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" {
			return fmt.Errorf("edgesync: path %q contains an empty segment", p)
		}
	}
	if strings.HasPrefix(p, ".") {
		return fmt.Errorf("edgesync: path %q may not start with a dot", p)
	}
	if !strings.HasSuffix(p, ".parquet") {
		// The sync unit is an immutable Parquet file. Anything else is either
		// a mistake or an attempt to write something the query layer would
		// never read but an operator might execute.
		return fmt.Errorf("edgesync: path %q is not a .parquet file", p)
	}
	return nil
}

// isHexSHA256 reports whether s is a 64-character lowercase hex digest.
func isHexSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

// hasherWriter adapts a hash.Hash to io.Writer without exposing Sum.
type hasherWriter struct{ h hash.Hash }

func (w hasherWriter) Write(p []byte) (int, error) { return w.h.Write(p) }

// countingReader records how many bytes were actually read, so a short body is
// distinguishable from a complete one.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// SweepStaging deletes abandoned staging files older than maxAge and reports
// how many it removed.
//
// Without this the staging area grows without bound: a spoke that declares a
// large file, sends a few bytes, and never returns leaves a partial behind,
// and nothing else in the system reclaims it. A compromised or merely buggy
// spoke can repeat that with fresh paths until the hub's disk is full.
//
// maxAge must be comfortably longer than a plausible contact gap, because a
// staged prefix IS a legitimate resume checkpoint — sweeping too eagerly turns
// a recoverable transfer into a restart from zero on exactly the link least
// able to afford it.
//
// Cluster note: this deletes from storage only. Staged files are never in the
// manifest (they are unverified by definition), so there is no manifest-before-
// storage ordering to preserve and no Raft proposal to batch.
func (r *Receiver) SweepStaging(ctx context.Context, maxAge time.Duration, now time.Time) (int, error) {
	lister, ok := r.backend.(storage.ObjectLister)
	if !ok {
		// Without modification times there is no safe way to tell an
		// abandoned partial from one mid-transfer, and deleting the latter
		// would destroy a live resume checkpoint.
		return 0, fmt.Errorf("%w: backend does not support listing object metadata", ErrReceiveInternal)
	}

	objects, err := lister.ListObjects(ctx, StagingPrefix+"/")
	if err != nil {
		return 0, fmt.Errorf("%w: list staging: %w", ErrReceiveInternal, err)
	}

	cutoff := now.Add(-maxAge)
	var removed int
	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		if !obj.LastModified.Before(cutoff) {
			continue
		}
		if err := r.backend.Delete(ctx, obj.Path); err != nil {
			// Keep going: one undeletable orphan must not stop the sweep from
			// reclaiming the rest.
			r.logger.Warn().Err(err).Str("path", obj.Path).Msg("Failed to delete abandoned sync staging file")
			continue
		}
		removed++
	}

	if removed > 0 {
		r.logger.Info().
			Int("removed", removed).
			Dur("max_age", maxAge).
			Msg("Swept abandoned sync staging files")
	}
	return removed, nil
}
