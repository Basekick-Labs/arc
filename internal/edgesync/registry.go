package edgesync

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// SecretBytes is the length of a generated spoke secret before hex encoding.
//
// 32 bytes matches the HMAC-SHA256 block security level; anything shorter
// would be the weakest link in a scheme whose whole point is authenticating a
// remote party that can write files.
const SecretBytes = 32

var (
	// ErrSpokeNotFound is returned for an unregistered or deleted spoke.
	ErrSpokeNotFound = errors.New("edgesync: spoke not registered")

	// ErrSpokeExists is returned when registering an ID that is already taken.
	//
	// Deliberately not an upsert: silently replacing a spoke's secret would
	// lock out a live edge box with no signal, and re-registering an existing
	// ID is far more likely to be a mistake than an intent.
	ErrSpokeExists = errors.New("edgesync: spoke already registered")

	// ErrSpokeDisabled is returned when a registered spoke has been disabled.
	ErrSpokeDisabled = errors.New("edgesync: spoke is disabled")
)

// SecretCipher encrypts and decrypts spoke secrets at rest.
//
// Satisfied by mqtt.AESEncryptor. Declared here as an interface so this
// package does not depend on the MQTT one, and so a test can substitute a
// stub without an encryption key.
type SecretCipher interface {
	Encrypt(plaintext string) (string, error)
	Decrypt(ciphertext string) (string, error)
}

// Spoke is a registered edge instance.
//
// The secret is deliberately absent: it is returned exactly once, from
// Register or RotateSecret, and is never readable again.
type Spoke struct {
	SpokeID       string
	Name          string
	Enabled       bool
	LastSeenAt    *time.Time
	FilesReceived int64
	BytesReceived int64
	RegisteredAt  time.Time
}

// Registry stores which spokes may sync to this hub, and their secrets.
type Registry struct {
	db     *sql.DB
	cipher SecretCipher
	logger zerolog.Logger
}

// NewRegistry creates the registry and initializes its schema.
//
// A cipher is required. §8.1 specifies a `secret_hash` column, but a one-way
// hash cannot work here: HMAC verification recomputes the MAC from the secret,
// so the hub needs the plaintext at request time — unlike an API token, which
// is only ever checked against a presented value. The secret is therefore
// encrypted at rest rather than hashed, and a hub without a key refuses to
// start rather than silently storing write credentials in the clear.
func NewRegistry(db *sql.DB, cipher SecretCipher, logger zerolog.Logger) (*Registry, error) {
	if db == nil {
		return nil, errors.New("edgesync: registry requires a non-nil database")
	}
	if cipher == nil {
		return nil, errors.New("edgesync: registry requires a cipher; spoke secrets must not be stored in plaintext")
	}
	// A nil check is not enough. mqtt.NewPasswordEncryptor returns a
	// pass-through encryptor for an empty key — non-nil, and it satisfies this
	// interface — so a caller that forgot to check the key would store every
	// spoke secret in plaintext while this constructor reported success.
	//
	// Round-trip a canary instead of trusting the type: if the "ciphertext"
	// still contains the plaintext, the cipher is not encrypting and the hub
	// must not start. This makes the invariant structural rather than a
	// property of one nil-check in a caller forty lines away.
	if err := verifyCipherEncrypts(cipher); err != nil {
		return nil, err
	}

	r := &Registry{db: db, cipher: cipher, logger: logger}
	if err := r.initSchema(); err != nil {
		return nil, fmt.Errorf("edgesync: initialize registry schema: %w", err)
	}
	return r, nil
}

func (r *Registry) initSchema() error {
	const schema = `
	CREATE TABLE IF NOT EXISTS sync_spokes (
		spoke_id         TEXT PRIMARY KEY,
		name             TEXT NOT NULL,
		-- Encrypted, not hashed: the hub must recompute an HMAC from this, so
		-- it has to be recoverable. Named for what it holds so the storage
		-- model cannot be misread from the schema.
		secret_encrypted TEXT NOT NULL,
		enabled          INTEGER NOT NULL DEFAULT 1,
		last_seen_at     TIMESTAMP,
		files_received   INTEGER NOT NULL DEFAULT 0,
		bytes_received   INTEGER NOT NULL DEFAULT 0,
		registered_at    TIMESTAMP NOT NULL
	);
	`

	if _, err := r.db.Exec(schema); err != nil {
		return fmt.Errorf("create sync_spokes table: %w", err)
	}
	r.logger.Debug().Msg("Sync spoke registry schema initialized")
	return nil
}

// Register creates a spoke and returns its secret.
//
// The secret is generated here rather than supplied by the caller, and this is
// the ONLY time it is readable — the operator must capture it from this
// response to configure the edge box. Generating it removes the failure mode
// where an operator or an automation picks something weak or reuses one across
// the fleet.
func (r *Registry) Register(ctx context.Context, spokeID, name string) (secret string, err error) {
	if err := validateSpokeID(spokeID); err != nil {
		return "", err
	}
	if strings.TrimSpace(name) == "" {
		// A human-readable label is what an operator uses to tell "rocket-07"
		// from "rocket-08" in an alert six months from now.
		return "", errors.New("edgesync: spoke name is required")
	}
	// Bounded and control-character-free. The name is echoed into logs and API
	// responses, so an unbounded one bloats every list response and an
	// embedded newline lets a registration forge log lines against whatever
	// tooling reads them. 128 matches the cap Arc applies elsewhere.
	if len(name) > 128 {
		return "", fmt.Errorf("edgesync: spoke name is %d bytes; the maximum is 128", len(name))
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return "", errors.New("edgesync: spoke name may not contain control characters")
		}
	}

	secret, err = GenerateSecret()
	if err != nil {
		return "", err
	}
	encrypted, err := r.cipher.Encrypt(secret)
	if err != nil {
		return "", fmt.Errorf("edgesync: encrypt spoke secret: %w", err)
	}

	// INSERT, never upsert. Replacing an existing spoke's secret would lock
	// out a live edge box with no signal to anyone.
	res, err := r.db.ExecContext(ctx, `
		INSERT INTO sync_spokes (spoke_id, name, secret_encrypted, enabled, registered_at)
		VALUES (?, ?, ?, 1, ?)
		ON CONFLICT(spoke_id) DO NOTHING`,
		spokeID, name, encrypted, time.Now().UTC())
	if err != nil {
		return "", fmt.Errorf("edgesync: register spoke %q: %w", spokeID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("edgesync: register spoke %q: %w", spokeID, err)
	}
	if n == 0 {
		return "", fmt.Errorf("%q: %w", spokeID, ErrSpokeExists)
	}

	r.logger.Info().Str("spoke_id", spokeID).Str("name", name).Msg("Registered edge sync spoke")
	return secret, nil
}

// Secret returns a spoke's decrypted secret for HMAC verification.
//
// Returns ErrSpokeDisabled for a registered-but-disabled spoke, so a caller
// can log the difference — but callers on the request path MUST NOT surface
// that distinction to the client, since it would let an attacker enumerate
// which spoke IDs exist.
func (r *Registry) Secret(ctx context.Context, spokeID string) (string, error) {
	var encrypted string
	var enabled bool

	err := r.db.QueryRowContext(ctx,
		`SELECT secret_encrypted, enabled FROM sync_spokes WHERE spoke_id = ?`,
		spokeID).Scan(&encrypted, &enabled)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%q: %w", spokeID, ErrSpokeNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("edgesync: look up spoke %q: %w", spokeID, err)
	}
	if !enabled {
		return "", fmt.Errorf("%q: %w", spokeID, ErrSpokeDisabled)
	}

	secret, err := r.cipher.Decrypt(encrypted)
	if err != nil {
		// Almost always a changed or lost ARC_ENCRYPTION_KEY. Worth saying so:
		// the alternative reading — "this spoke's row is corrupt" — sends an
		// operator looking in the wrong place.
		return "", fmt.Errorf("edgesync: decrypt secret for %q (has ARC_ENCRYPTION_KEY changed?): %w", spokeID, err)
	}
	return secret, nil
}

// RotateSecret issues a new secret for an existing spoke and returns it once.
//
// The old secret stops working immediately, so an edge box mid-transfer will
// start failing authentication until it is reconfigured. That is the point of
// rotation, but it means this is not a routine operation.
func (r *Registry) RotateSecret(ctx context.Context, spokeID string) (string, error) {
	secret, err := GenerateSecret()
	if err != nil {
		return "", err
	}
	encrypted, err := r.cipher.Encrypt(secret)
	if err != nil {
		return "", fmt.Errorf("edgesync: encrypt rotated secret: %w", err)
	}

	res, err := r.db.ExecContext(ctx,
		`UPDATE sync_spokes SET secret_encrypted = ? WHERE spoke_id = ?`,
		encrypted, spokeID)
	if err != nil {
		return "", fmt.Errorf("edgesync: rotate secret for %q: %w", spokeID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return "", fmt.Errorf("edgesync: rotate secret for %q: %w", spokeID, err)
	} else if n == 0 {
		return "", fmt.Errorf("%q: %w", spokeID, ErrSpokeNotFound)
	}

	r.logger.Warn().Str("spoke_id", spokeID).Msg("Rotated edge sync spoke secret; the previous secret no longer authenticates")
	return secret, nil
}

// SetEnabled enables or disables a spoke without deleting it.
//
// Disabling is the reversible way to cut a spoke off — its history and byte
// counters survive, and re-enabling does not require re-provisioning a secret.
func (r *Registry) SetEnabled(ctx context.Context, spokeID string, enabled bool) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE sync_spokes SET enabled = ? WHERE spoke_id = ?`, enabled, spokeID)
	if err != nil {
		return fmt.Errorf("edgesync: set enabled for %q: %w", spokeID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("edgesync: set enabled for %q: %w", spokeID, err)
	} else if n == 0 {
		return fmt.Errorf("%q: %w", spokeID, ErrSpokeNotFound)
	}
	return nil
}

// Delete removes a spoke entirely.
//
// The spoke's received files and index entries are deliberately left alone:
// deleting a registration must not delete data the hub was trusted with. An
// operator reclaiming that storage does it explicitly.
func (r *Registry) Delete(ctx context.Context, spokeID string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM sync_spokes WHERE spoke_id = ?`, spokeID)
	if err != nil {
		return fmt.Errorf("edgesync: delete spoke %q: %w", spokeID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("edgesync: delete spoke %q: %w", spokeID, err)
	} else if n == 0 {
		return fmt.Errorf("%q: %w", spokeID, ErrSpokeNotFound)
	}
	r.logger.Info().Str("spoke_id", spokeID).Msg("Deleted edge sync spoke registration")
	return nil
}

// Get returns a spoke's metadata, never its secret.
func (r *Registry) Get(ctx context.Context, spokeID string) (*Spoke, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT spoke_id, name, enabled, last_seen_at, files_received, bytes_received, registered_at
		FROM sync_spokes WHERE spoke_id = ?`, spokeID)

	s, err := scanSpoke(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%q: %w", spokeID, ErrSpokeNotFound)
	}
	return s, err
}

// List returns every registered spoke, newest first.
func (r *Registry) List(ctx context.Context) ([]*Spoke, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT spoke_id, name, enabled, last_seen_at, files_received, bytes_received, registered_at
		FROM sync_spokes ORDER BY registered_at DESC, spoke_id DESC`)
	if err != nil {
		return nil, fmt.Errorf("edgesync: list spokes: %w", err)
	}
	defer rows.Close()

	var out []*Spoke
	for rows.Next() {
		s, err := scanSpoke(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("edgesync: iterate spokes: %w", err)
	}
	return out, nil
}

// RecordActivity updates a spoke's last-seen time and transfer counters.
//
// Best-effort by design: this is observability, and failing a verified,
// committed transfer because a counter could not be bumped would trade real
// data for a statistic. Callers log the error and carry on.
func (r *Registry) RecordActivity(ctx context.Context, spokeID string, files, bytes int64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE sync_spokes
		SET last_seen_at = ?, files_received = files_received + ?, bytes_received = bytes_received + ?
		WHERE spoke_id = ?`,
		time.Now().UTC(), files, bytes, spokeID)
	if err != nil {
		return fmt.Errorf("edgesync: record activity for %q: %w", spokeID, err)
	}
	return nil
}

// verifyCipherEncrypts confirms a cipher actually transforms its input.
//
// Catches a pass-through implementation, which would otherwise satisfy
// SecretCipher while storing credentials in the clear.
func verifyCipherEncrypts(c SecretCipher) error {
	const canary = "edgesync-cipher-canary"

	sealed, err := c.Encrypt(canary)
	if err != nil {
		return fmt.Errorf("edgesync: cipher failed a self-test: %w", err)
	}
	if sealed == canary || strings.Contains(sealed, canary) {
		return errors.New("edgesync: the configured cipher does not encrypt (is ARC_ENCRYPTION_KEY set?); " +
			"refusing to store spoke secrets in plaintext")
	}

	opened, err := c.Decrypt(sealed)
	if err != nil {
		return fmt.Errorf("edgesync: cipher failed to decrypt its own output: %w", err)
	}
	if opened != canary {
		return errors.New("edgesync: the configured cipher does not round-trip; spoke secrets would be unrecoverable")
	}
	return nil
}

// Count returns how many spokes are registered.
//
// Used at startup to tell an operator whether the hub can accept anything.
// A COUNT rather than len(List(...)) so it does not scan and allocate a row
// per spoke just to produce a number.
func (r *Registry) Count(ctx context.Context) (int64, error) {
	var n int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sync_spokes`).Scan(&n); err != nil {
		return 0, fmt.Errorf("edgesync: count spokes: %w", err)
	}
	return n, nil
}

// VerifyStoredSecrets checks that the configured key can still decrypt what is
// already stored, and reports how many spokes are registered.
//
// Called at startup because a changed or lost ARC_ENCRYPTION_KEY is systemic
// and otherwise invisible: the admin endpoints keep returning 200 with a full
// spoke list (metadata is not encrypted), so the hub looks healthy while every
// spoke fails authentication. The first failed sync would reveal it, but that
// might be a contact window away — and on an edge deployment, a missed window
// can be hours.
//
// One spoke is enough to prove the key: they are all encrypted under it, so
// either it works or nothing does. Decrypting the whole fleet would cost a
// linear scan at every boot for no extra signal.
func (r *Registry) VerifyStoredSecrets(ctx context.Context) (registered int64, err error) {
	registered, err = r.Count(ctx)
	if err != nil {
		return 0, err
	}
	if registered == 0 {
		return 0, nil
	}

	var spokeID string
	if err := r.db.QueryRowContext(ctx,
		`SELECT spoke_id FROM sync_spokes ORDER BY spoke_id LIMIT 1`).Scan(&spokeID); err != nil {
		return registered, fmt.Errorf("edgesync: read a spoke for the key check: %w", err)
	}

	// Secret() returns ErrSpokeDisabled for a disabled spoke without
	// attempting a decrypt, which would make the check vacuous — read the
	// column directly so the key is exercised regardless of enabled state.
	var encrypted string
	if err := r.db.QueryRowContext(ctx,
		`SELECT secret_encrypted FROM sync_spokes WHERE spoke_id = ?`, spokeID).Scan(&encrypted); err != nil {
		return registered, fmt.Errorf("edgesync: read a stored secret for the key check: %w", err)
	}
	if _, err := r.cipher.Decrypt(encrypted); err != nil {
		return registered, fmt.Errorf(
			"edgesync: cannot decrypt stored spoke secrets — ARC_ENCRYPTION_KEY appears to have changed since %d spoke(s) were registered; "+
				"restore the original key, or re-register every spoke: %w", registered, err)
	}
	return registered, nil
}

// GenerateSecret returns a cryptographically random hex-encoded secret.
func GenerateSecret() (string, error) {
	b := make([]byte, SecretBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("edgesync: generate spoke secret: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func scanSpoke(s rowScanner) (*Spoke, error) {
	var (
		sp         Spoke
		lastSeen   sql.NullTime
		registered time.Time
	)
	if err := s.Scan(&sp.SpokeID, &sp.Name, &sp.Enabled, &lastSeen,
		&sp.FilesReceived, &sp.BytesReceived, &registered); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		return nil, fmt.Errorf("edgesync: scan spoke: %w", err)
	}
	sp.RegisteredAt = registered.UTC()
	if lastSeen.Valid {
		t := lastSeen.Time.UTC()
		sp.LastSeenAt = &t
	}
	return &sp, nil
}
