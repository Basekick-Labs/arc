package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// LicenseBootRecord is the committed handoff certificate. Owner records the
// original handoff's provenance; each boot selects one serving participant.
// Paths name the original owner's verified destination, identically on every
// participant. A newly selected owner uses its separately verified local copy.
type LicenseBootRecord struct {
	Version         int               `json:"version"`
	ClusterName     string            `json:"cluster_name"`
	Generation      uint64            `json:"generation"`
	Owner           string            `json:"owner"`
	Controller      string            `json:"controller"`
	Participants    map[string]string `json:"participants"`
	StoragePath     string            `json:"storage_path"`
	WALPath         string            `json:"wal_path"`
	InventorySHA256 string            `json:"inventory_sha256"`
	// LocalNodeID is retained in each prepared copy before the process changes.
	// The canonical certificate carries its original owner's value; bootstrap
	// resolves this participant using its separate local prepared record.
	LocalNodeID string `json:"local_node_id,omitempty"`
}

func (r *LicenseBootRecord) validate(clusterName string) error {
	if r == nil || r.Version != 1 || r.Generation == 0 || r.ClusterName == "" || r.ClusterName != clusterName || strings.ContainsRune(clusterName, 0) {
		return errors.New("invalid license bootstrap version, generation or cluster")
	}
	if len(r.Participants) == 0 || len(r.Participants) > 4096 || r.Participants[r.Owner] == "" || r.Participants[r.Controller] == "" {
		return errors.New("license bootstrap owner and controller must be participants")
	}
	if r.LocalNodeID != "" && r.Participants[r.LocalNodeID] == "" {
		return errors.New("local license bootstrap identity is not a participant")
	}
	addresses := make(map[string]bool, len(r.Participants))
	for id, addr := range r.Participants {
		host, port, err := net.SplitHostPort(addr)
		p, numberErr := strconv.Atoi(port)
		if id == "" || strings.ContainsRune(id+addr, 0) || err != nil || host == "" || numberErr != nil || p < 1 || p > 65535 || addresses[addr] {
			return errors.New("invalid or duplicate license bootstrap participant address")
		}
		addresses[addr] = true
	}
	for _, path := range []string{r.StoragePath, r.WALPath} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) || path == filepath.VolumeName(path)+string(filepath.Separator) {
			return errors.New("license bootstrap requires clean absolute data paths below the filesystem root")
		}
	}
	if r.StoragePath == r.WALPath {
		return errors.New("license bootstrap storage and WAL paths must differ")
	}
	digest, err := hex.DecodeString(r.InventorySHA256)
	if err != nil || len(digest) != sha256.Size {
		return errors.New("invalid license bootstrap inventory digest")
	}
	return nil
}

// ReadLicenseBootRecord returns nil for a missing certificate and fails closed
// for corrupt, unsupported or cross-cluster state. Call before data-side effects.
func ReadLicenseBootRecord(path, clusterName string) (*LicenseBootRecord, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return nil, errors.New("invalid license bootstrap record file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var r LicenseBootRecord
	dec := json.NewDecoder(io.LimitReader(f, (1<<20)+1))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return nil, fmt.Errorf("read license bootstrap: %w", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, errors.New("trailing license bootstrap record content")
	}
	if err := r.validate(clusterName); err != nil {
		return nil, err
	}
	return &r, nil
}

// WriteLicenseBootRecord atomically publishes a fully synced, private record.
// Callers serialize generation changes under their lifecycle/process lock.
func WriteLicenseBootRecord(path string, r *LicenseBootRecord) error {
	if r == nil {
		return errors.New("nil license bootstrap record")
	}
	if err := r.validate(r.ClusterName); err != nil {
		return err
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	old, err := ReadLicenseBootRecord(path, r.ClusterName)
	if err != nil {
		return err
	}
	if old != nil {
		previous, _ := json.Marshal(old)
		if old.Generation > r.Generation || (old.Generation == r.Generation && !bytes.Equal(previous, data)) {
			return errors.New("license bootstrap certificate regression or conflicting generation")
		}
	}
	dir := filepath.Dir(path)
	// If this is the first certificate, persist newly created directory entries
	// as well as the final rename. Syncing only dir would not persist dir's own
	// link in its parent after a power loss.
	var parents []string
	for p := dir; ; p = filepath.Dir(p) {
		if _, err := os.Stat(p); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		parent := filepath.Dir(p)
		if parent == p {
			return errors.New("license bootstrap record has no existing parent")
		}
		parents = append(parents, parent)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	for i := len(parents) - 1; i >= 0; i-- {
		if err := syncDir(parents[i]); err != nil {
			return err
		}
	}
	f, err := os.CreateTemp(dir, ".license-boot-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	return syncDir(dir)
}

type licenseBootStatus struct {
	Owner             string `json:"owner"`
	Generation        uint64 `json:"generation"`
	RaftOff           bool   `json:"raft_off"`
	CertificateSHA256 string `json:"certificate_sha256"`
	Incarnation       string `json:"incarnation"`
	Serving           bool   `json:"serving"`
	EffectiveOwner    string `json:"effective_owner"`
	PreferredOwner    string `json:"preferred_owner,omitempty"`
	PreparedElection  string `json:"prepared_election,omitempty"`
}

const licensePreparePrefix = "license_boot_prepare_v1:"

type licenseBootElection struct {
	Owner             string            `json:"owner"`
	PreferredOwner    string            `json:"preferred_owner,omitempty"`
	Incarnations      map[string]string `json:"incarnations"`
	CertificateSHA256 string            `json:"certificate_sha256"`
}

func (e *licenseBootElection) digest() string {
	data, _ := json.Marshal(e)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Every participant computes the same owner from the complete boot vector.
// Incarnations change only across processes, so there is no timeout or health
// threshold that can elect a replacement for an unreachable running owner.
func licenseBootOwner(incarnations map[string]string) string {
	data, _ := json.Marshal(incarnations) // encoding/json sorts map keys.
	seed := sha256.Sum256(data)
	ids := make([]string, 0, len(incarnations))
	for id := range incarnations {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var owner, best string
	for _, id := range ids {
		sum := sha256.Sum256(append(append([]byte(nil), seed[:]...), []byte("\x00"+id)...))
		score := hex.EncodeToString(sum[:])
		if owner == "" || score > best {
			owner, best = id, score
		}
	}
	return owner
}

// LicenseBootstrap serves authenticated boot status and unanimous preparation,
// independently of Raft. Start only before data services or after process
// replacement. Close only AFTER all serving data services have stopped.
type LicenseBootstrap struct {
	record                 LicenseBootRecord
	nodeID, secret, digest string
	listener               net.Listener
	tlsConfig              *tls.Config
	nonces                 *security.NonceCache
	lock                   *os.File
	done                   chan struct{}
	closeOnce              sync.Once
	wg                     sync.WaitGroup
	mu                     sync.Mutex
	waitMu                 sync.Mutex
	incarnation            string
	selected               string
	serving                bool
	preferSelf             bool
	promise                *licenseBootElection
	crashReleaseApproved   bool
}

// StartLicenseBootstrap claims the local state directory and coordinator port.
// The caller must retain this object for the full OSS/parked process lifetime.
func StartLicenseBootstrap(cfg *config.ClusterConfig, record *LicenseBootRecord, logger zerolog.Logger, preferSelf ...bool) (*LicenseBootstrap, error) {
	if cfg == nil || cfg.SharedSecret == "" || cfg.RaftDataDir == "" || cfg.CoordinatorAddr == "" {
		return nil, errors.New("bootstrap requires cluster identity, secret, state directory and coordinator address")
	}
	if err := record.validate(cfg.ClusterName); err != nil {
		return nil, err
	}
	id := ResolveNodeID(cfg.NodeID)
	if record.Participants[id] == "" {
		return nil, errors.New("local node is absent from license bootstrap certificate")
	}
	var incarnation [16]byte
	if _, err := rand.Read(incarnation[:]); err != nil {
		return nil, err
	}
	tlsCfg, err := security.ClusterTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.RaftDataDir, 0700); err != nil {
		return nil, err
	}
	lock, err := acquireLicenseBootLock(filepath.Join(cfg.RaftDataDir, "license-bootstrap.lock"))
	if err != nil {
		return nil, err
	}
	listener, err := security.Listen("tcp", cfg.CoordinatorAddr, tlsCfg)
	if err != nil {
		lock.Close()
		return nil, err
	}
	data, _ := json.Marshal(record)
	var owned LicenseBootRecord
	_ = json.Unmarshal(data, &owned) // Own the map; caller changes cannot alter the certificate.
	sum := sha256.Sum256(data)
	b := &LicenseBootstrap{record: owned, nodeID: id, secret: cfg.SharedSecret, digest: hex.EncodeToString(sum[:]), incarnation: hex.EncodeToString(incarnation[:]), listener: listener, tlsConfig: tlsCfg, nonces: security.NewNonceCache(security.HMACTimestampTolerance), lock: lock, done: make(chan struct{})}
	// A renewal-triggered exec retains the current data owner. This is an
	// internal process preference, never an automatic failover permission.
	b.preferSelf = len(preferSelf) > 0 && preferSelf[0]
	b.wg.Add(1)
	go b.serve()
	logger.Info().Str("node_id", id).Uint64("generation", record.Generation).Msg("License bootstrap control ready; Raft is off")
	return b, nil
}

func (b *LicenseBootstrap) serve() {
	defer b.wg.Done()
	slots := make(chan struct{}, 32)
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			return
		}
		select {
		case slots <- struct{}{}:
			b.wg.Add(1)
			go func() { defer b.wg.Done(); defer func() { <-slots }(); defer conn.Close(); b.handle(conn) }()
		default:
			conn.Close()
		}
	}
}

func (b *LicenseBootstrap) authorize(q *protocol.LocalPoliciesQuery) error {
	if q == nil || q.ClusterName != b.record.ClusterName || q.Epoch != b.record.Generation || b.record.Participants[q.NodeID] == "" || q.Nonce == "" || len(q.Nonce) > 128 || strings.ContainsRune(q.Nonce+q.NodeID, 0) {
		return errors.New("invalid license bootstrap request identity")
	}
	if len(q.Kind) > 512<<10 || (q.Kind != "license_boot_v1" && !strings.HasPrefix(q.Kind, licensePreparePrefix) && (q.Kind != "license_release_v1" || q.NodeID != b.record.Controller)) {
		return errors.New("unauthorized license bootstrap operation")
	}
	if err := security.ValidateLocalPoliciesQueryHMAC(b.secret, q.Nonce, q.NodeID, q.ClusterName, q.Kind, q.Epoch, q.Timestamp, q.HMAC, security.HMACTimestampTolerance); err != nil {
		return err
	}
	if !b.nonces.Track(q.NodeID, q.Nonce) {
		return errors.New("replayed license bootstrap request")
	}
	return nil
}

func (b *LicenseBootstrap) handle(conn net.Conn) {
	msg, err := protocol.ReceiveMessage(conn, 3*time.Second)
	if err != nil || msg.Type != protocol.MsgLocalPoliciesQuery {
		return
	}
	q, ok := msg.Payload.(*protocol.LocalPoliciesQuery)
	if !ok || b.authorize(q) != nil {
		return
	}
	status := protocol.LocalPoliciesOK
	if strings.HasPrefix(q.Kind, licensePreparePrefix) {
		var election licenseBootElection
		data, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(q.Kind, licensePreparePrefix))
		if err != nil || json.Unmarshal(data, &election) != nil || b.acceptPrepare(q.NodeID, &election) != nil {
			status = protocol.LocalPoliciesRefused
		}
	}
	rows, _ := json.Marshal(b.status())
	r := &protocol.LocalPoliciesReply{NodeID: b.nodeID, Status: status, Policies: rows, Timestamp: time.Now().Unix()}
	r.HMAC = security.ComputeLocalPoliciesReplyHMAC(b.secret, q.Nonce, r.NodeID, b.record.ClusterName, q.Kind, q.Epoch, r.Status, r.Policies, r.Timestamp)
	_ = protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgLocalPoliciesReply, Payload: r}, 3*time.Second)
}

func (b *LicenseBootstrap) queryPeer(ctx context.Context, id, addr, kind string) (licenseBootStatus, error) {
	var state licenseBootStatus
	conn, err := security.DialContext(ctx, "tcp", addr, 3*time.Second, b.tlsConfig)
	if err != nil {
		return state, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return state, err
	}
	q := &protocol.LocalPoliciesQuery{NodeID: b.nodeID, ClusterName: b.record.ClusterName, Kind: kind, Epoch: b.record.Generation, Nonce: hex.EncodeToString(random[:]), Timestamp: time.Now().Unix()}
	q.HMAC = security.ComputeLocalPoliciesQueryHMAC(b.secret, q.Nonce, q.NodeID, q.ClusterName, q.Kind, q.Epoch, q.Timestamp)
	if err := protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgLocalPoliciesQuery, Payload: q}, 3*time.Second); err != nil {
		return state, err
	}
	msg, err := protocol.ReceiveMessage(conn, 3*time.Second)
	if err != nil {
		return state, err
	}
	r, ok := msg.Payload.(*protocol.LocalPoliciesReply)
	if !ok || msg.Type != protocol.MsgLocalPoliciesReply || r.NodeID != id {
		return state, errors.New("unexpected license bootstrap peer")
	}
	if err := security.ValidateLocalPoliciesReplyHMAC(b.secret, q.Nonce, id, q.ClusterName, q.Kind, q.Epoch, r.Status, r.Policies, r.Timestamp, r.HMAC, security.HMACTimestampTolerance); err != nil {
		return state, err
	}
	if kind == "license_release_v1" {
		if r.Status != protocol.LocalPoliciesOK {
			return state, errors.New("peer has not persisted its sealed certificate")
		}
		// A still-Raft peer acknowledges its durable local release. This is
		// never counted as a bootstrap vote or permission to serve.
		return state, nil
	}
	if kind == lifecycleDurableKind {
		if json.Unmarshal(r.Policies, &state) != nil || r.Status != protocol.LocalPoliciesOK || state.RaftOff || state.Serving || state.Owner != b.record.Owner || state.Generation != b.record.Generation || state.CertificateSHA256 != b.digest {
			return state, errors.New("peer has not persisted the same sealed certificate")
		}
		return state, nil
	}
	if json.Unmarshal(r.Policies, &state) != nil || r.Status != protocol.LocalPoliciesOK || !state.RaftOff || state.Owner != b.record.Owner || state.Generation != b.record.Generation || state.CertificateSHA256 != b.digest {
		return state, errors.New("peer has not entered or accepted the same committed OSS generation")
	}
	incarnation, err := hex.DecodeString(state.Incarnation)
	if err != nil || len(incarnation) != 16 || (state.Serving && state.EffectiveOwner != id) || (state.PreferredOwner != "" && state.PreferredOwner != id) {
		return state, errors.New("invalid bootstrap process identity or serving owner")
	}
	return state, nil
}

func (b *LicenseBootstrap) status() licenseBootStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := licenseBootStatus{Owner: b.record.Owner, Generation: b.record.Generation, RaftOff: true, CertificateSHA256: b.digest, Incarnation: b.incarnation, Serving: b.serving, EffectiveOwner: b.selected}
	if b.preferSelf {
		s.PreferredOwner = b.nodeID
	}
	if b.promise != nil {
		s.PreparedElection = b.promise.digest()
	}
	return s
}

// SelectedOwner returns the effective owner, which may differ from the original
// certificate Owner. Only a successful WaitOwner authorizes local data service.
func (b *LicenseBootstrap) SelectedOwner() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.selected
}

func (b *LicenseBootstrap) acceptPrepare(requester string, election *licenseBootElection) error {
	if election.CertificateSHA256 != b.digest || len(election.Incarnations) != len(b.record.Participants) || requester != election.Owner || b.record.Participants[election.Owner] == "" {
		return errors.New("invalid bootstrap election")
	}
	if (election.PreferredOwner == "" && election.Owner != licenseBootOwner(election.Incarnations)) || (election.PreferredOwner != "" && election.PreferredOwner != election.Owner) {
		return errors.New("invalid bootstrap owner preference")
	}
	for id := range b.record.Participants {
		incarnation, err := hex.DecodeString(election.Incarnations[id])
		if err != nil || len(incarnation) != 16 {
			return errors.New("incomplete bootstrap election")
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.preferSelf && election.Owner != b.nodeID {
		return errors.New("bootstrap renewal retains this node's current data ownership")
	}
	if election.Incarnations[b.nodeID] != b.incarnation {
		return errors.New("bootstrap process incarnation changed")
	}
	if b.promise != nil && b.promise.digest() == election.digest() {
		return nil
	}
	if b.serving || b.promise != nil {
		return errors.New("bootstrap participant already promised another election")
	}
	data, _ := json.Marshal(election)
	b.promise = new(licenseBootElection)
	_ = json.Unmarshal(data, b.promise)
	return nil
}

// observeRound runs only between this node's prepare attempts. In particular,
// an owner candidate NEVER releases its own promise while gathering acks; that
// promise must still match when it starts serving. A running owner never releases
// it. These invariants prevent delayed acks from opening two owners when a peer
// restarts between the status and prepare rounds.
func (b *LicenseBootstrap) observeRound(election *licenseBootElection) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.serving {
		return
	}
	b.selected = election.Owner
	if b.promise != nil {
		for id, incarnation := range election.Incarnations {
			if b.promise.Incarnations[id] != incarnation {
				b.promise = nil
				break
			}
		}
	}
}

func (b *LicenseBootstrap) prepareOwner(ctx context.Context, election *licenseBootElection) error {
	if err := b.acceptPrepare(b.nodeID, election); err != nil {
		return err
	}
	data, _ := json.Marshal(election)
	kind := licensePreparePrefix + base64.RawURLEncoding.EncodeToString(data)
	for id, addr := range b.record.Participants {
		if id == b.nodeID {
			continue
		}
		state, err := b.queryPeer(ctx, id, addr, kind)
		if err != nil {
			return err
		}
		if state.Incarnation != election.Incarnations[id] || state.PreparedElection != election.digest() || state.Serving {
			return errors.New("bootstrap peer did not promise this process vector")
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-b.done:
		return errors.New("license bootstrap closed")
	default:
	}
	if b.promise == nil || b.promise.digest() != election.digest() {
		return errors.New("bootstrap preparation was superseded")
	}
	b.serving, b.selected = true, b.nodeID
	return nil
}

// WaitOwner returns successfully only for the unanimously prepared serving node.
// Other nodes keep participating while parked. A new owner is possible only
// after every configured participant responds, including a restarted predecessor;
// an unreachable peer never creates an automatic failover opportunity.
func (b *LicenseBootstrap) WaitOwner(ctx context.Context) error {
	// One driver per process: handlers may promise but cannot release promises.
	b.waitMu.Lock()
	defer b.waitMu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-b.done:
			return errors.New("license bootstrap closed")
		default:
		}
		local := b.status()
		if local.Serving {
			return nil
		}
		election := &licenseBootElection{CertificateSHA256: b.digest, Incarnations: map[string]string{b.nodeID: local.Incarnation}}
		var blocked error
		serving := ""
		preferred := local.PreferredOwner
		conflictingPreferences := false
		for id, addr := range b.record.Participants {
			if id == b.nodeID {
				continue
			}
			state, err := b.queryPeer(ctx, id, addr, "license_boot_v1")
			if err != nil {
				// The controller may have crashed after persisting its certificate
				// but before releasing sealed peers. Resume idempotent releases;
				// each peer checks its own durable generation before accepting.
				// A writer still needs every participant Raft-off and unanimous.
				blocked = fmt.Errorf("waiting for bootstrap participant %s: %w", id, err)
				break
			}
			election.Incarnations[id] = state.Incarnation
			if state.PreferredOwner != "" {
				if preferred != "" && preferred != state.PreferredOwner {
					conflictingPreferences = true
				}
				preferred = state.PreferredOwner
			}
			if state.Serving {
				if serving != "" && serving != id {
					return errors.New("conflicting bootstrap serving owners")
				}
				serving = id
			}
		}
		if blocked != nil && b.nodeID == b.record.Controller {
			b.resumeSealedPeers(ctx)
		}
		if blocked == nil && serving != "" {
			b.mu.Lock()
			b.selected = serving
			b.mu.Unlock()
		} else if blocked == nil && conflictingPreferences {
			blocked = errors.New("conflicting bootstrap renewal owner preferences")
		} else if blocked == nil {
			election.PreferredOwner = preferred
			election.Owner = preferred
			if election.Owner == "" {
				election.Owner = licenseBootOwner(election.Incarnations)
			}
			b.observeRound(election)
			if election.Owner == b.nodeID {
				blocked = b.prepareOwner(ctx, election)
				if blocked == nil {
					return nil
				}
			}
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			if blocked != nil {
				return fmt.Errorf("%w: %v", ctx.Err(), blocked)
			}
			return ctx.Err()
		case <-b.done:
			timer.Stop()
			return errors.New("license bootstrap closed")
		case <-timer.C:
		}
	}
}

// Preserve the original all-participants-durable barrier after controller crash.
// Releasing even one voter while another has not applied sealed can lose quorum
// permanently. A release ACK is never used as a Raft-off election vote.
func (b *LicenseBootstrap) resumeSealedPeers(ctx context.Context) {
	if !b.crashReleaseApproved {
		for id, addr := range b.record.Participants {
			if id == b.nodeID {
				continue
			}
			if _, err := b.queryPeer(ctx, id, addr, "license_boot_v1"); err == nil {
				continue
			}
			if _, err := b.queryPeer(ctx, id, addr, lifecycleDurableKind); err != nil {
				return
			}
		}
		b.crashReleaseApproved = true
	}
	for id, addr := range b.record.Participants {
		if id != b.nodeID {
			_, _ = b.queryPeer(ctx, id, addr, "license_release_v1")
		}
	}
}

// Close releases the listener and local process lock after all handlers finish.
func (b *LicenseBootstrap) Close() error {
	b.closeOnce.Do(func() { close(b.done); b.listener.Close(); b.wg.Wait(); b.lock.Close() })
	return nil
}
