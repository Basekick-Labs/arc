package originstream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// Target names the writer a standby should receive from: the designated
// primary in this node's applied FSM, and its coordinator address. ok is
// false when this node should not receive (no primary, or it is the
// primary itself).
type Target func() (node, addr string, ok bool)

// ClientConfig configures the standby side.
type ClientConfig struct {
	Store        *Store
	LocalNodeID  string
	ClusterName  string
	SharedSecret string
	Target       Target
	Dial         func(ctx context.Context, addr string) (net.Conn, error)
	Ready        TrimReadiness
	Logger       zerolog.Logger
}

// Client is the standby side: it follows the designated primary, stores what
// it streams, and trims what is registered and held locally.
type Client struct {
	cfg ClientConfig
}

const (
	readIdle      = 3 * floorsHeartbeat
	syncEvery     = 100 * time.Millisecond
	trimEvery     = 30 * time.Second
	retargetEvery = time.Second
	backoffMin    = time.Second
	backoffMax    = 30 * time.Second
)

func NewClient(cfg ClientConfig) *Client { return &Client{cfg: cfg} }

// Run receives until ctx ends.
func (c *Client) Run(ctx context.Context) {
	trimDone := make(chan struct{})
	go func() { defer close(trimDone); c.trimLoop(ctx) }()
	defer func() { <-trimDone }()
	backoff := backoffMin
	for ctx.Err() == nil {
		node, addr, ok := c.cfg.Target()
		if !ok {
			sleepCtx(ctx, retargetEvery)
			continue
		}
		err := c.session(ctx, node, addr)
		metrics.Get().SetStandbyStreamConnected(false)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errRetarget) {
			backoff = backoffMin
			continue
		}
		c.cfg.Logger.Info().Err(err).Str("primary", node).Dur("retry_in", backoff).Msg("Origin stream from the primary writer ended; reconnecting")
		sleepCtx(ctx, backoff)
		if backoff *= 2; backoff > backoffMax {
			backoff = backoffMax
		}
	}
}

var errRetarget = errors.New("the designated primary changed")

func (c *Client) session(ctx context.Context, node, addr string) error {
	conn, err := c.cfg.Dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	nonce, err := security.GenerateNonce()
	if err != nil {
		return err
	}
	ts := time.Now().Unix()
	hello := &protocol.OriginStreamHello{
		Version: ProtocolVersion, NodeID: c.cfg.LocalNodeID, ClusterName: c.cfg.ClusterName, Nonce: nonce, Timestamp: ts,
		HMAC: security.ComputeOriginStreamHelloHMAC(c.cfg.SharedSecret, ProtocolVersion, nonce, c.cfg.LocalNodeID, c.cfg.ClusterName, ts),
	}
	if err := protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgOriginStreamHello, Payload: hello}, 10*time.Second); err != nil {
		return err
	}
	msg, err := protocol.ReceiveMessage(conn, 10*time.Second)
	if err != nil {
		return err
	}
	ack, ok := msg.Payload.(*protocol.OriginStreamHelloAck)
	if !ok || msg.Type != protocol.MsgOriginStreamHelloAck {
		return fmt.Errorf("unexpected answer %s to the origin stream hello", msg.Type)
	}
	if err := security.ValidateOriginStreamHelloAckHMAC(c.cfg.SharedSecret, nonce, ack.OriginNodeID, ack.Instance, ack.Error, ack.Timestamp, ack.HMAC, security.HMACTimestampTolerance); err != nil {
		return err
	}
	if ack.Error != "" {
		return fmt.Errorf("primary refused the origin stream: %s", ack.Error)
	}
	if ack.OriginNodeID != node {
		return fmt.Errorf("origin stream answered by %s, expected the primary %s", ack.OriginNodeID, node)
	}
	instance, err := strconv.ParseUint(ack.Instance, 16, 64)
	if err != nil || instance == 0 {
		return fmt.Errorf("origin stream answer has no valid instance")
	}
	key, err := security.DeriveOriginStreamSessionKey(c.cfg.SharedSecret, nonce)
	if err != nil {
		return err
	}
	c.cfg.Store.SetCurrent(node, instance)
	c.cfg.Logger.Info().Str("primary", node).Str("instance", ack.Instance).Msg("Receiving committed batches from the primary writer")
	metrics.Get().SetStandbyStreamConnected(true)

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	retarget := make(chan struct{}, 1)
	go func() {
		syncT := time.NewTicker(syncEvery)
		targetT := time.NewTicker(retargetEvery)
		defer syncT.Stop()
		defer targetT.Stop()
		for {
			select {
			case <-sctx.Done():
				_ = conn.Close() // unblocks the read below
				return
			case <-syncT.C:
				if err := c.cfg.Store.Sync(); err != nil {
					c.cfg.Logger.Warn().Err(err).Msg("Standby store sync failed")
				}
			case <-targetT.C:
				if n, _, ok := c.cfg.Target(); !ok || n != node {
					retarget <- struct{}{}
					_ = conn.Close()
					return
				}
			}
		}
	}()
	defer func() { _ = c.cfg.Store.Sync() }()

	var seq uint64
	for {
		_ = conn.SetReadDeadline(time.Now().Add(readIdle))
		typ, body, err := readFrame(conn, key)
		if err != nil {
			select {
			case <-retarget:
				return errRetarget
			default:
			}
			return err
		}
		switch typ {
		case frameEntry:
			fseq, e, err := parseEntry(body)
			if err != nil {
				return err
			}
			if fseq != seq+1 {
				return fmt.Errorf("origin stream frame %d after %d", fseq, seq)
			}
			seq = fseq
			if e.Instance != instance {
				return fmt.Errorf("origin stream entry for instance %016x on a session of %016x", e.Instance, instance)
			}
			if err := c.cfg.Store.Append(node, e); err != nil {
				return err
			}
		case frameFloors:
			fseq, f, err := parseFloors(body)
			if err != nil {
				return err
			}
			if fseq != seq+1 {
				return fmt.Errorf("origin stream frame %d after %d", fseq, seq)
			}
			seq = fseq
			if f.Instance != instance {
				return fmt.Errorf("origin stream floors for instance %016x on a session of %016x", f.Instance, instance)
			}
			if err := c.cfg.Store.AcceptFloors(node, f); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown origin stream frame type %d", typ)
		}
	}
}

func (c *Client) trimLoop(ctx context.Context) {
	t := time.NewTicker(trimEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if freed, err := c.cfg.Store.Trim(ctx, c.cfg.Ready); err != nil {
				c.cfg.Logger.Warn().Err(err).Msg("Standby store trim failed")
			} else if freed > 0 {
				c.cfg.Logger.Debug().Int64("bytes", freed).Msg("Standby store trimmed")
			}
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
