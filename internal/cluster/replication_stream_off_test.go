package cluster

// #888: the WAL replication stream reached only readers and compactors. Each
// applied what it received to its own ingest buffer and flushed it to its own
// Parquet, which queries read alongside the copy pulled from the primary, so
// every replicated row came back twice. StartReplication now starts no stream
// on any role; readers and compactors get data from registered Parquet only.

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/replication"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

const testReplicationSecret = "issue-888-test-shared-secret-0123456789"

// A replica that starts replication must never dial a writer. The writer here
// is a real listener, so the assertion is on the wire, not on a field: before
// the fix the target loop attached on its first pass and this accept returned
// a connection within milliseconds.
func TestStartReplicationReplicaNeverDialsAWriter(t *testing.T) {
	for _, role := range []NodeRole{RoleReader, RoleCompactor} {
		t.Run(string(role), func(t *testing.T) {
			c := targetRig(t, "")
			c.localNode.Role = role
			c.cfg.ReplicationEnabled = true
			// Without a secret the receiver refuses to dial at all, which
			// would make the wire assertion below pass for the wrong reason.
			c.cfg.SharedSecret = testReplicationSecret

			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			t.Cleanup(func() { _ = ln.Close() })

			w := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
			w.Address = ln.Addr().String()
			w.UpdateState(StateHealthy)
			if err := c.registry.Register(w); err != nil {
				t.Fatalf("register writer: %v", err)
			}
			// The writer is reachable: the selector would pick it.
			if got := c.findWriterAddr(); got != w.Address {
				t.Fatalf("setup: findWriterAddr() = %q, want %q", got, w.Address)
			}

			if err := c.StartReplication(); err != nil {
				t.Fatalf("StartReplication: %v", err)
			}

			accepted := make(chan net.Conn, 1)
			go func() {
				conn, err := ln.Accept()
				if err == nil {
					accepted <- conn
				}
			}()
			select {
			case conn := <-accepted:
				_ = conn.Close()
				t.Fatalf("%s dialled the writer after StartReplication: a replica must not consume the WAL stream, "+
					"it flushes what it receives into a second queryable copy (#888)", role)
			case <-time.After(2 * time.Second):
			}

			c.mu.RLock()
			receiver := c.replicationReceiver
			c.mu.RUnlock()
			if receiver != nil {
				t.Fatalf("%s built a replication receiver", role)
			}
		})
	}
}

// A writer starts no Sender either. Nothing may consume the stream until the
// standby-writer path exists (#1207), so a Sender would only cost the ingest
// path a hook call and a payload hash per write.
func TestStartReplicationWriterStartsNoSender(t *testing.T) {
	for _, role := range []NodeRole{RoleWriter, RoleStandalone} {
		t.Run(string(role), func(t *testing.T) {
			c := &Coordinator{
				cfg:       &config.ClusterConfig{ReplicationEnabled: true},
				localNode: NewNode("w", "w", role, "test-cluster"),
				logger:    zerolog.Nop(),
			}
			c.ctx, c.cancel = context.WithCancel(context.Background())
			t.Cleanup(c.cancel)

			if err := c.StartReplication(); err != nil {
				t.Fatalf("StartReplication: %v", err)
			}
			c.mu.RLock()
			sender := c.replicationSender
			c.mu.RUnlock()
			if sender != nil {
				sender.Stop()
				t.Fatalf("%s started a replication sender", role)
			}
		})
	}
}

// Rolling upgrades rely on this: a reader still on the old version keeps
// dialling the writer, and an upgraded writer, having no Sender, must refuse
// it rather than stream entries the reader would duplicate. In production
// handleReplicateSync refuses first on the same nil check; the live rig showed
// that refusal. This pins the refusal at the accept step behind it.
func TestUpgradedWriterRefusesALegacyReader(t *testing.T) {
	c := &Coordinator{
		// The secret and the nonce are what a Sender needs to admit a reader;
		// without them a Sender would refuse too, and this would prove nothing.
		cfg:       &config.ClusterConfig{ReplicationEnabled: true, SharedSecret: testReplicationSecret},
		localNode: NewNode("w", "w", RoleWriter, "test-cluster"),
		logger:    zerolog.Nop(),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	t.Cleanup(c.cancel)
	if err := c.StartReplication(); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}

	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	go func() {
		// Drain the rejection so the server-side write does not block on the pipe.
		buf := make([]byte, 4096)
		for {
			if _, err := client.Read(buf); err != nil {
				return
			}
		}
	}()
	err := c.AcceptReplicationConnection(server, &replication.ReplicateSync{
		ReaderID: "reader-1", HandshakeNonce: "0123456789abcdef0123456789abcdef",
	})
	if err == nil {
		t.Fatal("AcceptReplicationConnection admitted a reader; the upgraded writer must refuse the stream")
	}
	// Refused because there is no Sender, not for some other reason a Sender
	// could also give (a missing secret or nonce).
	if !strings.Contains(err.Error(), "not a writer") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}
