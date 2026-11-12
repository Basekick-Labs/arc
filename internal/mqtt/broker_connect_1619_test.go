package mqtt

import (
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/rs/zerolog"
)

// closedPort returns a local TCP address nothing listens on.
func closedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// fakeBroker accepts connections and hands each, after reading its CONNECT,
// to serve with its 0-based index. It stops when the test ends.
func fakeBroker(t *testing.T, serve func(i int, conn net.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for i := 0; ; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
			go func(i int) {
				if _, err := packets.ReadPacket(conn); err != nil {
					return
				}
				serve(i, conn)
			}(i)
		}
	}()
	return ln.Addr().String()
}

func connack(conn net.Conn, code byte) {
	ack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	ack.ReturnCode = code
	_ = ack.Write(conn)
}

func startSubscriber(t *testing.T, broker string, timeoutSeconds int) (*Subscriber, error) {
	t.Helper()
	sub := &Subscription{
		ID: "s", Name: "s", Broker: broker, ClientID: "arc-test", Topics: []string{"sensors/#"},
		Database: "iot", ConnectTimeoutSeconds: timeoutSeconds, ReconnectMaxSeconds: 1, KeepAliveSeconds: 30,
	}
	s := NewSubscriber(sub, nil, nil, nil, zerolog.Nop())
	t.Cleanup(func() {
		if s.client != nil {
			s.client.Disconnect(0)
		}
	})
	return s, s.Start()
}

// #1619: a connect failure is classified so the API can answer 502 with the
// error's own text, which is also the stored error_message. A transport
// failure could not connect; a CONNACK refusal reached a broker that refused.
// The broker URL's userinfo is never in the text.
func TestSubscriberStartClassifiesConnectFailures(t *testing.T) {
	t.Run("unreachable", func(t *testing.T) {
		_, err := startSubscriber(t, "tcp://u:secret@"+closedPort(t), 2)
		if !errors.Is(err, ErrBrokerConnect) || !errors.Is(err, packets.ErrorNetworkError) {
			t.Fatalf("err = %v, want ErrBrokerConnect wrapping a network error", err)
		}
		msg, ok := BrokerConnectMessage(err)
		if !ok || msg != err.Error() || !strings.HasPrefix(msg, "could not connect to the broker: ") {
			t.Fatalf("message = %q, %v; want err's text, starting \"could not connect to the broker: \"", msg, ok)
		}
		if strings.Contains(msg, "secret") || strings.Contains(msg, "u:") {
			t.Fatalf("message %q carries the broker URL's userinfo", msg)
		}
	})

	t.Run("refused", func(t *testing.T) {
		// Every connection is refused: paho retries once with MQTT 3.1.
		addr := fakeBroker(t, func(_ int, conn net.Conn) { connack(conn, packets.ErrRefusedNotAuthorised) })
		_, err := startSubscriber(t, "tcp://"+addr, 2)
		if !errors.Is(err, ErrBrokerConnect) || !errors.Is(err, packets.ErrorRefusedNotAuthorised) {
			t.Fatalf("err = %v, want ErrBrokerConnect wrapping the refusal", err)
		}
		if want := "the broker refused the connection: not Authorized"; err.Error() != want {
			t.Fatalf("err = %q, want %q", err, want)
		}
	})

	t.Run("not a connect failure", func(t *testing.T) {
		s := NewSubscriber(&Subscription{ID: "s", Name: "s", Broker: "tcp://" + closedPort(t),
			TLSEnabled: true, TLSCAPath: "/nonexistent/ca.pem"}, nil, nil, nil, zerolog.Nop())
		err := s.Start()
		if err == nil || errors.Is(err, ErrBrokerConnect) {
			t.Fatalf("unreadable CA file: err = %v, want a failure that is not ErrBrokerConnect", err)
		}
		if _, ok := BrokerConnectMessage(err); ok {
			t.Fatalf("BrokerConnectMessage matched %v", err)
		}
	})
}

// #1619: a connect that timed out left paho connecting. Its MQTT 3.1 retry
// gets a fresh handshake deadline, so a broker that answered it subscribed and
// ingested for a subscription with no subscriber, stored as error, that stop
// answers "not running". Start now disconnects the client, and the late
// connection is aborted before onConnect can subscribe.
func TestSubscriberStartTimeoutAbortsALateConnect(t *testing.T) {
	var subscribed, closedByClient atomic.Bool
	acked := make(chan struct{})
	addr := fakeBroker(t, func(i int, conn net.Conn) {
		if i == 0 {
			return // stall the first handshake past the timeout
		}
		time.Sleep(300 * time.Millisecond) // after Start has returned
		connack(conn, packets.Accepted)
		close(acked)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		for {
			p, err := packets.ReadPacket(conn)
			if err != nil {
				var ne net.Error
				if !errors.As(err, &ne) || !ne.Timeout() {
					closedByClient.Store(true)
				}
				return
			}
			if _, ok := p.(*packets.SubscribePacket); ok {
				subscribed.Store(true)
			}
		}
	})

	start := time.Now()
	_, err := startSubscriber(t, "tcp://"+addr, 1)
	if want := "could not connect to the broker: connection timeout after 1 seconds"; err == nil || err.Error() != want || !errors.Is(err, ErrBrokerConnect) {
		t.Fatalf("Start = %v, want %q matching ErrBrokerConnect", err, want)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("Start took %v", time.Since(start))
	}

	select {
	case <-acked:
	case <-time.After(5 * time.Second):
		t.Fatal("paho never retried the connection: the test does not reach a late connect")
	}
	deadline := time.Now().Add(3 * time.Second)
	for !closedByClient.Load() && !subscribed.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if subscribed.Load() {
		t.Fatal("the late connection subscribed: a failed start left a client ingesting")
	}
	if !closedByClient.Load() {
		t.Fatal("the late connection stayed open")
	}
}

// errToken is a completed connect whose error is err, or a connect still
// pending when timedOut.
type errToken struct {
	err      error
	timedOut bool
}

func (t errToken) Wait() bool                     { return !t.timedOut }
func (t errToken) WaitTimeout(time.Duration) bool { return !t.timedOut }
func (t errToken) Done() <-chan struct{}          { return make(chan struct{}) }
func (t errToken) Error() error                   { return t.err }

type connectFailClient struct {
	pahomqtt.Client
	token       errToken
	disconnects atomic.Int64
}

func (c *connectFailClient) Connect() pahomqtt.Token { return c.token }
func (c *connectFailClient) Disconnect(uint)         { c.disconnects.Add(1) }

// Both connect-failure returns disconnect the client: after a timeout paho is
// still connecting, and a client that reported an error is released too.
func TestSubscriberStartDisconnectsOnEveryConnectFailure(t *testing.T) {
	for name, token := range map[string]errToken{
		"timeout": {timedOut: true},
		"error":   {err: packets.ErrorRefusedBadUsernameOrPassword},
	} {
		t.Run(name, func(t *testing.T) {
			client := &connectFailClient{token: token}
			s := NewSubscriber(&Subscription{ID: "s", Name: "s", Broker: "tcp://127.0.0.1:1883", ConnectTimeoutSeconds: 1},
				nil, nil, nil, zerolog.Nop())
			s.newClient = func(*pahomqtt.ClientOptions) pahomqtt.Client { return client }
			if err := s.Start(); !errors.Is(err, ErrBrokerConnect) {
				t.Fatalf("Start = %v, want ErrBrokerConnect", err)
			}
			if n := client.disconnects.Load(); n != 1 {
				t.Fatalf("Disconnect called %d times, want 1", n)
			}
		})
	}
}
