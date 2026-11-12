package mqtt

import (
	"bytes"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/metrics"
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
	return startSubscriberLogging(t, broker, timeoutSeconds, zerolog.Nop())
}

func startSubscriberLogging(t *testing.T, broker string, timeoutSeconds int, logger zerolog.Logger) (*Subscriber, error) {
	t.Helper()
	sub := &Subscription{
		ID: "s", Name: "s", Broker: broker, ClientID: "arc-test", Topics: []string{"sensors/#"},
		Database: "iot", ConnectTimeoutSeconds: timeoutSeconds, ReconnectMaxSeconds: 1, KeepAliveSeconds: 30,
	}
	s := NewSubscriber(sub, nil, nil, nil, logger)
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
// connection is refused: it never subscribes, and onConnect, which paho
// still calls for it, neither reports it established nor sets the connected
// gauge, which nothing would reset.
func TestSubscriberStartTimeoutAbortsALateConnect(t *testing.T) {
	metrics.Get().SetMQTTConnected(false)
	t.Cleanup(func() { metrics.Get().SetMQTTConnected(false) })
	logs := &syncBuffer{}
	logger := zerolog.New(logs).Level(zerolog.DebugLevel)

	var subscribed, closedByClient atomic.Bool
	acked := make(chan struct{})
	startReturned := make(chan struct{})
	addr := fakeBroker(t, func(i int, conn net.Conn) {
		if i == 0 {
			return // stall the first handshake past the timeout
		}
		// The MQTT 3.1 retry: accepted only once Start has given up, inside
		// its fresh handshake deadline.
		select {
		case <-startReturned:
		case <-time.After(5 * time.Second):
			return
		}
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
	_, err := startSubscriberLogging(t, "tcp://"+addr, 1, logger)
	close(startReturned)
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

	// Observed from inside onConnect: wait until its guard ran for the
	// refused connection, or until it set the gauge instead.
	const aborted = "MQTT connection aborted before it was established; not subscribing"
	connected := func() bool { return metrics.Get().Snapshot()["mqtt_connected"] != int64(0) }
	deadline = time.Now().Add(3 * time.Second)
	for !strings.Contains(logs.String(), aborted) && !connected() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	out := logs.String()
	if connected() {
		t.Fatalf("arc_mqtt_connected = 1 after a refused late connect, want 0; logs:\n%s", out)
	}
	if !strings.Contains(out, aborted) {
		t.Fatalf("onConnect never ran its guard for the refused connection; logs:\n%s", out)
	}
	if strings.Contains(out, "MQTT connection established") {
		t.Fatalf("onConnect reported the refused connection established; logs:\n%s", out)
	}
}

// syncBuffer is a log sink safe for paho's goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
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

// A reconnect after writes resumed that fails disconnects the client too:
// after a timeout paho is still connecting, and a late connect would
// subscribe while the status reads error.
func TestSubscriberResumeDisconnectsOnAFailedReconnect(t *testing.T) {
	for name, token := range map[string]errToken{
		"timeout": {timedOut: true},
		"error":   {err: packets.ErrorNetworkError},
	} {
		t.Run(name, func(t *testing.T) {
			client := &connectFailClient{token: token}
			var status SubscriptionStatus
			s := &Subscriber{id: "s", config: &Subscription{ID: "s", Name: "s", ConnectTimeoutSeconds: 1},
				client: client, running: true, logger: zerolog.Nop(),
				onStatusChange: func(_ string, st SubscriptionStatus, _ string) { status = st }}
			s.suspended.Store(true)
			s.resume()
			if n := client.disconnects.Load(); n != 1 {
				t.Fatalf("Disconnect called %d times, want 1", n)
			}
			if status != StatusError {
				t.Fatalf("status = %q, want error", status)
			}
		})
	}
}

// The broker URL is logged without its password or query values (#1619).
func TestStartLogsTheBrokerURLRedacted(t *testing.T) {
	logs := &syncBuffer{}
	_, err := startSubscriberLogging(t, "ws://user:hunter2-secret@"+closedPort(t)+"/mqtt?token=tok-secret&x=1#frag",
		1, zerolog.New(logs))
	if err == nil {
		t.Fatal("Start against a closed port succeeded")
	}
	out := logs.String()
	if !strings.Contains(out, "Connecting to MQTT broker") {
		t.Fatalf("no connect log line; logs:\n%s", out)
	}
	for _, secret := range []string{"hunter2-secret", "tok-secret", "frag"} {
		if strings.Contains(out, secret) {
			t.Fatalf("the log carries %q:\n%s", secret, out)
		}
	}
	if !strings.Contains(out, "user:REDACTED@") || !strings.Contains(out, "token=REDACTED") {
		t.Fatalf("the log does not name the user and query keys; logs:\n%s", out)
	}
}

func TestRedactBrokerURL(t *testing.T) {
	for in, want := range map[string]string{
		"tcp://broker:1883":                      "tcp://broker:1883",
		"tcp://u@broker:1883":                    "tcp://u@broker:1883",
		"tcp://u:p@broker:1883":                  "tcp://u:REDACTED@broker:1883",
		"tcp://u:@broker:1883":                   "tcp://u:REDACTED@broker:1883",
		"wss://h/mqtt?a=1&b=2":                   "wss://h/mqtt?a=REDACTED&b=REDACTED",
		"ssl://u:p%40ss@[::1]:8883/x?k=v#secret": "ssl://u:REDACTED@[::1]:8883/x?k=REDACTED",
		"tcp://bad host:1883":                    "(unparsable broker URL)",
		"tcp://u:1234/ss@host:1883":              "(unparsable broker URL)",
		"tcp://u:12?34@host:1883":                "(unparsable broker URL)",
		"tcp://u:12#34@host:1883":                "(unparsable broker URL)",
		"tcp:u:p@host":                           "(unparsable broker URL)",
	} {
		if got := redactBrokerURL(in); got != want {
			t.Errorf("redactBrokerURL(%q) = %q, want %q", in, got, want)
		}
	}
}
