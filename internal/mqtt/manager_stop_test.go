package mqtt

import (
	"context"
	"errors"
	"strings"
	"testing"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
)

type stoppingClient struct {
	paho.Client
	token        *unsubscribeToken
	disconnected bool
}

func (c *stoppingClient) IsConnected() bool                { return !c.disconnected }
func (c *stoppingClient) Unsubscribe(...string) paho.Token { return c.token }
func (c *stoppingClient) Disconnect(uint)                  { c.disconnected = true }

func TestManagerShutdownPersistsStatusOnUnsubscribeFailure(t *testing.T) {
	brokerErr := errors.New("broker rejected unsubscribe")
	for _, pause := range []bool{false, true} {
		for _, timeout := range []bool{false, true} {
			name := "stop"
			if pause {
				name = "pause"
			}
			if timeout {
				name += "/timeout"
			} else {
				name += "/broker-error"
			}
			t.Run(name, func(t *testing.T) {
				mgr := newTestManager(t)
				ctx := context.Background()
				sub := &Subscription{Name: "stop-test", Broker: "tcp://localhost:1883", ClientID: "test", Topics: []string{"sensors/#"}, Database: "iot", Status: StatusRunning}
				sub.SetDefaults()
				if err := mgr.repo.Create(ctx, sub); err != nil {
					t.Fatal(err)
				}
				client := &stoppingClient{token: &unsubscribeToken{finished: !timeout, err: brokerErr, done: make(chan struct{})}}
				_, cancel := context.WithCancel(ctx)
				defer cancel()
				mgr.subscribers[sub.ID] = &Subscriber{id: sub.ID, config: sub, running: true, client: client, cancel: cancel, logger: zerolog.Nop()}
				want := StatusStopped
				var err error
				if pause {
					want = StatusPaused
					err = mgr.PauseSubscription(ctx, sub.ID)
				} else {
					err = mgr.StopSubscription(ctx, sub.ID)
				}
				if err == nil {
					t.Fatal("expected unsubscribe failure")
				}
				if timeout && !strings.Contains(err.Error(), "timed out") {
					t.Fatalf("expected timeout: %v", err)
				}
				if !timeout && !errors.Is(err, brokerErr) {
					t.Fatalf("lost broker error: %v", err)
				}
				got, getErr := mgr.repo.Get(ctx, sub.ID)
				if getErr != nil {
					t.Fatal(getErr)
				}
				if got.Status != want {
					t.Fatalf("persisted status = %v, want %v", got.Status, want)
				}
				if !client.disconnected {
					t.Fatal("client still connected")
				}
				if _, exists := mgr.subscribers[sub.ID]; exists {
					t.Fatal("subscriber still registered")
				}
			})
		}
	}
}
