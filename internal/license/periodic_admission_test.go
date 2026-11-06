package license

import (
	"context"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func startObservedAdmissionValidation(t *testing.T, c *Client, interval time.Duration) (<-chan struct{}, <-chan struct{}, func()) {
	t.Helper()
	failed, admitted, stopped := make(chan struct{}, 8), make(chan struct{}, 8), make(chan struct{})
	c.logger = zerolog.New(io.Discard).Level(zerolog.DebugLevel).Hook(zerolog.HookFunc(func(_ *zerolog.Event, _ zerolog.Level, message string) {
		switch message {
		case "Periodic license validation failed":
			select {
			case failed <- struct{}{}:
			default:
			}
		case "License verified successfully":
			select {
			case admitted <- struct{}{}:
			default:
			}
		case "Stopping periodic license validation":
			close(stopped)
		}
	}))
	var once sync.Once
	stop := func() {
		once.Do(c.Stop)
		select {
		case <-stopped:
		case <-time.After(3 * time.Second):
			t.Error("periodic admission worker did not stop")
		}
	}
	t.Cleanup(stop)
	c.StartPeriodicValidation(interval)
	return failed, admitted, stop
}

func blockPeriodicAdmission(t *testing.T, c *Client) http.RoundTripper {
	t.Helper()
	fresh := c.httpClient.Transport
	c.httpClient.Transport = replacementTransport(func(*http.Request) (*http.Response, error) {
		return admissionResponse(http.StatusForbidden, `{"error":"maximum machine activations reached"}`), nil
	})
	if _, err := c.ActivateOrVerify(context.Background()); FailureClassOf(err) != ClassAdmissionDenied || !c.AdmissionBlocked() {
		t.Fatalf("did not establish admission fence: %v", err)
	}
	return fresh
}

func TestPeriodicAdmissionRetriesThroughTransportFailureAndRecovers(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	c := admissionClient(t, nil)
	fresh := blockPeriodicAdmission(t, c)
	var attempts atomic.Int32
	allowRecovery := make(chan struct{})
	c.httpClient.Transport = replacementTransport(func(r *http.Request) (*http.Response, error) {
		n := attempts.Add(1)
		if n == 1 {
			return admissionResponse(http.StatusForbidden, `{"error":"maximum machine activations reached"}`), nil
		}
		select {
		case <-allowRecovery:
			return fresh.RoundTrip(r)
		default:
			return admissionResponse(http.StatusServiceUnavailable, "temporary authority outage"), nil
		}
	})
	failed, admitted, stop := startObservedAdmissionValidation(t, c, 10*time.Millisecond)
	for i := 0; i < 2; i++ {
		select {
		case <-failed:
		case <-time.After(time.Second):
			t.Fatal("periodic worker stopped retrying admission")
		}
		if !c.AdmissionBlocked() || c.GetLicense() != nil || c.ServingAllowed() || c.LifecycleState().Phase != PhaseAdmissionBlocked {
			t.Fatal("quota or subsequent network failure reopened the installation")
		}
	}
	close(allowRecovery)
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("periodic worker did not recover through a fresh signed response")
	}
	if c.AdmissionBlocked() || !c.ServingAllowed() || !c.LifecycleState().Valid || attempts.Load() < 3 {
		t.Fatal("fresh signed proof did not clear the periodic admission fence")
	}
	stop() // Joins through the existing worker's stop log; no retry goroutine remains.
}

func TestPeriodicAdmissionDoesNotWaitNormalFourHourInterval(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	c := admissionClient(t, nil)
	fresh := blockPeriodicAdmission(t, c)
	var attempts atomic.Int32
	c.httpClient.Transport = replacementTransport(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return fresh.RoundTrip(r)
	})
	_, admitted, stop := startObservedAdmissionValidation(t, c, 4*time.Hour)
	// Exercise the real five-second initial admission retry, without changing
	// production constants. The former four-hour ticker cannot pass this check.
	select {
	case <-admitted:
	case <-time.After(8 * time.Second):
		t.Fatal("startup admission fence waited for the normal validation interval")
	}
	if c.AdmissionBlocked() || !c.ServingAllowed() || attempts.Load() != 1 {
		t.Fatal("initial admission retry failed or started duplicate workers")
	}
	stop() // Stop must interrupt the now-restored four-hour timer immediately.
}
