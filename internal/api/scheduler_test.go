package api

// #1433: the scheduler control routes answered 503 "enterprise license
// required" whenever the scheduler was not running, including on a licensed
// node with the feature off in config, and 500 for a retention trigger the
// scheduler refused without running anything. A CQ reload the scheduler
// refused answered 200.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/license"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type fakeRetentionScheduler struct{ err error }

func (f fakeRetentionScheduler) Status() map[string]interface{} {
	return map[string]interface{}{"running": true}
}

func (f fakeRetentionScheduler) TriggerNow(context.Context) error { return f.err }

type fakeCQScheduler struct{ err error }

func (f fakeCQScheduler) Status() map[string]interface{} {
	return map[string]interface{}{"running": true}
}

func (f fakeCQScheduler) ReloadAll() error { return f.err }

func (f fakeCQScheduler) JobCount() int { return 0 }

// fakeSchedulerLicense stands in for a license client: a valid one cannot be
// built outside the license package.
type fakeSchedulerLicense struct {
	lic      *license.License
	entitled bool
}

func (f fakeSchedulerLicense) GetLicense() *license.License { return f.lic }

func (f fakeSchedulerLicense) CanUseCQScheduler() bool { return f.entitled }

func (f fakeSchedulerLicense) CanUseRetentionScheduler() bool { return f.entitled }

// Each test license has both a Status and a signed ExpiresAt, so it reads the
// same whether validity is decided by one or the other.
func validLicense() *license.License {
	return &license.License{Status: "active", ExpiresAt: time.Now().Add(24 * time.Hour)}
}

func expiredLicense() *license.License {
	return &license.License{Status: "expired", ExpiresAt: time.Now().Add(-30 * 24 * time.Hour)} // past any grace cutoff
}

func schedulerRequest(t *testing.T, h *SchedulerHandler, method, path string) (int, map[string]interface{}) {
	t.Helper()
	app := fiber.New()
	h.RegisterRoutes(app)
	resp, err := app.Test(httptest.NewRequest(method, path, nil), -1)
	require.NoError(t, err)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &body), string(raw))
	return resp.StatusCode, body
}

func TestSchedulerNotRunningReasons(t *testing.T) {
	const tier, config = "tier lacks it", "off in config"
	cases := []struct {
		name       string
		lic        *license.License
		entitled   bool
		wantStatus int
		wantReason string
	}{
		{"no license", nil, false, http.StatusForbidden, "Enterprise license required"},
		{"expired license", expiredLicense(), false, http.StatusForbidden, "Enterprise license required"},
		{"tier without the feature", validLicense(), false, http.StatusForbidden, tier},
		{"off in config", validLicense(), true, http.StatusServiceUnavailable, config},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, reason := schedulerNotRunning(tc.lic, tc.entitled, tier, config)
			require.Equal(t, tc.wantStatus, status)
			require.Equal(t, tc.wantReason, reason)
		})
	}
}

// The status route's reason for each license state, and the control routes
// answering with that reason: 403 for the license, 503 otherwise.
func TestSchedulerRoutesWhenNotRunning(t *testing.T) {
	cases := []struct {
		name            string
		lic             SchedulerLicense
		wantCQ          string
		wantRetention   string
		wantControlCode int
	}{
		{"no license", nil,
			"Enterprise license required",
			"Enterprise license required",
			http.StatusForbidden},
		{"expired license", fakeSchedulerLicense{lic: expiredLicense()},
			"Enterprise license required",
			"Enterprise license required",
			http.StatusForbidden},
		{"tier without the feature", fakeSchedulerLicense{lic: validLicense()},
			"License tier does not include CQ scheduler feature",
			"License tier does not include retention scheduler feature",
			http.StatusForbidden},
		{"off in config", fakeSchedulerLicense{lic: validLicense(), entitled: true},
			"CQ feature not enabled (continuous_query.enabled=false), or the scheduler failed to start (see the startup log)",
			"Retention feature not enabled (retention.enabled=false), or the scheduler failed to start (see the startup log)",
			http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewSchedulerHandler(nil, nil, nil, nil, zerolog.Nop())
			h.licenseClient = tc.lic

			status, body := schedulerRequest(t, h, http.MethodGet, "/api/v1/schedulers/")
			require.Equal(t, http.StatusOK, status)
			require.Equal(t, map[string]interface{}{"enabled": false, "reason": tc.wantCQ}, body["cq_scheduler"])
			require.Equal(t, map[string]interface{}{"enabled": false, "reason": tc.wantRetention}, body["retention_scheduler"])

			status, body = schedulerRequest(t, h, http.MethodPost, "/api/v1/schedulers/cq/reload")
			require.Equal(t, tc.wantControlCode, status)
			require.Equal(t, "CQ scheduler is not running: "+tc.wantCQ, body["error"])

			status, body = schedulerRequest(t, h, http.MethodPost, "/api/v1/schedulers/retention/trigger")
			require.Equal(t, tc.wantControlCode, status)
			require.Equal(t, "Retention scheduler is not running: "+tc.wantRetention, body["error"])
		})
	}
}

func TestCQReloadRefusalStatus(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{"reloaded", nil, http.StatusOK},
		{"license gone since startup", ErrCQReloadUnlicensed, http.StatusForbidden},
		{"reading the CQs failed", errors.New("database is locked"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewSchedulerHandler(fakeCQScheduler{err: tc.err}, nil, nil, nil, zerolog.Nop())
			status, body := schedulerRequest(t, h, http.MethodPost, "/api/v1/schedulers/cq/reload")
			require.Equal(t, tc.wantStatus, status, body)
			if tc.err != nil {
				require.Contains(t, body["error"], tc.err.Error())
			}
		})
	}
}

func TestRetentionTriggerRefusalStatus(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{"triggered", nil, http.StatusOK},
		{"not the primary writer", ErrRetentionTriggerRoleGated, http.StatusConflict},
		{"a cycle is running", ErrRetentionTriggerRunning, http.StatusConflict},
		{"license gone since startup", ErrRetentionTriggerUnlicensed, http.StatusForbidden},
		{"a policy failed", errors.New("policy 3: delete failed"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewSchedulerHandler(nil, fakeRetentionScheduler{err: tc.err}, nil, nil, zerolog.Nop())
			status, body := schedulerRequest(t, h, http.MethodPost, "/api/v1/schedulers/retention/trigger")
			require.Equal(t, tc.wantStatus, status, body)
			if tc.err != nil {
				require.Contains(t, body["error"], tc.err.Error())
			}
		})
	}
}
