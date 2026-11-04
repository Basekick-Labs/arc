package api

// #1433: the scheduler control routes answered 503 "enterprise license
// required" whenever the scheduler was not running, including on a licensed
// node with the feature off in config, and 500 for a retention trigger the
// scheduler refused without running anything.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

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
	valid := &license.License{Status: "active"}
	cases := []struct {
		name       string
		lic        *license.License
		entitled   bool
		wantStatus int
		wantReason string
	}{
		{"no license", nil, false, http.StatusForbidden, "Enterprise license required"},
		{"expired license", &license.License{Status: "expired"}, false, http.StatusForbidden, "Enterprise license required"},
		{"tier without the feature", valid, false, http.StatusForbidden, tier},
		{"off in config", valid, true, http.StatusServiceUnavailable, config},
		{"grace period, off in config", &license.License{Status: "grace_period"}, true, http.StatusServiceUnavailable, config},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, reason := schedulerNotRunning(tc.lic, tc.entitled, tier, config)
			require.Equal(t, tc.wantStatus, status)
			require.Equal(t, tc.wantReason, reason)
		})
	}
}

// Without a license the control routes say so with a 403, as other
// license-gated routes do, and give the reason the status route gives.
func TestSchedulerControlRoutesWithoutALicense(t *testing.T) {
	h := NewSchedulerHandler(nil, nil, nil, nil, zerolog.Nop())

	status, body := schedulerRequest(t, h, http.MethodGet, "/api/v1/schedulers/")
	require.Equal(t, http.StatusOK, status)
	cqReason := body["cq_scheduler"].(map[string]interface{})["reason"].(string)
	retentionReason := body["retention_scheduler"].(map[string]interface{})["reason"].(string)
	require.Equal(t, "Enterprise license required", cqReason)
	require.Equal(t, "Enterprise license required", retentionReason)

	status, body = schedulerRequest(t, h, http.MethodPost, "/api/v1/schedulers/cq/reload")
	require.Equal(t, http.StatusForbidden, status)
	require.Equal(t, "CQ scheduler is not running: "+cqReason, body["error"])

	status, body = schedulerRequest(t, h, http.MethodPost, "/api/v1/schedulers/retention/trigger")
	require.Equal(t, http.StatusForbidden, status)
	require.Equal(t, "Retention scheduler is not running: "+retentionReason, body["error"])
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
