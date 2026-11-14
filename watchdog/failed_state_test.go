package watchdog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TwiN/gatus/v5/alerting"
	"github.com/TwiN/gatus/v5/alerting/alert"
	"github.com/TwiN/gatus/v5/alerting/provider/custom"
	"github.com/TwiN/gatus/v5/config"
	"github.com/TwiN/gatus/v5/config/connectivity"
	"github.com/TwiN/gatus/v5/config/endpoint"
	"github.com/TwiN/gatus/v5/config/maintenance"
	"github.com/TwiN/gatus/v5/metrics"
	"github.com/TwiN/gatus/v5/storage"
	"github.com/TwiN/gatus/v5/storage/store"
	"github.com/TwiN/gatus/v5/storage/store/common"
	"github.com/TwiN/gatus/v5/storage/store/common/paging"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

// These tests drive the real failed-state loop (monitorEndpointWithFailedState -> executeEndpoint /
// executeFailedStateCheck -> store -> HandleAlerting -> custom alert provider) against a local HTTP server.
// Only the scheduler's clock is fake: it jumps straight to the next due check instead of sleeping, so that hours
// of schedule run in milliseconds. All times below are measured from the first check, which is the first failed
// check in every scenario where the endpoint starts down.

// fakeClock is a clock whose After returns immediately after moving the fake time forward.
// Once the fake time would reach `until`, it cancels the monitoring context and returns a channel that never
// fires, which ends the loop deterministically. In other words, checks run in [start, until).
type fakeClock struct {
	mu        sync.Mutex
	start     time.Time
	now       time.Time
	until     time.Time
	cancel    context.CancelFunc
	onAdvance func(elapsed time.Duration) // runs in the loop's goroutine, before the check that is now due
}

func newFakeClock(runFor time.Duration, cancel context.CancelFunc) *fakeClock {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &fakeClock{start: start, now: start, until: start.Add(runFor), cancel: cancel}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the fake time forward, e.g. to simulate a check that takes a while
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *fakeClock) Elapsed() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now.Sub(c.start)
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	next := c.now.Add(d)
	if !next.Before(c.until) {
		c.mu.Unlock()
		c.cancel()
		return nil // blocks forever; the loop returns through ctx.Done()
	}
	c.now = next
	elapsed := next.Sub(c.start)
	c.mu.Unlock()
	if c.onAdvance != nil {
		c.onAdvance(elapsed)
	}
	ch := make(chan time.Time, 1)
	ch <- next
	return ch
}

// target is a local HTTP server acting both as the monitored endpoint (/health) and as the alert webhook (/alert).
type target struct {
	server  *httptest.Server
	clk     *fakeClock
	healthy func(elapsed time.Duration) bool
	// onHealthCheck, if set, is called for the n-th request to /health (1-based), before it is answered
	onHealthCheck func(n int)

	mu     sync.Mutex
	checks []time.Duration // fake time of every request to /health
	alerts []string        // "TRIGGERED@<elapsed>" / "RESOLVED@<elapsed>" for every alert sent
}

func newTarget(t *testing.T, clk *fakeClock, healthy func(elapsed time.Duration) bool) *target {
	tg := &target{clk: clk, healthy: healthy}
	tg.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		elapsed := tg.clk.Elapsed()
		tg.mu.Lock()
		defer tg.mu.Unlock()
		switch r.URL.Path {
		case "/health":
			tg.checks = append(tg.checks, elapsed)
			if tg.onHealthCheck != nil {
				tg.onHealthCheck(len(tg.checks))
			}
			if tg.healthy(elapsed) {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
		case "/alert":
			body, _ := io.ReadAll(r.Body)
			tg.alerts = append(tg.alerts, fmt.Sprintf("%s@%s", body, elapsed))
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(tg.server.Close)
	return tg
}

func (tg *target) Checks() []time.Duration {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	return slices.Clone(tg.checks)
}

func (tg *target) Alerts() []string {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	return slices.Clone(tg.alerts)
}

// downUntil returns a health function for an endpoint that is down from the start until recoveredAt
func downUntil(recoveredAt time.Duration) func(time.Duration) bool {
	return func(elapsed time.Duration) bool { return elapsed >= recoveredAt }
}

// every returns the times from..to (inclusive) in steps of step
func every(step, from, to time.Duration) []time.Duration {
	var times []time.Duration
	for d := from; d <= to; d += step {
		times = append(times, d)
	}
	return times
}

func concat(parts ...[]time.Duration) []time.Duration {
	var all []time.Duration
	for _, part := range parts {
		all = append(all, part...)
	}
	return all
}

func at(d ...time.Duration) []time.Duration { return d }

// setUpFailedStateTest resets the package-level state used by the watchdog and the store
func setUpFailedStateTest(t *testing.T, concurrency int64) {
	_ = os.Unsetenv("MOCK_ALERT_PROVIDER") // Use the real custom alert provider
	ctx = context.Background()
	monitoringSemaphore = semaphore.NewWeighted(concurrency)
	if err := store.Initialize(nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Get().Clear() })
}

func newFailedStateEndpoint(t *testing.T, name string, tg *target, interval, failedStateInterval time.Duration, alerts ...*alert.Alert) *endpoint.Endpoint {
	ep := &endpoint.Endpoint{
		Name:        name,
		URL:         tg.server.URL + "/health",
		Interval:    interval,
		Conditions:  []endpoint.Condition{"[STATUS] == 200"},
		Alerts:      alerts,
		FailedState: &endpoint.FailedStateConfig{Interval: failedStateInterval},
	}
	if err := ep.ValidateAndSetDefaults(); err != nil {
		t.Fatal(err)
	}
	return ep
}

func newCustomAlert(failureThreshold, successThreshold int) *alert.Alert {
	enabled, sendOnResolved := true, true
	return &alert.Alert{
		Type:             alert.TypeCustom,
		Enabled:          &enabled,
		FailureThreshold: failureThreshold,
		SuccessThreshold: successThreshold,
		SendOnResolved:   &sendOnResolved,
	}
}

func newFailedStateConfig(tg *target, withAlerting bool) *config.Config {
	cfg := &config.Config{Maintenance: maintenance.GetDefaultConfig()}
	if withAlerting {
		cfg.Alerting = &alerting.Config{
			Custom: &custom.AlertProvider{
				DefaultConfig: custom.Config{URL: tg.server.URL + "/alert", Method: http.MethodPost, Body: "[ALERT_TRIGGERED_OR_RESOLVED]"},
			},
		}
	}
	return cfg
}

// storedResults returns the success of every result stored for the endpoint, oldest first
func storedResults(t *testing.T, ep *endpoint.Endpoint) []bool {
	status, err := store.Get().GetEndpointStatusByKey(ep.Key(), paging.NewEndpointStatusParams().WithResults(1, 1000))
	if errors.Is(err, common.ErrEndpointNotFound) {
		return nil // nothing was ever stored
	}
	if err != nil {
		t.Fatalf("failed to get endpoint status: %v", err)
	}
	var successes []bool
	for _, result := range status.Results {
		successes = append(successes, result.Success)
	}
	return successes
}

func runFailedStateLoop(ep *endpoint.Endpoint, cfg *config.Config, clk *fakeClock, monitoringCtx context.Context) {
	monitorEndpointWithFailedState(ep, cfg, nil, monitoringCtx, clk)
}

func formatDurations(durations []time.Duration) string {
	var parts []string
	for _, d := range durations {
		part := fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
		if seconds := int(d.Seconds()) % 60; seconds != 0 {
			part += fmt.Sprintf("%02ds", seconds)
		}
		parts = append(parts, part)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func assertChecks(t *testing.T, tg *target, expected []time.Duration) {
	t.Helper()
	if actual := tg.Checks(); !slices.Equal(actual, expected) {
		t.Errorf("unexpected checks\nexpected: %s\nactual:   %s", formatDurations(expected), formatDurations(actual))
	}
}

func assertAlerts(t *testing.T, tg *target, expected ...string) {
	t.Helper()
	if actual := tg.Alerts(); !slices.Equal(actual, expected) {
		t.Errorf("unexpected alerts\nexpected: %v\nactual:   %v", expected, actual)
	}
}

func assertStoredResults(t *testing.T, ep *endpoint.Endpoint, expected ...bool) {
	t.Helper()
	if actual := storedResults(t, ep); !slices.Equal(actual, expected) {
		t.Errorf("unexpected stored results (success of each, oldest first)\nexpected: %v\nactual:   %v", expected, actual)
	}
}

const (
	h = time.Hour
	m = time.Minute
)

func TestFailedStateScheduler_Next(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scenarios := []struct {
		name                string
		interval            time.Duration
		failedStateInterval time.Duration
		expected            []string // first 8 checks while failing the whole time
	}{
		{
			name:                "1h-5m",
			interval:            h,
			failedStateInterval: 5 * m,
			expected:            []string{"5m0s/failed-state", "10m0s/failed-state", "15m0s/failed-state", "20m0s/failed-state", "25m0s/failed-state", "30m0s/failed-state", "35m0s/failed-state", "40m0s/failed-state"},
		},
		{
			// A failed-state interval that validation accepts must actually do something
			name:                "1h-40m",
			interval:            h,
			failedStateInterval: 40 * m,
			expected:            []string{"40m0s/failed-state", "1h0m0s/regular", "1h40m0s/failed-state", "2h0m0s/regular", "2h40m0s/failed-state", "3h0m0s/regular", "3h40m0s/failed-state", "4h0m0s/regular"},
		},
		{
			// A failed-state check that would fall exactly on the next regular check is skipped
			name:                "1h-30m",
			interval:            h,
			failedStateInterval: 30 * m,
			expected:            []string{"30m0s/failed-state", "1h0m0s/regular", "1h30m0s/failed-state", "2h0m0s/regular", "2h30m0s/failed-state", "3h0m0s/regular", "3h30m0s/failed-state", "4h0m0s/regular"},
		},
		{
			name:                "1h-25m",
			interval:            h,
			failedStateInterval: 25 * m,
			expected:            []string{"25m0s/failed-state", "50m0s/failed-state", "1h0m0s/regular", "1h25m0s/failed-state", "1h50m0s/failed-state", "2h0m0s/regular", "2h25m0s/failed-state", "2h50m0s/failed-state"},
		},
		{
			name:                "1h-59m",
			interval:            h,
			failedStateInterval: 59 * m,
			expected:            []string{"59m0s/failed-state", "1h0m0s/regular", "1h59m0s/failed-state", "2h0m0s/regular", "2h59m0s/failed-state", "3h0m0s/regular", "3h59m0s/failed-state", "4h0m0s/regular"},
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			s := &failedStateScheduler{interval: scenario.interval, failedStateInterval: scenario.failedStateInterval, nextRegularCheck: start.Add(scenario.interval), lastCheck: start}
			var actual []string
			for range 8 {
				dueAt, kind := s.next(true)
				actual = append(actual, fmt.Sprintf("%s/%s", dueAt.Sub(start), kind))
				s.lastCheck = dueAt
				if kind == regularCheck {
					s.regularCheckDone(dueAt)
				}
			}
			if !slices.Equal(actual, scenario.expected) {
				t.Errorf("expected %v, got %v", scenario.expected, actual)
			}
		})
	}
	t.Run("not-failing", func(t *testing.T) {
		s := &failedStateScheduler{interval: h, failedStateInterval: 5 * m, nextRegularCheck: start.Add(h), lastCheck: start}
		if dueAt, kind := s.next(false); kind != regularCheck || dueAt != start.Add(h) {
			t.Errorf("expected regular check at 1h, got %s check at %s", kind, dueAt.Sub(start))
		}
	})
	t.Run("missed-regular-checks-are-dropped", func(t *testing.T) {
		s := &failedStateScheduler{interval: h, failedStateInterval: 5 * m, nextRegularCheck: start.Add(h)}
		// The regular check due at 1h only finished at 3h30m
		s.regularCheckDone(start.Add(3*h + 30*m))
		if s.nextRegularCheck != start.Add(4*h) {
			t.Errorf("expected next regular check at 4h, got %s", s.nextRegularCheck.Sub(start))
		}
	})
}

// An hourly endpoint with failed-state.interval 5m and the default thresholds (failure-threshold 3,
// success-threshold 2) that is down from 0h00m until 3h12m. This is the example in the README.
func TestMonitorEndpointWithFailedState_RecoveryIsNoticedSoonerWithoutExtraAlertsOrResults(t *testing.T) {
	setUpFailedStateTest(t, 10)
	monitoringCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := newFakeClock(4*h+30*m, cancel)
	tg := newTarget(t, clk, downUntil(3*h+12*m))
	ep := newFailedStateEndpoint(t, "recovery", tg, h, 5*m, newCustomAlert(3, 2))
	cfg := newFailedStateConfig(tg, true)

	runFailedStateLoop(ep, cfg, clk, monitoringCtx)

	assertChecks(t, tg, concat(
		every(5*m, 0, 3*h+20*m), // regular checks on the hour, failed-state checks every 5m in between
		at(4*h),                 // back to the regular schedule after the alert was resolved at 3h20m
	))
	// The alert is triggered by the 3rd failed regular check (2h00m), not by the failed-state checks in between, and
	// resolved by the 2nd successful failed-state check (3h20m). Without failed-state, the recovery would only have
	// been seen at 4h00m and the alert resolved at 5h00m.
	assertAlerts(t, tg, "TRIGGERED@2h0m0s", "RESOLVED@3h20m0s")
	// Only the regular checks' failures (0h, 1h, 2h, 3h) are stored, not the 35 failed failed-state checks.
	// Then the two successful failed-state checks (3h15m, 3h20m) and the regular check at 4h00m.
	assertStoredResults(t, ep, false, false, false, false, true, true, true)
	// So the uptime is 3 successes out of 7 stored results, not 3 out of all 42 checks
	uptime, err := store.Get().GetUptimeByKey(ep.Key(), time.Now().Add(-24*h), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if expected := 3.0 / 7.0; uptime != expected {
		t.Errorf("expected uptime %f, got %f", expected, uptime)
	}
	if ep.NumberOfFailuresInARow != 0 || ep.NumberOfSuccessesInARow != 3 {
		t.Errorf("expected 0 failures and 3 successes in a row, got %d and %d", ep.NumberOfFailuresInARow, ep.NumberOfSuccessesInARow)
	}
}

// A failed failed-state check must never send a reminder, even when one would be due.
func TestMonitorEndpointWithFailedState_FailedStateChecksNeverSendReminders(t *testing.T) {
	setUpFailedStateTest(t, 10)
	monitoringCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := newFakeClock(2*h+30*m, cancel)
	tg := newTarget(t, clk, downUntil(100*h))
	endpointAlert := newCustomAlert(1, 2)
	endpointAlert.MinimumReminderInterval = 5 * m
	ep := newFailedStateEndpoint(t, "reminders", tg, h, 5*m, endpointAlert)
	cfg := newFailedStateConfig(tg, true)
	// Reminders use the real clock, so pretend that the last reminder was sent a day ago before every check: a reminder
	// is then due on every failure that goes through the regular alerting path.
	clk.onAdvance = func(time.Duration) { ep.LastReminderSent = time.Now().Add(-24 * h) }

	runFailedStateLoop(ep, cfg, clk, monitoringCtx)

	assertChecks(t, tg, every(5*m, 0, 2*h+25*m))
	// The initial alert at 0h00m and one reminder per failed regular check (1h00m, 2h00m), none from the 27 failed
	// failed-state checks in between
	assertAlerts(t, tg, "TRIGGERED@0s", "TRIGGERED@1h0m0s", "TRIGGERED@2h0m0s")
	assertStoredResults(t, ep, false, false, false)
	if ep.NumberOfFailuresInARow != 3 {
		t.Errorf("expected failed-state checks not to count toward NumberOfFailuresInARow, got %d", ep.NumberOfFailuresInARow)
	}
}

// A failed failed-state check that flips the stored status from up to down is stored, but doesn't count toward
// failure-threshold and doesn't send anything. It resets the success streak.
func TestMonitorEndpointWithFailedState_FlapDuringRecovery(t *testing.T) {
	setUpFailedStateTest(t, 10)
	monitoringCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := newFakeClock(3*h+30*m, cancel)
	// Down until 2h12m, up from 2h12m to 2h18m (the 2h15m check succeeds), down again until 2h22m
	tg := newTarget(t, clk, func(elapsed time.Duration) bool {
		return (elapsed >= 2*h+12*m && elapsed < 2*h+18*m) || elapsed >= 2*h+22*m
	})
	ep := newFailedStateEndpoint(t, "flap", tg, h, 5*m, newCustomAlert(3, 2))
	cfg := newFailedStateConfig(tg, true)

	runFailedStateLoop(ep, cfg, clk, monitoringCtx)

	assertChecks(t, tg, concat(every(5*m, 0, 2*h+30*m), at(3*h)))
	// Without the reset of the success streak by the flap at 2h20m, the alert would have been resolved at 2h25m
	assertAlerts(t, tg, "TRIGGERED@2h0m0s", "RESOLVED@2h30m0s")
	// 0h, 1h, 2h: regular failures. 2h05m and 2h10m: not stored. 2h15m: success. 2h20m: stored, since it flips the
	// status from up to down. 2h25m, 2h30m: successes. 3h: regular success.
	assertStoredResults(t, ep, false, false, false, true, false, true, true, true)
}

// A successful failed-state check resets the failure streak, so an alert can come later (or not at all), never sooner.
func TestMonitorEndpointWithFailedState_FailedStateSuccessResetsFailureStreak(t *testing.T) {
	setUpFailedStateTest(t, 10)
	monitoringCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := newFakeClock(4*h+30*m, cancel)
	// Down, except from 1h12m to 1h18m (the 1h15m check succeeds)
	tg := newTarget(t, clk, func(elapsed time.Duration) bool { return elapsed >= 1*h+12*m && elapsed < 1*h+18*m })
	ep := newFailedStateEndpoint(t, "streak", tg, h, 5*m, newCustomAlert(3, 2))
	cfg := newFailedStateConfig(tg, true)

	runFailedStateLoop(ep, cfg, clk, monitoringCtx)

	assertChecks(t, tg, concat(
		every(5*m, 0, 1*h+15*m), // failing until the success at 1h15m
		every(5*m, 2*h, 4*h+25*m),
	))
	// Without failed-state, the failed regular checks at 0h, 1h and 2h would have triggered the alert at 2h00m.
	// The success at 1h15m reset the streak, so it takes the failed regular checks at 2h, 3h and 4h.
	assertAlerts(t, tg, "TRIGGERED@4h0m0s")
	assertStoredResults(t, ep, false, false, true, false, false, false)
}

// While the last stored result is a failure, failed-state checks keep running during a maintenance window, so that
// a recovery shows up. Once a success is stored, they stop until the window is over, and resume afterwards while
// an alert is still open.
func TestMonitorEndpointWithFailedState_MaintenanceWindow(t *testing.T) {
	setUpFailedStateTest(t, 10)
	monitoringCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := newFakeClock(4*h+30*m, cancel)
	tg := newTarget(t, clk, downUntil(3*h+12*m))
	ep := newFailedStateEndpoint(t, "maintenance", tg, h, 5*m, newCustomAlert(3, 2))
	cfg := newFailedStateConfig(tg, true)
	// IsUnderMaintenance uses the real clock, so use a window that covers the real current time and switch it on and
	// off from the fake clock: under maintenance from 2h30m (inclusive) to 3h40m (exclusive).
	cfg.Maintenance = newMaintenanceWindowCoveringNow(t)
	clk.onAdvance = func(elapsed time.Duration) {
		underMaintenance := elapsed >= 2*h+30*m && elapsed < 3*h+40*m
		cfg.Maintenance.Enabled = &underMaintenance
	}
	clk.onAdvance(0) // not under maintenance for the initial check

	runFailedStateLoop(ep, cfg, clk, monitoringCtx)

	assertChecks(t, tg, concat(
		every(5*m, 0, 3*h+15*m), // keeps checking during the window while the stored result is a failure
		at(3*h+40*m, 3*h+45*m),  // nothing from 3h20m to 3h35m; resumes once the window is over at 3h40m
		at(4*h),
	))
	// The alert is triggered at 2h00m, before the window. The success at 3h15m happens during the window, so it is
	// stored but doesn't count toward success-threshold. 3h40m and 3h45m do, and resolve the alert.
	assertAlerts(t, tg, "TRIGGERED@2h0m0s", "RESOLVED@3h45m0s")
	assertStoredResults(t, ep, false, false, false, false, true, true, true, true)
}

func TestMonitorEndpointWithFailedState_MaintenanceWindowWithoutOpenAlert(t *testing.T) {
	setUpFailedStateTest(t, 10)
	monitoringCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := newFakeClock(2*h+30*m, cancel)
	tg := newTarget(t, clk, downUntil(1*h+12*m))
	ep := newFailedStateEndpoint(t, "maintenance-no-alert", tg, h, 5*m, newCustomAlert(3, 2))
	cfg := newFailedStateConfig(tg, true)
	cfg.Maintenance = newMaintenanceWindowCoveringNow(t) // under maintenance the whole time

	runFailedStateLoop(ep, cfg, clk, monitoringCtx)

	// The recovery at 1h15m shows up, then failed-state checks stop
	assertChecks(t, tg, concat(every(5*m, 0, 1*h+15*m), at(2*h)))
	assertAlerts(t, tg)
	assertStoredResults(t, ep, false, false, true, true)
}

func newMaintenanceWindowCoveringNow(t *testing.T) *maintenance.Config {
	enabled := true
	window := &maintenance.Config{
		Enabled:  &enabled,
		Start:    time.Now().UTC().Add(-1 * h).Format("15:04"),
		Duration: 3 * h,
	}
	if err := window.ValidateAndSetDefaults(); err != nil {
		t.Fatal(err)
	}
	if !window.IsUnderMaintenance() {
		t.Fatal("expected maintenance window to cover the current time")
	}
	return window
}

// An alert that is still triggered when Gatus starts (e.g. loaded from the database after a restart) keeps
// failed-state checks running until it is resolved, even though the first check succeeds.
func TestMonitorEndpointWithFailedState_PersistedTriggeredAlert(t *testing.T) {
	setUpFailedStateTest(t, 10)
	monitoringCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := newFakeClock(1*h+30*m, cancel)
	tg := newTarget(t, clk, downUntil(0))
	endpointAlert := newCustomAlert(3, 2)
	endpointAlert.Triggered = true
	ep := newFailedStateEndpoint(t, "persisted", tg, h, 5*m, endpointAlert)
	ep.NumberOfFailuresInARow = endpointAlert.FailureThreshold // what main.initializeStorage does
	cfg := newFailedStateConfig(tg, true)

	runFailedStateLoop(ep, cfg, clk, monitoringCtx)

	assertChecks(t, tg, at(0, 5*m, 1*h))
	assertAlerts(t, tg, "RESOLVED@5m0s")
	assertStoredResults(t, ep, true, true, true)
}

// Without alerting configured, failed-state checks still make the recovery show up sooner
func TestMonitorEndpointWithFailedState_WithoutAlerting(t *testing.T) {
	setUpFailedStateTest(t, 10)
	monitoringCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := newFakeClock(2*h+30*m, cancel)
	tg := newTarget(t, clk, downUntil(42*m))
	ep := newFailedStateEndpoint(t, "no-alerting", tg, h, 5*m, newCustomAlert(3, 2))
	cfg := newFailedStateConfig(tg, false)

	runFailedStateLoop(ep, cfg, clk, monitoringCtx)

	assertChecks(t, tg, concat(every(5*m, 0, 45*m), at(1*h, 2*h)))
	assertStoredResults(t, ep, false, true, true, true)
}

// When the endpoint never fails, it is only checked on its regular schedule
func TestMonitorEndpointWithFailedState_HealthyEndpoint(t *testing.T) {
	setUpFailedStateTest(t, 10)
	monitoringCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := newFakeClock(3*h+30*m, cancel)
	tg := newTarget(t, clk, downUntil(0))
	ep := newFailedStateEndpoint(t, "healthy", tg, h, 5*m, newCustomAlert(3, 2))
	cfg := newFailedStateConfig(tg, true)

	runFailedStateLoop(ep, cfg, clk, monitoringCtx)

	assertChecks(t, tg, at(0, 1*h, 2*h, 3*h))
	assertAlerts(t, tg)
	assertStoredResults(t, ep, true, true, true, true)
}

// An endpoint without failed-state is monitored by the plain ticker loop of monitorEndpoint, as before failed-state
// existed: only regular checks, one per interval, each of them stored, even while the endpoint is failing. The ticker
// loop has no clock abstraction, so this test uses the real clock, with a 200ms interval and about 1s of run time.
func TestMonitorEndpoint_WithoutFailedStateGetsNoExtraChecks(t *testing.T) {
	setUpFailedStateTest(t, 10)
	var mu sync.Mutex
	var checks []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		checks = append(checks, time.Now())
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError) // always failing
	}))
	defer server.Close()
	numberOfChecks := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(checks)
	}
	interval := 200 * time.Millisecond
	ep := &endpoint.Endpoint{Name: "without-failed-state", URL: server.URL, Interval: interval, Conditions: []endpoint.Condition{"[STATUS] == 200"}}
	if err := ep.ValidateAndSetDefaults(); err != nil {
		t.Fatal(err)
	}
	if ep.FailedState != nil {
		t.Fatal("expected no failed-state configuration")
	}
	cfg := &config.Config{Maintenance: maintenance.GetDefaultConfig()}
	monitoringCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		monitorEndpoint(ep, cfg, nil, monitoringCtx)
		close(done)
	}()
	for deadline := time.Now().Add(10 * time.Second); numberOfChecks() < 5 && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(checks) < 5 {
		t.Fatalf("expected at least 5 checks, got %d", len(checks))
	}
	// A failed-state check would show up as a gap shorter than the interval between two checks
	for i := 1; i < len(checks); i++ {
		if gap := checks[i].Sub(checks[i-1]); gap < interval*3/4 {
			t.Errorf("check %d came %s after the previous one, expected one interval (%s): an extra check ran", i+1, gap, interval)
		}
	}
	// Every check is a regular check, so every check is stored, including the failures
	results := storedResults(t, ep)
	if len(results) != len(checks) || slices.Contains(results, true) {
		t.Errorf("expected %d stored failures, one per check, got %v", len(checks), results)
	}
}

// The first check can take a while (slow target, alerts being sent). Regular checks are scheduled every interval from
// the end of the first check, so failed-state checks must be counted from there too. Otherwise, with an interval that
// is a multiple of failed-state.interval, the last failed-state check of the first cycle lands right before the first
// regular check instead of being skipped, and the target is checked twice in a row.
func TestMonitorEndpointWithFailedState_SlowFirstCheck(t *testing.T) {
	setUpFailedStateTest(t, 10)
	monitoringCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := newFakeClock(2*h+30*m, cancel)
	tg := newTarget(t, clk, downUntil(100*h))
	// The first check starts at 0s and takes 30s
	tg.onHealthCheck = func(n int) {
		if n == 1 {
			clk.Advance(30 * time.Second)
		}
	}
	ep := newFailedStateEndpoint(t, "slow-first-check", tg, h, 5*m, newCustomAlert(3, 2))
	cfg := newFailedStateConfig(tg, true)

	runFailedStateLoop(ep, cfg, clk, monitoringCtx)

	// Regular checks at 1h00m30s and 2h00m30s (every hour from the end of the first check), failed-state checks every
	// 5m from the end of the first check in between: 0h05m30s, ..., 0h55m30s, then the regular check at 1h00m30s, and
	// so on. In particular, no failed-state check at 1h00m, 30s before the regular check.
	assertChecks(t, tg, concat(at(0), every(5*m, 5*m+30*time.Second, 2*h+25*m+30*time.Second)))
	assertAlerts(t, tg, "TRIGGERED@2h0m30s")
}

// After a restart, Gatus loads each triggered alert and its success streak from the database (main.initializeStorage).
// A failed failed-state check resets the success streak, and that reset must be saved too: otherwise, after a restart,
// the successes from before the failure would still count, and the alert could be resolved without success-threshold
// successes in a row. Uses a real sqlite store, since the in-memory store doesn't persist anything.
func TestMonitorEndpointWithFailedState_SuccessStreakResetSurvivesRestart(t *testing.T) {
	setUpFailedStateTest(t, 10)
	if err := store.Initialize(&storage.Config{
		Type:                   storage.TypeSQLite,
		Path:                   filepath.Join(t.TempDir(), "gatus.db"),
		MaximumNumberOfResults: storage.DefaultMaximumNumberOfResults,
		MaximumNumberOfEvents:  storage.DefaultMaximumNumberOfEvents,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.Get().Close()
		_ = store.Initialize(nil)
	})
	savedStreak := func(ep *endpoint.Endpoint, endpointAlert *alert.Alert) int {
		exists, _, numberOfSuccessesInARow, err := store.Get().GetTriggeredEndpointAlert(ep, endpointAlert)
		if err != nil || !exists {
			t.Fatalf("expected a saved triggered alert, got exists=%v err=%v", exists, err)
		}
		return numberOfSuccessesInARow
	}

	// Before the restart: the alert triggers at 0s, the failed-state check at 5m succeeds (streak 1, saved), and the
	// one at 10m fails (a flap: the streak is reset to 0, which must be saved)
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	clk1 := newFakeClock(12*m, cancel1)
	tg1 := newTarget(t, clk1, func(elapsed time.Duration) bool { return elapsed >= 4*m && elapsed < 6*m })
	alert1 := newCustomAlert(1, 2)
	ep1 := newFailedStateEndpoint(t, "restart-streak", tg1, h, 5*m, alert1)
	streakSavedBeforeTheFlap := -1
	clk1.onAdvance = func(elapsed time.Duration) {
		if elapsed == 10*m {
			streakSavedBeforeTheFlap = savedStreak(ep1, alert1)
		}
	}
	runFailedStateLoop(ep1, newFailedStateConfig(tg1, true), clk1, ctx1)
	assertChecks(t, tg1, at(0, 5*m, 10*m))
	assertAlerts(t, tg1, "TRIGGERED@0s")
	if streakSavedBeforeTheFlap != 1 {
		t.Fatalf("expected the success at 5m to be saved as a streak of 1, got %d", streakSavedBeforeTheFlap)
	}
	if streak := savedStreak(ep1, alert1); streak != 0 {
		t.Fatalf("expected the failure at 10m to save the reset of the streak (0), got %d", streak)
	}

	// Restart: a new endpoint with the same configuration, loaded the way main.initializeStorage does it
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	clk2 := newFakeClock(12*m, cancel2)
	tg2 := newTarget(t, clk2, downUntil(0)) // healthy from now on
	alert2 := newCustomAlert(1, 2)
	ep2 := newFailedStateEndpoint(t, "restart-streak", tg2, h, 5*m, alert2)
	exists, resolveKey, numberOfSuccessesInARow, err := store.Get().GetTriggeredEndpointAlert(ep2, alert2)
	if err != nil || !exists {
		t.Fatalf("expected the triggered alert to be loaded after the restart, got exists=%v err=%v", exists, err)
	}
	alert2.Triggered, alert2.ResolveKey = true, resolveKey
	ep2.NumberOfSuccessesInARow, ep2.NumberOfFailuresInARow = numberOfSuccessesInARow, alert2.FailureThreshold
	runFailedStateLoop(ep2, newFailedStateConfig(tg2, true), clk2, ctx2)
	// The first check after the restart is the 1st success in a row, the failed-state check at 5m the 2nd. With a
	// stale streak of 1, the alert would already have been resolved by the first check (RESOLVED@0s).
	assertChecks(t, tg2, at(0, 5*m))
	assertAlerts(t, tg2, "RESOLVED@5m0s")
}

// If the first check after startup is skipped (here: no connectivity yet), nothing is known to be failing, so no
// failed-state checks run: the next check is the next regular check, as for an endpoint without failed-state.
func TestMonitorEndpointWithFailedState_FirstCheckSkippedAtStartup(t *testing.T) {
	setUpFailedStateTest(t, 10)
	monitoringCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := newFakeClock(2*h+30*m, cancel)
	tg := newTarget(t, clk, downUntil(0)) // the endpoint itself is healthy
	ep := newFailedStateEndpoint(t, "first-check-skipped", tg, h, 5*m, newCustomAlert(1, 2))
	cfg := newFailedStateConfig(tg, true)
	// The connectivity checker's target doesn't listen at startup, so the first check is skipped. It starts listening
	// as soon as the fake clock reaches 2m, i.e. before the next check of any kind: a failed-state check at 5m would
	// run, and so would the regular check at 1h. Interval 1ns makes the checker look again on every check.
	address := closedLocalAddress(t)
	cfg.Connectivity = &connectivity.Config{Checker: &connectivity.Checker{Target: address, Interval: time.Nanosecond}}
	var listener net.Listener
	clk.onAdvance = func(elapsed time.Duration) {
		if listener == nil && elapsed >= 2*m {
			var err error
			if listener, err = net.Listen("tcp", address); err != nil {
				t.Errorf("failed to listen on %s: %v", address, err)
				return
			}
			go func(l net.Listener) {
				for {
					connection, err := l.Accept()
					if err != nil {
						return
					}
					_ = connection.Close()
				}
			}(listener)
		}
	}

	runFailedStateLoop(ep, cfg, clk, monitoringCtx)
	if listener != nil {
		_ = listener.Close()
	}

	// No request at 0s (skipped), none at 5m, 10m, ... (no failed-state checks): the regular checks at 1h and 2h only
	assertChecks(t, tg, at(1*h, 2*h))
	assertAlerts(t, tg)
	assertStoredResults(t, ep, true, true)
}

// Metrics are published for exactly the results that are stored: every regular check, every successful failed-state
// check, and the failed failed-state check that flips the stored status from up to down; not for the other failed
// failed-state checks.
func TestMonitorEndpointWithFailedState_MetricsOnlyForStoredResults(t *testing.T) {
	setUpFailedStateTest(t, 10)
	monitoringCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := newFakeClock(2*h+30*m, cancel)
	// Down, except from 1h12m to 1h18m (the 1h15m check succeeds) and from 1h22m on
	tg := newTarget(t, clk, func(elapsed time.Duration) bool {
		return (elapsed >= 1*h+12*m && elapsed < 1*h+18*m) || elapsed >= 1*h+22*m
	})
	ep := newFailedStateEndpoint(t, "metrics", tg, h, 5*m, newCustomAlert(1, 2))
	cfg := newFailedStateConfig(tg, true)
	cfg.Metrics = true
	registry := prometheus.NewRegistry()
	metrics.InitializePrometheusMetrics(cfg, registry)
	t.Cleanup(metrics.UnregisterPrometheusMetrics)

	runFailedStateLoop(ep, cfg, clk, monitoringCtx)

	assertChecks(t, tg, concat(every(5*m, 0, 1*h+30*m), at(2*h)))
	assertAlerts(t, tg, "TRIGGERED@0s", "RESOLVED@1h30m0s")
	// Stored: 0h and 1h (regular failures), 1h15m (success), 1h20m (failure that flips the status from up to down),
	// 1h25m and 1h30m (successes, the 2nd resolves the alert), 2h (regular success). The other 13 failed failed-state
	// checks (0h05m-0h55m, 1h05m, 1h10m) are not stored.
	assertStoredResults(t, ep, false, false, true, false, true, true, true)
	published := map[string]float64{}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "gatus_results_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["key"] == ep.Key() {
				published[labels["success"]] += metric.GetCounter().GetValue()
			}
		}
	}
	if published["false"] != 3 || published["true"] != 4 {
		t.Errorf("expected gatus_results_total to count the 7 stored results (3 failures, 4 successes) and none of the "+
			"13 other failed failed-state checks, got failures=%v successes=%v", published["false"], published["true"])
	}
}

// Failed-state checks go through the same concurrency limit as regular checks
func TestExecuteFailedStateCheck_RespectsConcurrencyLimit(t *testing.T) {
	setUpFailedStateTest(t, 1)
	clk := newFakeClock(h, func() {})
	tg := newTarget(t, clk, downUntil(0))
	ep := newFailedStateEndpoint(t, "concurrency", tg, h, 5*m, newCustomAlert(3, 2))
	cfg := newFailedStateConfig(tg, true)
	// Another check is holding the only slot
	if err := monitoringSemaphore.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	done := make(chan *endpoint.Result)
	go func() {
		result, _ := executeFailedStateCheck(ep, cfg, nil, false)
		done <- result
	}()
	select {
	case <-done:
		t.Fatal("failed-state check ran while the concurrency limit was reached")
	case <-time.After(300 * time.Millisecond):
	}
	if checks := tg.Checks(); len(checks) != 0 {
		t.Fatalf("expected no request to the endpoint while the concurrency limit was reached, got %d", len(checks))
	}
	monitoringSemaphore.Release(1)
	select {
	case result := <-done:
		if result == nil || !result.Success {
			t.Fatalf("expected a successful result once the slot was released, got %+v", result)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("failed-state check didn't run after the slot was released")
	}
	if checks := tg.Checks(); len(checks) != 1 {
		t.Fatalf("expected exactly one request to the endpoint, got %d", len(checks))
	}
	// The slot must have been released again
	if !monitoringSemaphore.TryAcquire(1) {
		t.Fatal("expected the failed-state check to release its slot")
	}
	monitoringSemaphore.Release(1)
}

func TestExecuteFailedStateCheck_SkippedWhenShuttingDownWhileWaitingForConcurrencyLimit(t *testing.T) {
	setUpFailedStateTest(t, 1)
	clk := newFakeClock(h, func() {})
	tg := newTarget(t, clk, downUntil(0))
	ep := newFailedStateEndpoint(t, "concurrency-shutdown", tg, h, 5*m, newCustomAlert(3, 2))
	cfg := newFailedStateConfig(tg, true)
	if err := monitoringSemaphore.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	defer monitoringSemaphore.Release(1)
	var cancelWatchdog context.CancelFunc
	ctx, cancelWatchdog = context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if result, stored := executeFailedStateCheck(ep, cfg, nil, false); result != nil || stored {
			t.Errorf("expected the check to be skipped, got result=%+v stored=%v", result, stored)
		}
	}()
	time.Sleep(100 * time.Millisecond)
	cancelWatchdog()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("failed-state check didn't give up after the watchdog was shut down")
	}
	if checks := tg.Checks(); len(checks) != 0 {
		t.Errorf("expected no request to the endpoint, got %d", len(checks))
	}
}

// Failed-state checks go through the same connectivity check as regular checks
func TestExecuteFailedStateCheck_SkippedWithoutConnectivity(t *testing.T) {
	setUpFailedStateTest(t, 10)
	clk := newFakeClock(h, func() {})
	tg := newTarget(t, clk, downUntil(0))
	ep := newFailedStateEndpoint(t, "connectivity", tg, h, 5*m, newCustomAlert(3, 2))
	cfg := newFailedStateConfig(tg, true)
	cfg.Connectivity = &connectivity.Config{Checker: &connectivity.Checker{Target: closedLocalAddress(t), Interval: h}}

	if result, stored := executeFailedStateCheck(ep, cfg, nil, false); result != nil || stored {
		t.Errorf("expected the check to be skipped, got result=%+v stored=%v", result, stored)
	}
	if checks := tg.Checks(); len(checks) != 0 {
		t.Errorf("expected no request to the endpoint without connectivity, got %d", len(checks))
	}
	assertStoredResults(t, ep)
}

// closedLocalAddress returns a local TCP address nothing is listening on
func closedLocalAddress(t *testing.T) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func TestHandleFailedStateCheckFailure(t *testing.T) {
	setUpFailedStateTest(t, 10)
	endpointAlert := newCustomAlert(3, 2)
	endpointAlert.Triggered = true
	ep := &endpoint.Endpoint{Name: "handle-failure", Alerts: []*alert.Alert{endpointAlert}, NumberOfFailuresInARow: 5, NumberOfSuccessesInARow: 1}
	lastReminderSent := time.Now().Add(-time.Hour)
	ep.LastReminderSent = lastReminderSent

	handleFailedStateCheckFailure(ep, &alerting.Config{})

	if ep.NumberOfSuccessesInARow != 0 {
		t.Errorf("expected the success streak to be reset, got %d", ep.NumberOfSuccessesInARow)
	}
	if ep.NumberOfFailuresInARow != 5 {
		t.Errorf("expected the failure streak to be left alone, got %d", ep.NumberOfFailuresInARow)
	}
	if !endpointAlert.Triggered || ep.LastReminderSent != lastReminderSent {
		t.Error("expected the alert to be left alone")
	}

	ep.NumberOfSuccessesInARow = 1
	handleFailedStateCheckFailure(ep, nil)
	if ep.NumberOfSuccessesInARow != 1 {
		t.Errorf("expected nothing to change without alerting, like HandleAlerting, got %d", ep.NumberOfSuccessesInARow)
	}
}
