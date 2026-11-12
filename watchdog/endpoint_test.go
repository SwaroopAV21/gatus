package watchdog

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/TwiN/gatus/v5/alerting"
	"github.com/TwiN/gatus/v5/alerting/alert"
	"github.com/TwiN/gatus/v5/alerting/provider/custom"
	"github.com/TwiN/gatus/v5/client"
	"github.com/TwiN/gatus/v5/config"
	"github.com/TwiN/gatus/v5/config/endpoint"
	"github.com/TwiN/gatus/v5/config/maintenance"
	"github.com/TwiN/gatus/v5/storage"
	"github.com/TwiN/gatus/v5/storage/store"
	"github.com/TwiN/gatus/v5/storage/store/common/paging"
	"golang.org/x/sync/semaphore"
)

const (
	fakeTargetHost = "target.local"
	fakeAlertHost  = "alert.local"
)

// fakeTransport answers both the requests made to the monitored endpoint (fakeTargetHost) and the requests made by
// the custom alerting provider (fakeAlertHost), so that these tests neither need network access nor real sleeps.
type fakeTransport struct {
	mu             sync.Mutex
	healthy        bool
	targetRequests int
	alertRequests  []string // "TRIGGERED" or "RESOLVED", in the order they were sent
}

func (f *fakeTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	statusCode := http.StatusOK
	switch request.URL.Host {
	case fakeTargetHost:
		f.targetRequests++
		if !f.healthy {
			statusCode = http.StatusInternalServerError
		}
	case fakeAlertHost:
		f.alertRequests = append(f.alertRequests, strings.TrimPrefix(request.URL.Path, "/"))
	default:
		statusCode = http.StatusNotFound
	}
	return &http.Response{StatusCode: statusCode, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: request}, nil
}

func (f *fakeTransport) setHealthy(healthy bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.healthy = healthy
}

func (f *fakeTransport) checks() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.targetRequests
}

func (f *fakeTransport) alerts() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.alertRequests...)
}

// setupMonitoringTest must be called outside the synctest bubble. It injects the fake transport and initializes a
// fresh memory store.
func setupMonitoringTest(t *testing.T, healthy bool) *fakeTransport {
	transport := &fakeTransport{healthy: healthy}
	client.InjectHTTPClient(&http.Client{Transport: transport})
	t.Cleanup(func() { client.InjectHTTPClient(nil) })
	if err := store.Initialize(nil); err != nil {
		t.Fatal("failed to initialize store:", err)
	}
	return transport
}

func newTestEndpoint(name string, failedState *endpoint.FailedState, alerts ...*alert.Alert) *endpoint.Endpoint {
	ep := &endpoint.Endpoint{
		Name:        name,
		URL:         "http://" + fakeTargetHost + "/health",
		Interval:    time.Hour,
		FailedState: failedState,
		Conditions:  []endpoint.Condition{"[STATUS] == 200"},
		Alerts:      alerts,
	}
	if err := ep.ValidateAndSetDefaults(); err != nil {
		panic(err)
	}
	return ep
}

func newTestAlert(failureThreshold, successThreshold int, minimumReminderInterval time.Duration) *alert.Alert {
	sendOnResolved := true
	return &alert.Alert{
		Type:                    alert.TypeCustom,
		FailureThreshold:        failureThreshold,
		SuccessThreshold:        successThreshold,
		MinimumReminderInterval: minimumReminderInterval,
		SendOnResolved:          &sendOnResolved,
	}
}

func newTestConfig(withAlerting bool) *config.Config {
	cfg := &config.Config{Maintenance: maintenance.GetDefaultConfig()}
	if withAlerting {
		cfg.Alerting = &alerting.Config{
			Custom: &custom.AlertProvider{
				DefaultConfig: custom.Config{URL: "http://" + fakeAlertHost + "/[ALERT_TRIGGERED_OR_RESOLVED]", Method: "POST"},
			},
		}
	}
	return cfg
}

// monitoringStopTimeout is how long stop() waits (on the fake clock) for the monitoring goroutine to return after its
// context was cancelled. Once the context is cancelled, the goroutine should return without the clock advancing at all.
const monitoringStopTimeout = time.Minute

// startMonitoring must be called inside the synctest bubble. It starts monitoring the endpoint and returns a function
// that cancels the context (as a config reload would) and waits for the monitoring goroutine to return.
//
// If the goroutine doesn't return within monitoringStopTimeout, the test fails immediately with a clear message instead
// of hanging until the go test timeout: once the test's goroutine exits, synctest stops advancing the fake clock and
// reports the goroutine that was left behind.
func startMonitoring(t *testing.T, ep *endpoint.Endpoint, cfg *config.Config, concurrency int64) (stop func()) {
	ctx, cancelFunc = context.WithCancel(context.Background())
	monitoringSemaphore = semaphore.NewWeighted(concurrency)
	done := make(chan struct{})
	monitoringContext := ctx
	go func() {
		defer close(done)
		monitorEndpoint(ep, cfg, nil, monitoringContext)
	}()
	synctest.Wait() // Wait for the initial check to complete
	return func() {
		t.Helper()
		cancelFunc()
		select {
		case <-done:
		case <-time.After(monitoringStopTimeout):
			t.Fatalf("monitoring goroutine for endpoint with key=%s did not return within %s after its context was cancelled; the monitoring loop (including failed-state rechecks) must stop when the context is cancelled, e.g. on config reload", ep.Key(), monitoringStopTimeout)
		}
	}
}

// advance moves the fake clock forward and waits until every goroutine in the bubble is idle
func advance(d time.Duration) {
	time.Sleep(d)
	synctest.Wait()
}

// endpointState is a snapshot of the alerting state of an endpoint
type endpointState struct {
	failuresInARow  int
	successesInARow int
	triggered       bool // whether the first alert of the endpoint is triggered
}

// snapshot reads the alerting state of an endpoint being monitored while holding the monitoring semaphore, which the
// monitoring goroutine always holds while it modifies that state. Reading the fields directly from the test goroutine
// would be a data race, because the race detector doesn't see goroutines woken up by synctest's fake clock as being
// synchronized with the test goroutine.
func snapshot(ep *endpoint.Endpoint) endpointState {
	if err := monitoringSemaphore.Acquire(context.Background(), 1); err != nil {
		panic(err)
	}
	defer monitoringSemaphore.Release(1)
	state := endpointState{failuresInARow: ep.NumberOfFailuresInARow, successesInARow: ep.NumberOfSuccessesInARow}
	if len(ep.Alerts) > 0 {
		state.triggered = ep.Alerts[0].Triggered
	}
	return state
}

func storedResults(t *testing.T, ep *endpoint.Endpoint) []*endpoint.Result {
	status, err := store.Get().GetEndpointStatusByKey(ep.Key(), paging.NewEndpointStatusParams().WithResults(1, 1000))
	if err != nil {
		t.Fatal("failed to get endpoint status:", err)
	}
	return status.Results
}

func storedResultsAsString(t *testing.T, ep *endpoint.Endpoint) string {
	var sb strings.Builder
	for _, result := range storedResults(t, ep) {
		if result.Success {
			sb.WriteString("S")
		} else {
			sb.WriteString("F")
		}
	}
	return sb.String()
}

func assertAlerts(t *testing.T, transport *fakeTransport, expected ...string) {
	t.Helper()
	if actual := transport.alerts(); strings.Join(actual, ",") != strings.Join(expected, ",") {
		t.Errorf("expected alerts %v, got %v", expected, actual)
	}
}

func assertChecks(t *testing.T, transport *fakeTransport, expected int) {
	t.Helper()
	if actual := transport.checks(); actual != expected {
		t.Errorf("expected %d checks of the target, got %d", expected, actual)
	}
}

func assertStoredResults(t *testing.T, ep *endpoint.Endpoint, expected string) {
	t.Helper()
	if actual := storedResultsAsString(t, ep); actual != expected {
		t.Errorf("expected stored results to be %q, got %q", expected, actual)
	}
}

func TestMonitorEndpoint_WithoutFailedStateIsOnlyCheckedOnInterval(t *testing.T) {
	transport := setupMonitoringTest(t, false)
	synctest.Test(t, func(t *testing.T) {
		ep := newTestEndpoint("no-failed-state", nil, newTestAlert(1, 1, 0))
		stop := startMonitoring(t, ep, newTestConfig(true), 1)
		defer stop()
		assertChecks(t, transport, 1)
		assertAlerts(t, transport, "TRIGGERED")
		transport.setHealthy(true)
		advance(59*time.Minute + 59*time.Second)
		assertChecks(t, transport, 1)
		assertStoredResults(t, ep, "F")
		advance(time.Second)
		assertChecks(t, transport, 2)
		assertStoredResults(t, ep, "FS")
		assertAlerts(t, transport, "TRIGGERED", "RESOLVED")
	})
}

func TestMonitorEndpoint_FailedStateNoticesRecoverySooner(t *testing.T) {
	transport := setupMonitoringTest(t, false)
	synctest.Test(t, func(t *testing.T) {
		ep := newTestEndpoint("recovery", &endpoint.FailedState{Interval: time.Minute}, newTestAlert(1, 2, 5*time.Minute))
		stop := startMonitoring(t, ep, newTestConfig(true), 1)
		defer stop()
		// t=0: the initial check fails and triggers the alert
		assertChecks(t, transport, 1)
		assertStoredResults(t, ep, "F")
		assertAlerts(t, transport, "TRIGGERED")
		// t=10m30s: 10 rechecks have failed. They must not be stored, counted or remind about the alert, even though
		// minimum-reminder-interval (5m) has elapsed.
		advance(10*time.Minute + 30*time.Second)
		assertChecks(t, transport, 11)
		assertStoredResults(t, ep, "F")
		assertAlerts(t, transport, "TRIGGERED")
		if snapshot(ep).failuresInARow != 1 {
			t.Errorf("expected failed rechecks not to be counted, got NumberOfFailuresInARow=%d", snapshot(ep).failuresInARow)
		}
		// The endpoint recovers. The recheck at t=11m succeeds, but success-threshold is 2.
		transport.setHealthy(true)
		advance(time.Minute)
		assertChecks(t, transport, 12)
		assertStoredResults(t, ep, "FS")
		assertAlerts(t, transport, "TRIGGERED")
		if !snapshot(ep).triggered {
			t.Error("expected alert to still be triggered after 1 success with success-threshold=2")
		}
		// The recheck at t=12m resolves the alert just like a regular check would
		advance(time.Minute)
		assertChecks(t, transport, 13)
		assertStoredResults(t, ep, "FSS")
		assertAlerts(t, transport, "TRIGGERED", "RESOLVED")
		if snapshot(ep).triggered {
			t.Error("expected alert to be resolved")
		}
		// Only the stored results count toward uptime: 2 successes out of 3 results, not 2 out of 13 checks
		if uptime, _ := store.Get().GetUptimeByKey(ep.Key(), time.Now().Add(-time.Hour), time.Now()); uptime < 0.66 || uptime > 0.67 {
			t.Errorf("expected uptime to be 2/3, got %f", uptime)
		}
		// Once healthy, rechecks stop, and the regular schedule is unchanged (t=1h)
		advance(47*time.Minute + 29*time.Second) // t=59m59s
		assertChecks(t, transport, 13)
		advance(time.Second) // t=1h
		assertChecks(t, transport, 14)
		assertStoredResults(t, ep, "FSSS")
	})
}

func TestMonitorEndpoint_FailedStateDoesNotAffectFailureThresholdOrReminders(t *testing.T) {
	transport := setupMonitoringTest(t, false)
	synctest.Test(t, func(t *testing.T) {
		ep := newTestEndpoint("thresholds", &endpoint.FailedState{Interval: time.Minute}, newTestAlert(3, 2, 5*time.Minute))
		stop := startMonitoring(t, ep, newTestConfig(true), 1)
		defer stop()
		// t=59m30s: 1 regular check and 59 rechecks, all failed. failure-threshold=3 must not have been reached.
		advance(59*time.Minute + 30*time.Second)
		assertChecks(t, transport, 60)
		assertStoredResults(t, ep, "F")
		assertAlerts(t, transport)
		// t=1h: the second regular check (no recheck at the same time)
		advance(30 * time.Second)
		assertChecks(t, transport, 61)
		assertStoredResults(t, ep, "FF")
		assertAlerts(t, transport)
		if snapshot(ep).failuresInARow != 2 {
			t.Errorf("expected NumberOfFailuresInARow=2, got %d", snapshot(ep).failuresInARow)
		}
		// t=2h: the third regular check triggers the alert, exactly when it would without failed-state
		advance(59*time.Minute + 59*time.Second)
		assertAlerts(t, transport)
		advance(time.Second)
		assertChecks(t, transport, 121)
		assertStoredResults(t, ep, "FFF")
		assertAlerts(t, transport, "TRIGGERED")
		// Rechecks never send reminders, even though minimum-reminder-interval (5m) elapses many times
		advance(59*time.Minute + 59*time.Second)
		assertChecks(t, transport, 180)
		assertAlerts(t, transport, "TRIGGERED")
		// Regular checks still send reminders as usual
		advance(time.Second)
		assertChecks(t, transport, 181)
		assertStoredResults(t, ep, "FFFF")
		assertAlerts(t, transport, "TRIGGERED", "TRIGGERED")
	})
}

func TestMonitorEndpoint_FailedStateRecheckFailureWhileRecoveringOnlyResetsSuccessStreak(t *testing.T) {
	transport := setupMonitoringTest(t, false)
	synctest.Test(t, func(t *testing.T) {
		ep := newTestEndpoint("flapping", &endpoint.FailedState{Interval: time.Minute}, newTestAlert(1, 3, 5*time.Minute))
		stop := startMonitoring(t, ep, newTestConfig(true), 1)
		defer stop()
		assertAlerts(t, transport, "TRIGGERED")
		// Rechecks at t=1m and t=2m succeed
		transport.setHealthy(true)
		advance(2*time.Minute + 30*time.Second)
		assertStoredResults(t, ep, "FSS")
		if snapshot(ep).successesInARow != 2 {
			t.Fatalf("expected NumberOfSuccessesInARow=2, got %d", snapshot(ep).successesInARow)
		}
		// The recheck at t=3m fails: it's stored, but only resets the success streak
		transport.setHealthy(false)
		advance(time.Minute)
		assertStoredResults(t, ep, "FSSF")
		assertAlerts(t, transport, "TRIGGERED")
		if snapshot(ep).successesInARow != 0 {
			t.Errorf("expected the success streak to be reset, got NumberOfSuccessesInARow=%d", snapshot(ep).successesInARow)
		}
		if snapshot(ep).failuresInARow != 0 {
			t.Errorf("expected the failed recheck not to be counted as a failure, got NumberOfFailuresInARow=%d", snapshot(ep).failuresInARow)
		}
		if !snapshot(ep).triggered {
			t.Error("expected alert to still be triggered")
		}
		// Persistence of the reset streak is covered by TestHandleFailedStateRecheckFailure, because the memory store
		// used here doesn't persist triggered alerts
		// The recheck at t=4m fails again; since the last stored result is now a failure, it's discarded
		advance(time.Minute)
		assertChecks(t, transport, 5)
		assertStoredResults(t, ep, "FSSF")
		assertAlerts(t, transport, "TRIGGERED")
		// The endpoint recovers: rechecks at t=5m and t=6m aren't enough, because the streak was reset
		transport.setHealthy(true)
		advance(2 * time.Minute)
		assertStoredResults(t, ep, "FSSFSS")
		assertAlerts(t, transport, "TRIGGERED")
		// The recheck at t=7m is the 3rd success in a row and resolves the alert
		advance(time.Minute)
		assertStoredResults(t, ep, "FSSFSSS")
		assertAlerts(t, transport, "TRIGGERED", "RESOLVED")
	})
}

func TestMonitorEndpoint_FailedStateWithoutAlerting(t *testing.T) {
	transport := setupMonitoringTest(t, false)
	synctest.Test(t, func(t *testing.T) {
		ep := newTestEndpoint("no-alerting", &endpoint.FailedState{Interval: time.Minute})
		stop := startMonitoring(t, ep, newTestConfig(false), 1)
		defer stop()
		advance(5*time.Minute + 30*time.Second)
		assertChecks(t, transport, 6)
		assertStoredResults(t, ep, "F")
		// The recovery is stored at the next recheck, after which rechecks stop
		transport.setHealthy(true)
		advance(time.Minute)
		assertChecks(t, transport, 7)
		assertStoredResults(t, ep, "FS")
		advance(30 * time.Minute)
		assertChecks(t, transport, 7)
		assertStoredResults(t, ep, "FS")
	})
}

func TestMonitorEndpoint_FailedStateRechecksWhileAlertIsStillTriggered(t *testing.T) {
	// Simulates a restart while an alert was triggered: the persisted alert is loaded as triggered, and the endpoint
	// is healthy by the time the initial check runs. Rechecks must continue until the alert is resolved.
	transport := setupMonitoringTest(t, true)
	synctest.Test(t, func(t *testing.T) {
		triggeredAlert := newTestAlert(1, 2, 0)
		triggeredAlert.Triggered = true
		ep := newTestEndpoint("triggered-on-start", &endpoint.FailedState{Interval: time.Minute}, triggeredAlert)
		ep.NumberOfFailuresInARow = 1
		stop := startMonitoring(t, ep, newTestConfig(true), 1)
		defer stop()
		assertStoredResults(t, ep, "S")
		assertAlerts(t, transport)
		advance(time.Minute)
		assertChecks(t, transport, 2)
		assertStoredResults(t, ep, "SS")
		assertAlerts(t, transport, "RESOLVED")
		advance(30 * time.Minute)
		assertChecks(t, transport, 2)
	})
}

func TestMonitorEndpoint_FailedStateDuringMaintenanceWindow(t *testing.T) {
	// The endpoint is down and its alert was triggered before a maintenance window that starts at 00:00 and lasts 30m.
	// synctest's fake clock starts at 2000-01-01 00:00:00 UTC, so monitoring starts inside the window.
	transport := setupMonitoringTest(t, false)
	synctest.Test(t, func(t *testing.T) {
		triggeredAlert := newTestAlert(1, 2, 0)
		triggeredAlert.Triggered = true
		ep := &endpoint.Endpoint{
			Name:               "maintenance",
			URL:                "http://" + fakeTargetHost + "/health",
			Interval:           time.Hour,
			FailedState:        &endpoint.FailedState{Interval: 5 * time.Minute},
			Conditions:         []endpoint.Condition{"[STATUS] == 200"},
			Alerts:             []*alert.Alert{triggeredAlert},
			MaintenanceWindows: []*maintenance.Config{{Start: "00:00", Duration: 30 * time.Minute}},
		}
		if err := ep.ValidateAndSetDefaults(); err != nil {
			t.Fatal(err)
		}
		// Maintenance windows only start strictly after their start time, so start monitoring 1s into the window.
		// All times below are relative to that start.
		time.Sleep(time.Second)
		if !ep.MaintenanceWindows[0].IsUnderMaintenance() {
			t.Fatal("expected the test to start inside the maintenance window")
		}
		ep.NumberOfFailuresInARow = 1
		stop := startMonitoring(t, ep, newTestConfig(true), 1)
		defer stop()
		// While the last result is a failure, rechecks run during maintenance as usual
		assertStoredResults(t, ep, "F")
		advance(7 * time.Minute)
		assertChecks(t, transport, 2) // the recheck at t=5m failed and was discarded
		assertStoredResults(t, ep, "F")
		transport.setHealthy(true)
		// The recheck at t=10m succeeds and is stored, but alerting isn't handled during maintenance, so the alert stays
		// open and rechecks stop, because they can't resolve it while the window lasts
		advance(5 * time.Minute)
		assertChecks(t, transport, 3)
		assertStoredResults(t, ep, "FS")
		assertAlerts(t, transport)
		if !snapshot(ep).triggered {
			t.Error("expected the alert to still be triggered during maintenance")
		}
		// The window ends at t=30m, but nothing is rechecked until the next regular check (t=1h)
		advance(47*time.Minute + 59*time.Second) // t=59m59s
		assertChecks(t, transport, 3)
		assertAlerts(t, transport)
		// The regular check at t=1h counts toward success-threshold (1/2), so rechecks resume
		advance(time.Second)
		assertChecks(t, transport, 4)
		assertStoredResults(t, ep, "FSS")
		assertAlerts(t, transport)
		// The recheck at t=1h5m resolves the alert
		advance(5 * time.Minute)
		assertChecks(t, transport, 5)
		assertStoredResults(t, ep, "FSSS")
		assertAlerts(t, transport, "RESOLVED")
		// Healthy and resolved: no more rechecks
		advance(50 * time.Minute)
		assertChecks(t, transport, 5)
	})
}

func TestMonitorEndpoint_FailedStateRechecksRespectConcurrencyLimit(t *testing.T) {
	transport := setupMonitoringTest(t, false)
	synctest.Test(t, func(t *testing.T) {
		ep := newTestEndpoint("concurrency", &endpoint.FailedState{Interval: time.Minute})
		stop := startMonitoring(t, ep, newTestConfig(false), 1)
		defer stop()
		assertChecks(t, transport, 1)
		// Take the only slot, as if another endpoint was being checked
		if err := monitoringSemaphore.Acquire(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		advance(5 * time.Minute)
		assertChecks(t, transport, 1) // The recheck due at t=1m is waiting for the semaphore
		monitoringSemaphore.Release(1)
		synctest.Wait()
		assertChecks(t, transport, 2)
		// Once the recheck is done, the next one is scheduled failed-state.interval later
		advance(59 * time.Second)
		assertChecks(t, transport, 2)
		advance(time.Second)
		assertChecks(t, transport, 3)
	})
}

func TestMonitorEndpoint_FailedStateRecheckBlockedOnSemaphoreStopsOnContextCancellation(t *testing.T) {
	transport := setupMonitoringTest(t, false)
	synctest.Test(t, func(t *testing.T) {
		ep := newTestEndpoint("cancel-while-waiting", &endpoint.FailedState{Interval: time.Minute})
		stop := startMonitoring(t, ep, newTestConfig(false), 1)
		if err := monitoringSemaphore.Acquire(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		advance(2 * time.Minute)
		// If the recheck didn't give up on the semaphore when the context is cancelled, stop() would block forever,
		// and synctest would report a deadlock
		stop()
		monitoringSemaphore.Release(1)
		advance(time.Hour)
		assertChecks(t, transport, 1)
	})
}

func TestMonitorEndpoint_FailedStateRecheckTimerStopsOnContextCancellation(t *testing.T) {
	transport := setupMonitoringTest(t, false)
	synctest.Test(t, func(t *testing.T) {
		ep := newTestEndpoint("cancel", &endpoint.FailedState{Interval: time.Minute})
		stop := startMonitoring(t, ep, newTestConfig(false), 1)
		advance(2*time.Minute + 30*time.Second)
		assertChecks(t, transport, 3)
		// Cancel while a recheck is scheduled, like a config reload would. stop() waits for the monitoring goroutine to
		// return; synctest also fails the test if any goroutine or timer of the bubble is left blocked.
		stop()
		advance(3 * time.Hour)
		assertChecks(t, transport, 3)
	})
}

func TestNeedsRecheck(t *testing.T) {
	disabled := false
	maintenanceEnabled := true
	alwaysUnderMaintenance := &maintenance.Config{Enabled: &maintenanceEnabled, Start: "00:00", Duration: 24 * time.Hour}
	if err := alwaysUnderMaintenance.ValidateAndSetDefaults(); err != nil {
		t.Fatal(err)
	}
	if !alwaysUnderMaintenance.IsUnderMaintenance() {
		t.Fatal("expected maintenance window used by the test to be active")
	}
	scenarios := []struct {
		name             string
		failedState      *endpoint.FailedState
		alerts           []*alert.Alert
		withAlerting     bool
		underMaintenance bool
		lastResultFailed bool
		expected         bool
	}{
		{name: "no-failed-state", lastResultFailed: true, withAlerting: true, expected: false},
		{name: "last-result-failed", failedState: &endpoint.FailedState{Interval: time.Minute}, lastResultFailed: true, expected: true},
		{name: "last-result-failed-under-maintenance", failedState: &endpoint.FailedState{Interval: time.Minute}, lastResultFailed: true, withAlerting: true, underMaintenance: true, expected: true},
		{name: "healthy", failedState: &endpoint.FailedState{Interval: time.Minute}, alerts: []*alert.Alert{{Type: alert.TypeCustom}}, withAlerting: true, expected: false},
		{name: "alert-still-triggered", failedState: &endpoint.FailedState{Interval: time.Minute}, alerts: []*alert.Alert{{Type: alert.TypeCustom}, {Type: alert.TypeCustom, Triggered: true}}, withAlerting: true, expected: true},
		{name: "alert-still-triggered-but-disabled", failedState: &endpoint.FailedState{Interval: time.Minute}, alerts: []*alert.Alert{{Type: alert.TypeCustom, Enabled: &disabled, Triggered: true}}, withAlerting: true, expected: false},
		{name: "alert-still-triggered-without-alerting", failedState: &endpoint.FailedState{Interval: time.Minute}, alerts: []*alert.Alert{{Type: alert.TypeCustom, Triggered: true}}, expected: false},
		{name: "alert-still-triggered-under-maintenance", failedState: &endpoint.FailedState{Interval: time.Minute}, alerts: []*alert.Alert{{Type: alert.TypeCustom, Triggered: true}}, withAlerting: true, underMaintenance: true, expected: false},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			ep := &endpoint.Endpoint{Name: "ep", FailedState: scenario.failedState, Alerts: scenario.alerts}
			cfg := newTestConfig(scenario.withAlerting)
			if scenario.underMaintenance {
				ep.MaintenanceWindows = []*maintenance.Config{alwaysUnderMaintenance}
			}
			if actual := needsRecheck(ep, cfg, scenario.lastResultFailed); actual != scenario.expected {
				t.Errorf("expected %v, got %v", scenario.expected, actual)
			}
		})
	}
}

func TestHandleFailedStateRecheckFailure(t *testing.T) {
	transport := setupMonitoringTest(t, false)
	// Use a SQLite store, because the memory store doesn't persist triggered alerts
	if err := store.Initialize(&storage.Config{Type: storage.TypeSQLite, Path: t.TempDir() + "/test.db"}); err != nil {
		t.Fatal("failed to initialize store:", err)
	}
	t.Cleanup(func() {
		store.Get().Close()
		_ = store.Initialize(nil)
	})
	triggeredAlert := newTestAlert(1, 3, 5*time.Minute)
	triggeredAlert.Triggered = true
	ep := &endpoint.Endpoint{Name: "handle-recheck-failure", NumberOfSuccessesInARow: 2, Alerts: []*alert.Alert{triggeredAlert}}
	if err := store.Get().UpsertTriggeredEndpointAlert(ep, triggeredAlert); err != nil {
		t.Fatal("failed to persist triggered alert:", err)
	}
	// Without alerting, nothing happens (like HandleAlerting)
	handleFailedStateRecheckFailure(ep, nil)
	if ep.NumberOfSuccessesInARow != 2 {
		t.Errorf("expected NumberOfSuccessesInARow to be unchanged without alerting, got %d", ep.NumberOfSuccessesInARow)
	}
	handleFailedStateRecheckFailure(ep, newTestConfig(true).Alerting)
	if ep.NumberOfSuccessesInARow != 0 {
		t.Errorf("expected NumberOfSuccessesInARow=0, got %d", ep.NumberOfSuccessesInARow)
	}
	if ep.NumberOfFailuresInARow != 0 {
		t.Errorf("expected NumberOfFailuresInARow to be unchanged, got %d", ep.NumberOfFailuresInARow)
	}
	if !triggeredAlert.Triggered {
		t.Error("expected alert to still be triggered")
	}
	if len(transport.alerts()) != 0 {
		t.Errorf("expected no alert to be sent, got %v", transport.alerts())
	}
	// The reset streak must be persisted, otherwise a restart would restore the interrupted streak
	exists, _, persistedNumberOfSuccessesInARow, err := store.Get().GetTriggeredEndpointAlert(ep, triggeredAlert)
	if err != nil || !exists {
		t.Fatalf("expected the triggered alert to still be persisted, got exists=%v err=%v", exists, err)
	}
	if persistedNumberOfSuccessesInARow != 0 {
		t.Errorf("expected persisted NumberOfSuccessesInARow=0, got %d", persistedNumberOfSuccessesInARow)
	}
}
