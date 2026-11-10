package watchdog

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"golang.org/x/sync/semaphore"
)

// notificationRecorder is an HTTP server used as the target of a custom alerting provider, which records whether
// each notification it receives is for a triggered (initial alert or reminder) or a resolved alert
type notificationRecorder struct {
	server        *httptest.Server
	mutex         sync.Mutex
	notifications []string
}

func newNotificationRecorder(t *testing.T) *notificationRecorder {
	recorder := &notificationRecorder{}
	recorder.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		recorder.mutex.Lock()
		recorder.notifications = append(recorder.notifications, string(body))
		recorder.mutex.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(recorder.server.Close)
	return recorder
}

func (recorder *notificationRecorder) get() []string {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return append([]string(nil), recorder.notifications...)
}

func (recorder *notificationRecorder) alertingConfig() *alerting.Config {
	return &alerting.Config{
		Custom: &custom.AlertProvider{
			DefaultConfig: custom.Config{
				URL:    recorder.server.URL,
				Method: http.MethodPost,
				Body:   "[ALERT_TRIGGERED_OR_RESOLVED]",
			},
		},
	}
}

func expectNotifications(t *testing.T, recorder *notificationRecorder, expected ...string) {
	t.Helper()
	actual := recorder.get()
	if len(actual) != len(expected) {
		t.Fatalf("expected notifications %v, got %v", expected, actual)
	}
	for i := range expected {
		if actual[i] != expected[i] {
			t.Fatalf("expected notifications %v, got %v", expected, actual)
		}
	}
}

// expectStoredResults checks the success of every result stored for the endpoint, from oldest to newest
func expectStoredResults(t *testing.T, ep *endpoint.Endpoint, expected ...bool) {
	t.Helper()
	var actual []bool
	status, err := store.Get().GetEndpointStatusByKey(ep.Key(), nil)
	if err == nil {
		for _, result := range status.Results {
			actual = append(actual, result.Success)
		}
	} else if len(expected) != 0 {
		t.Fatalf("expected stored results %v, got error: %v", expected, err)
	}
	if len(actual) != len(expected) {
		t.Fatalf("expected stored results (success) %v, got %v", expected, actual)
	}
	for i := range expected {
		if actual[i] != expected[i] {
			t.Fatalf("expected stored results (success) %v, got %v", expected, actual)
		}
	}
}

func newFailedStateTestEndpoint(name string, alerts ...*alert.Alert) *endpoint.Endpoint {
	return &endpoint.Endpoint{
		Name:        name,
		URL:         "https://example.org",
		Interval:    time.Hour,
		FailedState: &endpoint.FailedState{Interval: time.Minute},
		Conditions:  []endpoint.Condition{"[STATUS] == 200"},
		Alerts:      alerts,
	}
}

func newFailedStateTestConfig(alertingConfig *alerting.Config) *config.Config {
	return &config.Config{
		Alerting:    alertingConfig,
		Maintenance: maintenance.GetDefaultConfig(),
	}
}

func newCustomAlert(failureThreshold, successThreshold int, minimumReminderInterval time.Duration) *alert.Alert {
	enabled := true
	return &alert.Alert{
		Type:                    alert.TypeCustom,
		Enabled:                 &enabled,
		FailureThreshold:        failureThreshold,
		SuccessThreshold:        successThreshold,
		MinimumReminderInterval: minimumReminderInterval,
		SendOnResolved:          &enabled,
	}
}

// activeMaintenanceWindow returns a maintenance window that is currently active
func activeMaintenanceWindow(t *testing.T) *maintenance.Config {
	window := &maintenance.Config{
		Start:    time.Now().UTC().Add(-time.Minute).Format("15:04"),
		Duration: 2 * time.Hour,
		Timezone: "UTC",
	}
	if err := window.ValidateAndSetDefaults(); err != nil {
		t.Fatal("failed to create maintenance window:", err)
	}
	if !window.IsUnderMaintenance() {
		t.Fatal("expected maintenance window to be active")
	}
	return window
}

// regularCheck simulates a regular check, as done by executeEndpoint
func regularCheck(ep *endpoint.Endpoint, cfg *config.Config, tracker *failedStateTracker, success bool) {
	result := &endpoint.Result{Success: success, Timestamp: time.Now()}
	handleEndpointResult(ep, cfg, nil, result)
	tracker.recordStoredResult(result)
}

// recheck simulates a failed-state recheck, as done by executeFailedStateRecheck
func recheck(ep *endpoint.Endpoint, cfg *config.Config, tracker *failedStateTracker, success bool) {
	handleFailedStateRecheckResult(ep, cfg, nil, &endpoint.Result{Success: success, Timestamp: time.Now()}, tracker)
}

func TestFailedStateTracker_isFailing(t *testing.T) {
	recorder := newNotificationRecorder(t)
	scenarios := []struct {
		name                   string
		tracker                failedStateTracker
		alertTriggered         bool
		alertingConfig         *alerting.Config
		underMaintenanceWindow bool
		expected               bool
	}{
		{
			name:     "no-result-yet",
			tracker:  failedStateTracker{},
			expected: false,
		},
		{
			name:     "last-result-failed",
			tracker:  failedStateTracker{hasStoredResult: true, lastStoredResultSuccess: false},
			expected: true,
		},
		{
			name:     "last-result-succeeded",
			tracker:  failedStateTracker{hasStoredResult: true, lastStoredResultSuccess: true},
			expected: false,
		},
		{
			name:           "last-result-succeeded-but-alert-still-triggered",
			tracker:        failedStateTracker{hasStoredResult: true, lastStoredResultSuccess: true},
			alertTriggered: true,
			alertingConfig: recorder.alertingConfig(),
			expected:       true,
		},
		{
			name:           "alert-triggered-before-restart-and-no-result-yet",
			tracker:        failedStateTracker{},
			alertTriggered: true,
			alertingConfig: recorder.alertingConfig(),
			expected:       true,
		},
		{
			name:           "alert-still-triggered-but-no-alerting-config",
			tracker:        failedStateTracker{hasStoredResult: true, lastStoredResultSuccess: true},
			alertTriggered: true,
			alertingConfig: nil,
			expected:       false,
		},
		{
			name:                   "alert-still-triggered-but-under-maintenance",
			tracker:                failedStateTracker{hasStoredResult: true, lastStoredResultSuccess: true},
			alertTriggered:         true,
			alertingConfig:         recorder.alertingConfig(),
			underMaintenanceWindow: true,
			expected:               false,
		},
		{
			name:                   "last-result-failed-under-maintenance",
			tracker:                failedStateTracker{hasStoredResult: true, lastStoredResultSuccess: false},
			alertingConfig:         recorder.alertingConfig(),
			underMaintenanceWindow: true,
			expected:               true,
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			customAlert := newCustomAlert(1, 1, 0)
			customAlert.Triggered = scenario.alertTriggered
			ep := newFailedStateTestEndpoint("is-failing", customAlert)
			if scenario.underMaintenanceWindow {
				ep.MaintenanceWindows = []*maintenance.Config{activeMaintenanceWindow(t)}
			}
			cfg := newFailedStateTestConfig(scenario.alertingConfig)
			if actual := scenario.tracker.isFailing(ep, cfg); actual != scenario.expected {
				t.Errorf("expected isFailing to return %v, got %v", scenario.expected, actual)
			}
		})
	}
}

func TestFailedStateTracker_shouldScheduleRecheck(t *testing.T) {
	now := time.Now()
	failing := &failedStateTracker{hasStoredResult: true, lastStoredResultSuccess: false}
	healthy := &failedStateTracker{hasStoredResult: true, lastStoredResultSuccess: true}
	cfg := newFailedStateTestConfig(nil)
	ep := newFailedStateTestEndpoint("should-schedule-recheck") // interval=1h, failed-state.interval=1m
	if !failing.shouldScheduleRecheck(ep, cfg, now, now.Add(30*time.Minute)) {
		t.Error("expected a recheck to be scheduled for a failing endpoint whose next regular check is in 30m")
	}
	if failing.shouldScheduleRecheck(ep, cfg, now, now.Add(30*time.Second)) {
		t.Error("expected no recheck to be scheduled when the next regular check is due before the recheck would run")
	}
	if failing.shouldScheduleRecheck(ep, cfg, now, now.Add(time.Minute)) {
		t.Error("expected no recheck to be scheduled when the next regular check is due at the same time as the recheck")
	}
	if healthy.shouldScheduleRecheck(ep, cfg, now, now.Add(30*time.Minute)) {
		t.Error("expected no recheck to be scheduled for a healthy endpoint")
	}
	ep.FailedState = nil
	if failing.shouldScheduleRecheck(ep, cfg, now, now.Add(30*time.Minute)) {
		t.Error("expected no recheck to be scheduled for an endpoint without failed-state configuration")
	}
}

func TestFailedStateRecheck_FailuresDoNotCountTowardFailureThreshold(t *testing.T) {
	t.Setenv("MOCK_ALERT_PROVIDER", "")
	defer store.Get().Clear()
	recorder := newNotificationRecorder(t)
	cfg := newFailedStateTestConfig(recorder.alertingConfig())
	ep := newFailedStateTestEndpoint("failures-do-not-count", newCustomAlert(2, 2, 0))
	tracker := &failedStateTracker{}

	regularCheck(ep, cfg, tracker, false)
	verify(t, ep, 1, 0, false, "The alert shouldn't have triggered after 1 regular failure (failure-threshold is 2)")
	for i := 0; i < 10; i++ {
		recheck(ep, cfg, tracker, false)
	}
	verify(t, ep, 1, 0, false, "Failed rechecks must not count toward failure-threshold")
	expectNotifications(t, recorder)
	expectStoredResults(t, ep, false)

	regularCheck(ep, cfg, tracker, false)
	verify(t, ep, 2, 0, true, "The alert should've triggered after the 2nd regular failure")
	expectNotifications(t, recorder, "TRIGGERED")
	expectStoredResults(t, ep, false, false)
}

func TestFailedStateRecheck_FailuresDoNotSendReminders(t *testing.T) {
	t.Setenv("MOCK_ALERT_PROVIDER", "")
	defer store.Get().Clear()
	recorder := newNotificationRecorder(t)
	cfg := newFailedStateTestConfig(recorder.alertingConfig())
	customAlert := newCustomAlert(1, 2, 5*time.Minute)
	customAlert.Triggered = true
	ep := newFailedStateTestEndpoint("failures-do-not-remind", customAlert)
	ep.NumberOfFailuresInARow = 1
	lastReminderSent := time.Now().Add(-time.Hour) // a reminder is overdue
	ep.LastReminderSent = lastReminderSent
	tracker := &failedStateTracker{hasStoredResult: true, lastStoredResultSuccess: false}

	for i := 0; i < 5; i++ {
		recheck(ep, cfg, tracker, false)
	}
	verify(t, ep, 1, 0, true, "The alert should still be triggered")
	expectNotifications(t, recorder)
	if !ep.LastReminderSent.Equal(lastReminderSent) {
		t.Error("Failed rechecks must not update LastReminderSent")
	}
	expectStoredResults(t, ep)

	// Sanity check: the reminder was indeed due, and the next regular check sends it
	regularCheck(ep, cfg, tracker, false)
	verify(t, ep, 2, 0, true, "The alert should still be triggered")
	expectNotifications(t, recorder, "TRIGGERED")
}

func TestFailedStateRecheck_SuccessesResolveAlert(t *testing.T) {
	t.Setenv("MOCK_ALERT_PROVIDER", "")
	defer store.Get().Clear()
	recorder := newNotificationRecorder(t)
	cfg := newFailedStateTestConfig(recorder.alertingConfig())
	ep := newFailedStateTestEndpoint("successes-resolve", newCustomAlert(1, 2, 0))
	tracker := &failedStateTracker{}

	regularCheck(ep, cfg, tracker, false)
	verify(t, ep, 1, 0, true, "The alert should've triggered")
	expectNotifications(t, recorder, "TRIGGERED")
	if !tracker.isFailing(ep, cfg) {
		t.Fatal("The endpoint should be failing")
	}

	recheck(ep, cfg, tracker, false)
	recheck(ep, cfg, tracker, true)
	verify(t, ep, 0, 1, true, "The alert should still be triggered (success-threshold is 2)")
	expectNotifications(t, recorder, "TRIGGERED")
	if !tracker.isFailing(ep, cfg) {
		t.Fatal("The endpoint should still be in a failed state until its alert is resolved")
	}

	recheck(ep, cfg, tracker, true)
	verify(t, ep, 0, 2, false, "The alert should've been resolved by the 2nd successful recheck")
	expectNotifications(t, recorder, "TRIGGERED", "RESOLVED")
	if tracker.isFailing(ep, cfg) {
		t.Fatal("The endpoint should no longer be in a failed state")
	}
	// The failed recheck isn't stored, but the successful ones are
	expectStoredResults(t, ep, false, true, true)
}

func TestFailedStateRecheck_FailureAfterSuccessIsStoredOnceAndResetsSuccesses(t *testing.T) {
	t.Setenv("MOCK_ALERT_PROVIDER", "")
	defer store.Get().Clear()
	recorder := newNotificationRecorder(t)
	cfg := newFailedStateTestConfig(recorder.alertingConfig())
	ep := newFailedStateTestEndpoint("flapping", newCustomAlert(1, 3, 0))
	tracker := &failedStateTracker{}

	regularCheck(ep, cfg, tracker, false)
	recheck(ep, cfg, tracker, true)
	recheck(ep, cfg, tracker, true)
	verify(t, ep, 0, 2, true, "The alert should still be triggered (success-threshold is 3)")

	recheck(ep, cfg, tracker, false)
	verify(t, ep, 0, 0, true, "A failed recheck should reset the successes in a row without counting as a failure")
	recheck(ep, cfg, tracker, false)
	recheck(ep, cfg, tracker, false)
	verify(t, ep, 0, 0, true, "Failed rechecks must not count toward failure-threshold")
	// Only the first failed recheck following a successful result is stored
	expectStoredResults(t, ep, false, true, true, false)
	expectNotifications(t, recorder, "TRIGGERED")

	recheck(ep, cfg, tracker, true)
	recheck(ep, cfg, tracker, true)
	verify(t, ep, 0, 2, true, "The alert should still be triggered, because the successes in a row were reset")
	recheck(ep, cfg, tracker, true)
	verify(t, ep, 0, 3, false, "The alert should've been resolved")
	expectNotifications(t, recorder, "TRIGGERED", "RESOLVED")
	expectStoredResults(t, ep, false, true, true, false, true, true, true)
}

func TestFailedStateRecheck_WithoutAlertingConfig(t *testing.T) {
	defer store.Get().Clear()
	cfg := newFailedStateTestConfig(nil)
	ep := newFailedStateTestEndpoint("no-alerting")
	tracker := &failedStateTracker{}

	regularCheck(ep, cfg, tracker, false)
	if !tracker.isFailing(ep, cfg) {
		t.Fatal("The endpoint should be failing")
	}
	recheck(ep, cfg, tracker, false)
	recheck(ep, cfg, tracker, true)
	if tracker.isFailing(ep, cfg) {
		t.Fatal("The endpoint should no longer be failing")
	}
	expectStoredResults(t, ep, false, true)
}

func TestFailedStateRecheck_UnderMaintenance(t *testing.T) {
	t.Setenv("MOCK_ALERT_PROVIDER", "")
	defer store.Get().Clear()
	recorder := newNotificationRecorder(t)
	cfg := newFailedStateTestConfig(recorder.alertingConfig())
	customAlert := newCustomAlert(1, 2, 0)
	customAlert.Triggered = true
	ep := newFailedStateTestEndpoint("under-maintenance", customAlert)
	ep.NumberOfFailuresInARow, ep.NumberOfSuccessesInARow = 1, 1
	ep.MaintenanceWindows = []*maintenance.Config{activeMaintenanceWindow(t)}
	tracker := &failedStateTracker{hasStoredResult: true, lastStoredResultSuccess: false}

	recheck(ep, cfg, tracker, false)
	verify(t, ep, 1, 1, true, "Counters are not updated while under maintenance")
	recheck(ep, cfg, tracker, true)
	verify(t, ep, 1, 1, true, "Alerts are not resolved while under maintenance")
	expectNotifications(t, recorder)
	expectStoredResults(t, ep, true)
	if tracker.isFailing(ep, cfg) {
		t.Fatal("The endpoint shouldn't be rechecked while healthy, since its alert can't be resolved under maintenance")
	}
}

// failingThenHealthyServer returns a server that responds with a 500 to the first numberOfFailures requests, and
// with a 200 after that, as well as a pointer to the number of requests it has received
func failingThenHealthyServer(t *testing.T, numberOfFailures int64) (*httptest.Server, *atomic.Int64) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) <= numberOfFailures {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server, &requests
}

// setUpMonitoringGlobals sets the package-level context and semaphore used by the watchdog, the same way Monitor does
func setUpMonitoringGlobals(concurrency int64) {
	ctx, cancelFunc = context.WithCancel(context.Background())
	monitoringSemaphore = semaphore.NewWeighted(concurrency)
}

// startMonitoringForTest starts monitoring the endpoint like Monitor does, and returns a function that stops it
// like Shutdown does (by cancelling the shared context), then waits for the monitoring goroutine to return
func startMonitoringForTest(t *testing.T, ep *endpoint.Endpoint, cfg *config.Config) (stop func()) {
	setUpMonitoringGlobals(UnlimitedConcurrencyWeight)
	monitoringContext := ctx
	done := make(chan struct{})
	go func() {
		monitorEndpoint(ep, cfg, nil, monitoringContext)
		close(done)
	}()
	return func() {
		cancelFunc()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("monitoring did not stop")
		}
	}
}

func waitFor(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for " + description)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMonitorEndpointWithFailedState(t *testing.T) {
	t.Setenv("MOCK_ALERT_PROVIDER", "")
	defer store.Get().Clear()
	recorder := newNotificationRecorder(t)
	target, requests := failingThenHealthyServer(t, 3)
	ep := &endpoint.Endpoint{
		Name:        "monitor-with-failed-state",
		URL:         target.URL,
		Interval:    time.Hour, // no regular check will run during this test, other than the one on start
		FailedState: &endpoint.FailedState{Interval: 20 * time.Millisecond},
		Conditions:  []endpoint.Condition{"[STATUS] == 200"},
		Alerts:      []*alert.Alert{newCustomAlert(1, 2, 0)},
	}
	if err := ep.ValidateAndSetDefaults(); err != nil {
		t.Fatal(err)
	}
	cfg := newFailedStateTestConfig(recorder.alertingConfig())

	stop := startMonitoringForTest(t, ep, cfg)
	waitFor(t, "the alert to be resolved", func() bool { return len(recorder.get()) == 2 })
	// Give the loop plenty of time to (wrongly) recheck the endpoint again
	time.Sleep(20 * ep.FailedState.Interval)
	stop()

	// 1 regular check on start (failure), 2 failed rechecks, then 2 successful rechecks to reach success-threshold.
	// No rechecks after the alert has been resolved.
	if actual := requests.Load(); actual != 5 {
		t.Errorf("expected the endpoint to have been checked 5 times, got %d", actual)
	}
	expectNotifications(t, recorder, "TRIGGERED", "RESOLVED")
	expectStoredResults(t, ep, false, true, true)
	verify(t, ep, 0, 2, false, "The alert should've been resolved")
}

func TestMonitorEndpointWithoutFailedState(t *testing.T) {
	defer store.Get().Clear()
	target, requests := failingThenHealthyServer(t, 3)
	ep := &endpoint.Endpoint{
		Name:       "monitor-without-failed-state",
		URL:        target.URL,
		Interval:   time.Hour,
		Conditions: []endpoint.Condition{"[STATUS] == 200"},
	}
	if err := ep.ValidateAndSetDefaults(); err != nil {
		t.Fatal(err)
	}
	stop := startMonitoringForTest(t, ep, newFailedStateTestConfig(nil))
	waitFor(t, "the regular check on start", func() bool { return requests.Load() == 1 })
	time.Sleep(200 * time.Millisecond)
	stop()
	if actual := requests.Load(); actual != 1 {
		t.Errorf("expected the endpoint to have been checked once, got %d", actual)
	}
	expectStoredResults(t, ep, false)
}

// countingServer returns a server that always responds with the given status code, as well as a pointer to the number
// of requests it has received
func countingServer(t *testing.T, statusCode int) (*httptest.Server, *atomic.Int64) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(statusCode)
	}))
	t.Cleanup(server.Close)
	return server, &requests
}

// newValidatedEndpointForTest returns a validated endpoint with a failed-state configuration targeting url
func newValidatedEndpointForTest(t *testing.T, name, url string, interval, failedStateInterval time.Duration, alerts ...*alert.Alert) *endpoint.Endpoint {
	ep := &endpoint.Endpoint{
		Name:        name,
		URL:         url,
		Interval:    interval,
		FailedState: &endpoint.FailedState{Interval: failedStateInterval},
		Conditions:  []endpoint.Condition{"[STATUS] == 200"},
		Alerts:      alerts,
	}
	if err := ep.ValidateAndSetDefaults(); err != nil {
		t.Fatal(err)
	}
	return ep
}

func TestExecuteFailedStateRecheck_SkippedWithoutConnectivity(t *testing.T) {
	defer store.Get().Clear()
	setUpMonitoringGlobals(UnlimitedConcurrencyWeight)
	// Nothing listens on this address, so the connectivity checker reports that there is no connectivity
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddress := listener.Addr().String()
	_ = listener.Close()
	// Something listens on this one, so the connectivity checker reports that there is connectivity
	openListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer openListener.Close()

	target, requests := countingServer(t, http.StatusOK)
	ep := newValidatedEndpointForTest(t, "recheck-without-connectivity", target.URL, time.Hour, time.Minute)
	cfg := newFailedStateTestConfig(nil)
	cfg.Connectivity = &connectivity.Config{Checker: &connectivity.Checker{Target: closedAddress, Interval: time.Hour}}
	if cfg.Connectivity.Checker.IsConnected() {
		t.Fatal("expected the connectivity checker to report no connectivity")
	}
	tracker := &failedStateTracker{hasStoredResult: true, lastStoredResultSuccess: false}

	executeFailedStateRecheck(ep, cfg, nil, tracker)
	if actual := requests.Load(); actual != 0 {
		t.Errorf("expected the recheck to be skipped without connectivity, but the endpoint was requested %d time(s)", actual)
	}
	if !tracker.isFailing(ep, cfg) {
		t.Error("a skipped recheck should not change the endpoint's state")
	}
	expectStoredResults(t, ep)

	// Control: the same recheck runs once there is connectivity
	cfg.Connectivity = &connectivity.Config{Checker: &connectivity.Checker{Target: openListener.Addr().String(), Interval: time.Hour}}
	executeFailedStateRecheck(ep, cfg, nil, tracker)
	if actual := requests.Load(); actual != 1 {
		t.Errorf("expected the recheck to run with connectivity, but the endpoint was requested %d time(s)", actual)
	}
	expectStoredResults(t, ep, true)
}

func TestExecuteFailedStateRecheck_SkippedWhenContextIsCancelled(t *testing.T) {
	defer store.Get().Clear()
	target, requests := countingServer(t, http.StatusOK)
	ep := newValidatedEndpointForTest(t, "recheck-context-cancelled", target.URL, time.Hour, time.Minute)
	cfg := newFailedStateTestConfig(nil)
	tracker := &failedStateTracker{hasStoredResult: true, lastStoredResultSuccess: false}

	t.Run("already-cancelled", func(t *testing.T) {
		setUpMonitoringGlobals(UnlimitedConcurrencyWeight)
		cancelFunc()
		executeFailedStateRecheck(ep, cfg, nil, tracker)
		if actual := requests.Load(); actual != 0 {
			t.Errorf("expected the recheck to be skipped, but the endpoint was requested %d time(s)", actual)
		}
	})
	t.Run("cancelled-while-waiting-for-semaphore", func(t *testing.T) {
		setUpMonitoringGlobals(1)
		if err := monitoringSemaphore.Acquire(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		defer monitoringSemaphore.Release(1)
		done := make(chan struct{})
		go func() {
			executeFailedStateRecheck(ep, cfg, nil, tracker)
			close(done)
		}()
		time.Sleep(50 * time.Millisecond)
		cancelFunc()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the recheck should've returned once the context was cancelled")
		}
		if actual := requests.Load(); actual != 0 {
			t.Errorf("expected the recheck to be skipped, but the endpoint was requested %d time(s)", actual)
		}
	})
	expectStoredResults(t, ep)
	if !tracker.isFailing(ep, cfg) {
		t.Error("a skipped recheck should not change the endpoint's state")
	}
}

func TestMonitorEndpointWithFailedState_StopsRecheckingOnShutdown(t *testing.T) {
	defer store.Get().Clear()
	target, requests := countingServer(t, http.StatusInternalServerError)
	ep := newValidatedEndpointForTest(t, "recheck-stops-on-shutdown", target.URL, time.Hour, 20*time.Millisecond)
	stop := startMonitoringForTest(t, ep, newFailedStateTestConfig(nil))
	waitFor(t, "a few rechecks", func() bool { return requests.Load() >= 3 })
	stop()
	requestsAtShutdown := requests.Load()
	time.Sleep(10 * ep.FailedState.Interval)
	if actual := requests.Load(); actual != requestsAtShutdown {
		t.Errorf("expected no rechecks after shutdown, but the endpoint was requested %d more time(s)", actual-requestsAtShutdown)
	}
}

func TestExecuteFailedStateRecheck_WaitsForSemaphoreLikeRegularCheck(t *testing.T) {
	defer store.Get().Clear()
	cfg := newFailedStateTestConfig(nil)
	scenarios := []struct {
		name    string
		execute func(ep *endpoint.Endpoint)
	}{
		{
			name: "regular-check",
			execute: func(ep *endpoint.Endpoint) {
				executeEndpoint(ep, cfg, nil)
			},
		},
		{
			name: "failed-state-recheck",
			execute: func(ep *endpoint.Endpoint) {
				executeFailedStateRecheck(ep, cfg, nil, &failedStateTracker{hasStoredResult: true, lastStoredResultSuccess: false})
			},
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			target, requests := countingServer(t, http.StatusOK)
			ep := newValidatedEndpointForTest(t, "semaphore-"+scenario.name, target.URL, time.Hour, time.Minute)
			setUpMonitoringGlobals(1)
			defer cancelFunc()
			// Occupy the only slot, as if another endpoint was being evaluated
			if err := monitoringSemaphore.Acquire(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				scenario.execute(ep)
				close(done)
			}()
			time.Sleep(100 * time.Millisecond)
			if actual := requests.Load(); actual != 0 {
				t.Fatalf("expected the check to wait for the semaphore, but the endpoint was requested %d time(s)", actual)
			}
			select {
			case <-done:
				t.Fatal("expected the check to still be waiting for the semaphore")
			default:
			}
			monitoringSemaphore.Release(1)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("the check should've run once the semaphore was released")
			}
			if actual := requests.Load(); actual != 1 {
				t.Errorf("expected the endpoint to be requested once the semaphore was released, got %d", actual)
			}
			// The check must release the semaphore once it's done
			if !monitoringSemaphore.TryAcquire(1) {
				t.Error("expected the check to release the semaphore")
			}
		})
	}
}

func TestFailedStateRecheck_SuccessResetsFailuresBeforeAlertIsTriggered(t *testing.T) {
	t.Setenv("MOCK_ALERT_PROVIDER", "")
	defer store.Get().Clear()
	recorder := newNotificationRecorder(t)
	cfg := newFailedStateTestConfig(recorder.alertingConfig())
	ep := newFailedStateTestEndpoint("success-resets-failures", newCustomAlert(3, 2, 0))
	tracker := &failedStateTracker{}

	regularCheck(ep, cfg, tracker, false)
	regularCheck(ep, cfg, tracker, false)
	verify(t, ep, 2, 0, false, "The alert shouldn't have triggered after 2 failures (failure-threshold is 3)")
	recheck(ep, cfg, tracker, true)
	verify(t, ep, 0, 1, false, "A successful recheck should reset the failures in a row")
	if tracker.isFailing(ep, cfg) {
		t.Error("The endpoint should no longer be failing")
	}
	// Without the reset, this would've been the 3rd failure in a row and the alert would've triggered
	regularCheck(ep, cfg, tracker, false)
	verify(t, ep, 1, 0, false, "The alert shouldn't have triggered, because the failures in a row were reset")
	expectNotifications(t, recorder)
	expectStoredResults(t, ep, false, false, true, false)
}

func TestFailedStateRecheck_Metrics(t *testing.T) {
	defer store.Get().Clear()
	cfg := newFailedStateTestConfig(nil)
	cfg.Metrics = true
	registry := prometheus.NewRegistry()
	metrics.InitializePrometheusMetrics(cfg, registry)
	defer metrics.UnregisterPrometheusMetrics()
	ep := newFailedStateTestEndpoint("metrics")
	tracker := &failedStateTracker{}
	expectResultsTotal := func(failures, successes int) {
		t.Helper()
		expected := "# HELP gatus_results_total Number of results per endpoint\n# TYPE gatus_results_total counter\n"
		if failures > 0 {
			expected += fmt.Sprintf("gatus_results_total{group=\"\",key=\"_metrics\",name=\"metrics\",success=\"false\",type=\"HTTP\"} %d\n", failures)
		}
		if successes > 0 {
			expected += fmt.Sprintf("gatus_results_total{group=\"\",key=\"_metrics\",name=\"metrics\",success=\"true\",type=\"HTTP\"} %d\n", successes)
		}
		if err := testutil.GatherAndCompare(registry, strings.NewReader(expected), "gatus_results_total"); err != nil {
			t.Fatal(err)
		}
	}

	regularCheck(ep, cfg, tracker, false) // regularCheck doesn't go through executeEndpoint, but handleEndpointResult publishes metrics
	expectResultsTotal(1, 0)
	recheck(ep, cfg, tracker, false)
	recheck(ep, cfg, tracker, false)
	recheck(ep, cfg, tracker, false)
	expectResultsTotal(1, 0) // dropped failed rechecks are not published
	recheck(ep, cfg, tracker, true)
	expectResultsTotal(1, 1) // successful rechecks are published
	recheck(ep, cfg, tracker, false)
	expectResultsTotal(2, 1) // the first failed recheck after a success is stored, so it's published too
	recheck(ep, cfg, tracker, false)
	expectResultsTotal(2, 1)
	expectStoredResults(t, ep, false, true, false)
}

func TestMonitorEndpointWithFailedState_RechecksDoNotShiftRegularChecks(t *testing.T) {
	defer store.Get().Clear()
	const interval, failedStateInterval = 500 * time.Millisecond, 120 * time.Millisecond
	target, requests := countingServer(t, http.StatusInternalServerError)
	ep := newValidatedEndpointForTest(t, "cadence", target.URL, interval, failedStateInterval)
	stop := startMonitoringForTest(t, ep, newFailedStateTestConfig(nil))
	time.Sleep(3*interval + interval/2)
	stop()

	// The endpoint fails every time, so the only stored results are those of the regular checks (failed rechecks are
	// dropped): on start, then every interval after that
	status, err := store.Get().GetEndpointStatusByKey(ep.Key(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Results) != 4 {
		t.Fatalf("expected 4 regular checks (on start, then every %s), got %d", interval, len(status.Results))
	}
	start := status.Results[0].Timestamp
	for i, result := range status.Results {
		expected := start.Add(time.Duration(i) * interval)
		if drift := result.Timestamp.Sub(expected); drift > 150*time.Millisecond || drift < -150*time.Millisecond {
			t.Errorf("expected regular check #%d at %s, got %s (drift=%s)", i, expected.Format(time.StampMilli), result.Timestamp.Format(time.StampMilli), drift)
		}
	}
	// Sanity check: rechecks did run in between (3 per interval with these durations: 120ms, 240ms and 360ms after a
	// regular check, plus one at 480ms if it's scheduled before the next regular check)
	if actual := requests.Load(); actual < 4+3*3 {
		t.Errorf("expected rechecks to have run between regular checks, but the endpoint was only requested %d times", actual)
	}
}

func TestFailedStateRecheck_SuccessCountResetIsPersisted(t *testing.T) {
	t.Setenv("MOCK_ALERT_PROVIDER", "")
	// The memory store doesn't persist triggered alerts, so SQLite is used instead
	err := store.Initialize(&storage.Config{
		Type:                   storage.TypeSQLite,
		Path:                   filepath.Join(t.TempDir(), "gatus.db"),
		MaximumNumberOfResults: storage.DefaultMaximumNumberOfResults,
		MaximumNumberOfEvents:  storage.DefaultMaximumNumberOfEvents,
	})
	if err != nil {
		t.Fatal("failed to initialize sqlite store:", err)
	}
	defer func() {
		store.Get().Close()
		_ = store.Initialize(nil)
	}()
	recorder := newNotificationRecorder(t)
	cfg := newFailedStateTestConfig(recorder.alertingConfig())
	customAlert := newCustomAlert(1, 3, 0)
	ep := newFailedStateTestEndpoint("persisted-success-count", customAlert)
	tracker := &failedStateTracker{}
	expectPersistedNumberOfSuccessesInARow := func(expected int) {
		t.Helper()
		exists, _, numberOfSuccessesInARow, err := store.Get().GetTriggeredEndpointAlert(ep, customAlert)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatal("expected the triggered alert to be persisted")
		}
		if numberOfSuccessesInARow != expected {
			t.Fatalf("expected the persisted number of successes in a row to be %d, got %d", expected, numberOfSuccessesInARow)
		}
	}

	regularCheck(ep, cfg, tracker, false)
	verify(t, ep, 1, 0, true, "The alert should've triggered")
	expectPersistedNumberOfSuccessesInARow(0)
	recheck(ep, cfg, tracker, true)
	recheck(ep, cfg, tracker, true)
	verify(t, ep, 0, 2, true, "The alert should still be triggered (success-threshold is 3)")
	expectPersistedNumberOfSuccessesInARow(2)
	recheck(ep, cfg, tracker, false)
	verify(t, ep, 0, 0, true, "A failed recheck should reset the successes in a row")
	// Had this not been persisted, a restart would resume from 2 successes in a row, and the next success would
	// resolve the alert even though the endpoint failed in between
	expectPersistedNumberOfSuccessesInARow(0)
}
