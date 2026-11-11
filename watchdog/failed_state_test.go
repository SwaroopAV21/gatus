package watchdog

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TwiN/gatus/v5/alerting"
	"github.com/TwiN/gatus/v5/alerting/alert"
	"github.com/TwiN/gatus/v5/alerting/provider/custom"
	"github.com/TwiN/gatus/v5/config"
	"github.com/TwiN/gatus/v5/config/endpoint"
	"github.com/TwiN/gatus/v5/config/endpoint/failedstate"
	"github.com/TwiN/gatus/v5/config/maintenance"
	"github.com/TwiN/gatus/v5/storage"
	"github.com/TwiN/gatus/v5/storage/store"
	"github.com/TwiN/gatus/v5/storage/store/common/paging"
	"golang.org/x/sync/semaphore"
)

// testTarget is an HTTP server acting as a monitored endpoint whose health can be toggled
type testTarget struct {
	server   *httptest.Server
	healthy  atomic.Bool
	requests atomic.Int64
}

func newTestTarget(t *testing.T) *testTarget {
	target := &testTarget{}
	target.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target.requests.Add(1)
		if target.healthy.Load() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	t.Cleanup(target.server.Close)
	return target
}

// testAlertReceiver is an HTTP server acting as a custom alerting provider, counting the notifications it receives
type testAlertReceiver struct {
	server    *httptest.Server
	triggered atomic.Int64
	resolved  atomic.Int64
}

func newTestAlertReceiver(t *testing.T) *testAlertReceiver {
	receiver := &testAlertReceiver{}
	receiver.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch strings.TrimSpace(string(body)) {
		case "TRIGGERED":
			receiver.triggered.Add(1)
		case "RESOLVED":
			receiver.resolved.Add(1)
		default:
			t.Errorf("unexpected alert body: %q", string(body))
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(receiver.server.Close)
	return receiver
}

func (receiver *testAlertReceiver) alertingConfig() *alerting.Config {
	return &alerting.Config{
		Custom: &custom.AlertProvider{
			DefaultConfig: custom.Config{
				URL:    receiver.server.URL,
				Method: http.MethodPost,
				Body:   "[ALERT_TRIGGERED_OR_RESOLVED]",
			},
		},
	}
}

// setUpWatchdogForTest initializes the package-level state normally initialized by Monitor, as well as an in-memory store
func setUpWatchdogForTest(t *testing.T) {
	previousCtx, previousSemaphore := ctx, monitoringSemaphore
	ctx, monitoringSemaphore = context.Background(), semaphore.NewWeighted(UnlimitedConcurrencyWeight)
	t.Cleanup(func() {
		ctx, monitoringSemaphore = previousCtx, previousSemaphore
	})
	if err := store.Initialize(nil); err != nil {
		t.Fatal("failed to initialize store:", err)
	}
}

// newTestConfig returns a config with the defaults that config validation would otherwise set
func newTestConfig(alertingConfig *alerting.Config) *config.Config {
	return &config.Config{Alerting: alertingConfig, Maintenance: maintenance.GetDefaultConfig()}
}

func newFailedStateTestEndpoint(t *testing.T, name, url string, interval, failedStateInterval time.Duration, alerts ...*alert.Alert) *endpoint.Endpoint {
	ep := &endpoint.Endpoint{
		Name:        name,
		URL:         url,
		Interval:    interval,
		FailedState: &failedstate.Config{Interval: failedStateInterval},
		Conditions:  []endpoint.Condition{"[STATUS] == 200"},
		Alerts:      alerts,
	}
	if failedStateInterval == 0 {
		ep.FailedState = nil
	}
	if err := ep.ValidateAndSetDefaults(); err != nil {
		t.Fatal("failed to validate endpoint:", err)
	}
	return ep
}

func getStoredResults(t *testing.T, ep *endpoint.Endpoint) []*endpoint.Result {
	status, err := store.Get().GetEndpointStatusByKey(ep.Key(), paging.NewEndpointStatusParams().WithResults(1, 1000))
	if err != nil {
		return nil
	}
	return status.Results
}

func getStoredEventTypes(t *testing.T, ep *endpoint.Endpoint) []endpoint.EventType {
	status, err := store.Get().GetEndpointStatusByKey(ep.Key(), paging.NewEndpointStatusParams().WithEvents(1, 1000))
	if err != nil {
		t.Fatal("failed to get endpoint status:", err)
	}
	var eventTypes []endpoint.EventType
	for _, event := range status.Events {
		eventTypes = append(eventTypes, event.Type)
	}
	return eventTypes
}

// TestFailedState_ThresholdAndReminderInteractions walks an endpoint with a failed-state through an outage and its
// recovery, calling executeEndpointCheck the same way monitorEndpointWithFailedState does, and verifies at every step
// what is stored, what the alerting counters are and which notifications were actually sent.
func TestFailedState_ThresholdAndReminderInteractions(t *testing.T) {
	setUpWatchdogForTest(t)
	target := newTestTarget(t)
	receiver := newTestAlertReceiver(t)
	cfg := newTestConfig(receiver.alertingConfig())
	enabled := true
	endpointAlert := &alert.Alert{
		Type:                    alert.TypeCustom,
		Enabled:                 &enabled,
		FailureThreshold:        2,
		SuccessThreshold:        3,
		MinimumReminderInterval: 5 * time.Minute,
		SendOnResolved:          &enabled,
	}
	ep := newFailedStateTestEndpoint(t, "threshold-and-reminder-interactions", target.server.URL, time.Hour, time.Minute, endpointAlert)
	tracker := &failedStateTracker{}
	check := func(kind checkKind) {
		tracker.observe(executeEndpointCheck(ep, cfg, nil, kind, tracker.lastStoredResultFailed))
	}
	expect := func(step string, storedResults, failuresInARow, successesInARow int, triggered bool, triggeredNotifications, resolvedNotifications int64, failing bool) {
		t.Helper()
		if actual := len(getStoredResults(t, ep)); actual != storedResults {
			t.Errorf("[%s] expected %d stored results, got %d", step, storedResults, actual)
		}
		if ep.NumberOfFailuresInARow != failuresInARow {
			t.Errorf("[%s] expected NumberOfFailuresInARow=%d, got %d", step, failuresInARow, ep.NumberOfFailuresInARow)
		}
		if ep.NumberOfSuccessesInARow != successesInARow {
			t.Errorf("[%s] expected NumberOfSuccessesInARow=%d, got %d", step, successesInARow, ep.NumberOfSuccessesInARow)
		}
		if endpointAlert.Triggered != triggered {
			t.Errorf("[%s] expected alert triggered=%v, got %v", step, triggered, endpointAlert.Triggered)
		}
		if actual := receiver.triggered.Load(); actual != triggeredNotifications {
			t.Errorf("[%s] expected %d TRIGGERED notifications (initial + reminders), got %d", step, triggeredNotifications, actual)
		}
		if actual := receiver.resolved.Load(); actual != resolvedNotifications {
			t.Errorf("[%s] expected %d RESOLVED notifications, got %d", step, resolvedNotifications, actual)
		}
		if actual := tracker.isFailing(ep); actual != failing {
			t.Errorf("[%s] expected isFailing=%v, got %v", step, failing, actual)
		}
	}
	// The endpoint goes down
	target.healthy.Store(false)
	check(regularCheck)
	expect("1st failed regular check", 1, 1, 0, false, 0, 0, true)
	// Failed recovery checks must not count toward failure-threshold, nor be stored
	check(recoveryCheck)
	check(recoveryCheck)
	check(recoveryCheck)
	expect("failed recovery checks before failure-threshold", 1, 1, 0, false, 0, 0, true)
	// Only the next regular check reaches failure-threshold
	check(regularCheck)
	expect("2nd failed regular check", 2, 2, 0, true, 1, 0, true)
	// Even with a reminder due, failed recovery checks must not send reminders
	ep.LastReminderSent = time.Now().Add(-time.Hour)
	check(recoveryCheck)
	check(recoveryCheck)
	check(recoveryCheck)
	expect("failed recovery checks with a reminder due", 2, 2, 0, true, 1, 0, true)
	// Regular checks still send reminders, just like they would without a failed-state
	check(regularCheck)
	expect("3rd failed regular check with a reminder due", 3, 3, 0, true, 2, 0, true)
	// Make a reminder due again for the rest of the test, so that any reminder sent from a recovery check is caught
	ep.LastReminderSent = time.Now().Add(-time.Hour)
	// The endpoint recovers: a successful recovery check is handled like a regular success
	target.healthy.Store(true)
	check(recoveryCheck)
	expect("1st successful recovery check", 4, 0, 1, true, 2, 0, true)
	// The endpoint goes down again before success-threshold is reached: the failure is stored because the last
	// stored result was a success, but it only resets the success streak. It must not increment
	// NumberOfFailuresInARow nor send anything, despite a reminder being due.
	target.healthy.Store(false)
	check(recoveryCheck)
	expect("failed recovery check after a stored recovery success", 5, 0, 0, true, 2, 0, true)
	// Further failed recovery checks are dropped again
	check(recoveryCheck)
	check(recoveryCheck)
	expect("failed recovery checks after the stored failure", 5, 0, 0, true, 2, 0, true)
	// The endpoint recovers for good: success-threshold must be reached again from scratch
	target.healthy.Store(true)
	check(recoveryCheck)
	check(recoveryCheck)
	expect("2 successful recovery checks", 7, 0, 2, true, 2, 0, true)
	check(recoveryCheck)
	expect("3rd successful recovery check", 8, 0, 3, false, 2, 1, false)
	// Results: F F F S F S S S
	results := getStoredResults(t, ep)
	expectedSuccesses := []bool{false, false, false, true, false, true, true, true}
	for i, result := range results {
		if result.Success != expectedSuccesses[i] {
			t.Errorf("expected stored result #%d to have success=%v, got %v", i, expectedSuccesses[i], result.Success)
		}
	}
	expectedEventTypes := []endpoint.EventType{endpoint.EventStart, endpoint.EventUnhealthy, endpoint.EventHealthy, endpoint.EventUnhealthy, endpoint.EventHealthy}
	actualEventTypes := getStoredEventTypes(t, ep)
	if len(actualEventTypes) != len(expectedEventTypes) {
		t.Fatalf("expected events %v, got %v", expectedEventTypes, actualEventTypes)
	}
	for i := range expectedEventTypes {
		if actualEventTypes[i] != expectedEventTypes[i] {
			t.Errorf("expected events %v, got %v", expectedEventTypes, actualEventTypes)
			break
		}
	}
	// Uptime must only be based on the stored results (4 successes out of 8), not on every request that was sent
	uptime, err := store.Get().GetUptimeByKey(ep.Key(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal("failed to get uptime:", err)
	}
	if uptime != 4.0/8.0 {
		t.Errorf("expected uptime to be %f, got %f", 4.0/8.0, uptime)
	}
	// 5 regular checks and 11 recovery checks were executed, but only 8 results were stored
	if requests := target.requests.Load(); requests != 16 {
		t.Errorf("expected 16 requests to have been sent to the target, got %d", requests)
	}
}

// TestFailedState_FailedRecoveryCheckNeverSendsReminder covers the case where going through the normal trigger path
// would send a reminder: failure-threshold is 1, the alert is triggered and a reminder is due. A stored failed
// recovery check (the endpoint went down again during the recovery) must still not send anything.
func TestFailedState_FailedRecoveryCheckNeverSendsReminder(t *testing.T) {
	setUpWatchdogForTest(t)
	target := newTestTarget(t)
	receiver := newTestAlertReceiver(t)
	cfg := newTestConfig(receiver.alertingConfig())
	enabled := true
	endpointAlert := &alert.Alert{
		Type:                    alert.TypeCustom,
		Enabled:                 &enabled,
		FailureThreshold:        1,
		SuccessThreshold:        2,
		MinimumReminderInterval: 5 * time.Minute,
		SendOnResolved:          &enabled,
		Triggered:               true,
	}
	ep := newFailedStateTestEndpoint(t, "failed-recovery-check-never-sends-reminder", target.server.URL, time.Hour, time.Minute, endpointAlert)
	ep.NumberOfSuccessesInARow = 1 // a successful recovery check was already stored
	ep.LastReminderSent = time.Now().Add(-time.Hour)
	result, stored := executeEndpointCheck(ep, cfg, nil, recoveryCheck, false)
	if result == nil || result.Success || !stored {
		t.Fatal("expected a failed result to be stored")
	}
	if receiver.triggered.Load() != 0 || receiver.resolved.Load() != 0 {
		t.Errorf("expected no notification to be sent, got %d triggered and %d resolved", receiver.triggered.Load(), receiver.resolved.Load())
	}
	if ep.NumberOfSuccessesInARow != 0 || ep.NumberOfFailuresInARow != 0 || !endpointAlert.Triggered {
		t.Errorf("expected only the success streak to be reset, got failures=%d, successes=%d, triggered=%v", ep.NumberOfFailuresInARow, ep.NumberOfSuccessesInARow, endpointAlert.Triggered)
	}
	// Sanity check: a failed regular check in the same situation does send the reminder
	executeEndpointCheck(ep, cfg, nil, regularCheck, true)
	if receiver.triggered.Load() != 1 {
		t.Errorf("expected a failed regular check to send a reminder, got %d triggered notifications", receiver.triggered.Load())
	}
}

// TestFailedState_RecoveryWithoutTriggeredAlert verifies that recovery checks also speed up the recovery shown on the
// UI when no alert has been triggered yet (or when no alerting is configured at all)
func TestFailedState_RecoveryWithoutTriggeredAlert(t *testing.T) {
	for _, withAlertingConfig := range []bool{true, false} {
		name := "without-alerting-config"
		if withAlertingConfig {
			name = "with-alerting-config"
		}
		t.Run(name, func(t *testing.T) {
			setUpWatchdogForTest(t)
			target := newTestTarget(t)
			receiver := newTestAlertReceiver(t)
			cfg := newTestConfig(nil)
			if withAlertingConfig {
				cfg.Alerting = receiver.alertingConfig()
			}
			enabled := true
			endpointAlert := &alert.Alert{Type: alert.TypeCustom, Enabled: &enabled, FailureThreshold: 3, SuccessThreshold: 2, SendOnResolved: &enabled}
			ep := newFailedStateTestEndpoint(t, "recovery-without-triggered-alert-"+name, target.server.URL, time.Hour, time.Minute, endpointAlert)
			tracker := &failedStateTracker{}
			tracker.observe(executeEndpointCheck(ep, cfg, nil, regularCheck, tracker.lastStoredResultFailed))
			if !tracker.isFailing(ep) {
				t.Fatal("expected endpoint to be failing")
			}
			tracker.observe(executeEndpointCheck(ep, cfg, nil, recoveryCheck, tracker.lastStoredResultFailed))
			target.healthy.Store(true)
			tracker.observe(executeEndpointCheck(ep, cfg, nil, recoveryCheck, tracker.lastStoredResultFailed))
			if tracker.isFailing(ep) {
				t.Error("expected endpoint to no longer be failing after a successful recovery check")
			}
			results := getStoredResults(t, ep)
			if len(results) != 2 || results[0].Success || !results[1].Success {
				t.Errorf("expected stored results to be [failure, success], got %d results", len(results))
			}
			if endpointAlert.Triggered || receiver.triggered.Load() != 0 || receiver.resolved.Load() != 0 {
				t.Error("expected no alert to have been triggered or resolved")
			}
		})
	}
}

// TestFailedState_FailedRecoveryCheckWithoutAlertingConfig verifies that, like HandleAlerting, a stored failed
// recovery check doesn't touch the counters when alerting isn't configured.
func TestFailedState_FailedRecoveryCheckWithoutAlertingConfig(t *testing.T) {
	setUpWatchdogForTest(t)
	target := newTestTarget(t)
	ep := newFailedStateTestEndpoint(t, "failed-recovery-check-without-alerting-config", target.server.URL, time.Hour, time.Minute)
	ep.NumberOfSuccessesInARow = 1
	// The last stored result being a success, this failed recovery check is stored, but since alerting isn't
	// configured, the counters are left alone like HandleAlerting does
	result, stored := executeEndpointCheck(ep, newTestConfig(nil), nil, recoveryCheck, false)
	if result == nil || result.Success || !stored {
		t.Fatal("expected a failed result to be stored")
	}
	if ep.NumberOfSuccessesInARow != 1 || ep.NumberOfFailuresInARow != 0 {
		t.Errorf("expected counters to be untouched, got failures=%d, successes=%d", ep.NumberOfFailuresInARow, ep.NumberOfSuccessesInARow)
	}
}

// TestHandleFailedRecoveryCheck_PersistsSuccessStreakReset verifies that the reset of NumberOfSuccessesInARow is
// persisted for triggered alerts, so that a restart doesn't resurrect a broken success streak
func TestHandleFailedRecoveryCheck_PersistsSuccessStreakReset(t *testing.T) {
	if err := store.Initialize(&storage.Config{Type: storage.TypeSQLite, Path: filepath.Join(t.TempDir(), "test.db"), MaximumNumberOfResults: 100, MaximumNumberOfEvents: 50}); err != nil {
		t.Fatal("failed to initialize store:", err)
	}
	defer store.Initialize(nil)
	enabled := true
	triggeredAlert := &alert.Alert{Type: alert.TypeCustom, Enabled: &enabled, FailureThreshold: 1, SuccessThreshold: 3, Triggered: true}
	description := "other" // so that both alerts have a different checksum
	notTriggeredAlert := &alert.Alert{Type: alert.TypeCustom, Enabled: &enabled, FailureThreshold: 5, SuccessThreshold: 3, Description: &description}
	ep := &endpoint.Endpoint{Name: "persist-success-streak-reset", URL: "https://example.org", NumberOfFailuresInARow: 0, NumberOfSuccessesInARow: 2, Alerts: []*alert.Alert{triggeredAlert, notTriggeredAlert}}
	if err := store.Get().UpsertTriggeredEndpointAlert(ep, triggeredAlert); err != nil {
		t.Fatal(err)
	}
	handleFailedRecoveryCheck(ep, &alerting.Config{})
	if ep.NumberOfSuccessesInARow != 0 || ep.NumberOfFailuresInARow != 0 {
		t.Errorf("expected only NumberOfSuccessesInARow to be reset, got failures=%d, successes=%d", ep.NumberOfFailuresInARow, ep.NumberOfSuccessesInARow)
	}
	exists, _, numberOfSuccessesInARow, err := store.Get().GetTriggeredEndpointAlert(ep, triggeredAlert)
	if err != nil || !exists {
		t.Fatalf("expected triggered alert to still be persisted, exists=%v, err=%v", exists, err)
	}
	if numberOfSuccessesInARow != 0 {
		t.Errorf("expected persisted NumberOfSuccessesInARow to be 0, got %d", numberOfSuccessesInARow)
	}
	if exists, _, _, _ := store.Get().GetTriggeredEndpointAlert(ep, notTriggeredAlert); exists {
		t.Error("expected alert that isn't triggered not to be persisted")
	}
	if triggeredAlert.Triggered != true || notTriggeredAlert.Triggered != false {
		t.Error("expected Triggered to be left untouched")
	}
}

func TestFailedStateTracker(t *testing.T) {
	enabled, disabled := true, false
	ep := &endpoint.Endpoint{Alerts: []*alert.Alert{{Type: alert.TypeCustom, Enabled: &enabled}}}
	tracker := &failedStateTracker{}
	if tracker.isFailing(ep) {
		t.Error("expected endpoint with no stored failure and no triggered alert not to be failing")
	}
	tracker.observe(&endpoint.Result{Success: false}, true)
	if !tracker.isFailing(ep) {
		t.Error("expected endpoint whose last stored result failed to be failing")
	}
	// Skipped checks (e.g. no connectivity) and dropped results must not change the state
	tracker.observe(nil, false)
	tracker.observe(&endpoint.Result{Success: true}, false)
	if !tracker.lastStoredResultFailed {
		t.Error("expected results that weren't stored to be ignored")
	}
	tracker.observe(&endpoint.Result{Success: true}, true)
	if tracker.isFailing(ep) {
		t.Error("expected endpoint whose last stored result succeeded not to be failing")
	}
	// e.g. success-threshold not reached yet, or triggered alert loaded from the storage on start
	ep.Alerts[0].Triggered = true
	if !tracker.isFailing(ep) {
		t.Error("expected endpoint with a triggered alert to be failing")
	}
	ep.Alerts[0].Enabled = &disabled
	if tracker.isFailing(ep) {
		t.Error("expected triggered alerts that are disabled to be ignored")
	}
}

func TestShouldScheduleRecoveryCheck(t *testing.T) {
	now := time.Now()
	scenarios := []struct {
		name             string
		nextRegularCheck time.Time
		interval         time.Duration
		expected         bool
	}{
		{name: "recovery-check-due-well-before-next-regular-check", nextRegularCheck: now.Add(time.Hour), interval: 10 * time.Minute, expected: true},
		{name: "recovery-check-due-just-before-next-regular-check", nextRegularCheck: now.Add(10*time.Minute + time.Millisecond), interval: 10 * time.Minute, expected: true},
		{name: "recovery-check-due-at-the-same-time-as-next-regular-check", nextRegularCheck: now.Add(10 * time.Minute), interval: 10 * time.Minute, expected: false},
		{name: "recovery-check-due-after-next-regular-check", nextRegularCheck: now.Add(5 * time.Minute), interval: 10 * time.Minute, expected: false},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			if actual := shouldScheduleRecoveryCheck(now, scenario.nextRegularCheck, scenario.interval); actual != scenario.expected {
				t.Errorf("expected %v, got %v", scenario.expected, actual)
			}
		})
	}
}

// TestMonitorEndpoint_WithFailedState runs the actual monitoring loop with short intervals and verifies that
// recovery checks are only executed while the endpoint is failing, and stop once it has recovered.
func TestMonitorEndpoint_WithFailedState(t *testing.T) {
	setUpWatchdogForTest(t)
	target := newTestTarget(t)
	ep := newFailedStateTestEndpoint(t, "monitor-endpoint-with-failed-state", target.server.URL, 600*time.Millisecond, 100*time.Millisecond)
	monitorCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		monitorEndpoint(ep, newTestConfig(nil), nil, monitorCtx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()
	// Phase 1: the endpoint is down for ~1.5s, so regular checks are executed at ~0s, ~0.6s and ~1.2s,
	// and recovery checks roughly every 100ms in between
	time.Sleep(1500 * time.Millisecond)
	requestsWhileFailing := target.requests.Load()
	resultsWhileFailing := getStoredResults(t, ep)
	if len(resultsWhileFailing) < 2 || len(resultsWhileFailing) > 3 {
		t.Fatalf("expected only the 2-3 regular checks to be stored while failing, got %d stored results", len(resultsWhileFailing))
	}
	for _, result := range resultsWhileFailing {
		if result.Success {
			t.Error("expected all stored results to be failures")
		}
	}
	if requestsWhileFailing < int64(len(resultsWhileFailing))+6 {
		t.Errorf("expected recovery checks to have been executed while failing, got %d requests for %d regular checks", requestsWhileFailing, len(resultsWhileFailing))
	}
	// Phase 2: the endpoint recovers. The next recovery check (within ~100ms) stores a success, after which only
	// regular checks must be executed
	target.healthy.Store(true)
	time.Sleep(1300 * time.Millisecond)
	requestsAfterRecovery := target.requests.Load() - requestsWhileFailing
	resultsAfterRecovery := getStoredResults(t, ep)[len(resultsWhileFailing):]
	if len(resultsAfterRecovery) == 0 {
		t.Fatal("expected the recovery to have been stored")
	}
	if !resultsAfterRecovery[0].Success || resultsAfterRecovery[0].Timestamp.Sub(resultsWhileFailing[len(resultsWhileFailing)-1].Timestamp) > 600*time.Millisecond {
		t.Error("expected the recovery to have been stored before the next regular check")
	}
	if int64(len(resultsAfterRecovery)) != requestsAfterRecovery {
		t.Errorf("expected every check after the recovery to be stored, got %d requests and %d stored results", requestsAfterRecovery, len(resultsAfterRecovery))
	}
	// 1 recovery check + 2 regular checks at most; if recovery checks kept going, there would be ~13
	if requestsAfterRecovery > 3 {
		t.Errorf("expected recovery checks to stop once the endpoint recovered, got %d requests in 1.3s", requestsAfterRecovery)
	}
}

// TestMonitorEndpoint_WithoutFailedState makes sure that a failing endpoint without a failed-state is still only
// checked on its regular interval
func TestMonitorEndpoint_WithoutFailedState(t *testing.T) {
	setUpWatchdogForTest(t)
	target := newTestTarget(t)
	ep := newFailedStateTestEndpoint(t, "monitor-endpoint-without-failed-state", target.server.URL, 500*time.Millisecond, 0)
	if ep.FailedState != nil {
		t.Fatal("expected endpoint not to have a failed-state")
	}
	monitorCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		monitorEndpoint(ep, newTestConfig(nil), nil, monitorCtx)
		close(done)
	}()
	// Regular checks at ~0s, ~0.5s and ~1s
	time.Sleep(1250 * time.Millisecond)
	cancel()
	<-done
	requests := target.requests.Load()
	if requests < 2 || requests > 3 {
		t.Errorf("expected 2-3 requests, got %d", requests)
	}
	if results := getStoredResults(t, ep); int64(len(results)) != requests {
		t.Errorf("expected every check to be stored, got %d requests and %d stored results", requests, len(results))
	}
}
