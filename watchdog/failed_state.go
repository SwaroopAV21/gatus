package watchdog

import (
	"context"
	"time"

	"github.com/TwiN/gatus/v5/config"
	"github.com/TwiN/gatus/v5/config/endpoint"
	"github.com/TwiN/gatus/v5/metrics"
	"github.com/TwiN/gatus/v5/storage/store"
	"github.com/TwiN/logr"
)

// failedStateTracker keeps track of what is needed to decide when an endpoint with a failed-state configuration
// should be rechecked, and what to do with the result of a recheck.
//
// It is only ever accessed from the endpoint's own monitoring goroutine, so it needs no synchronization.
type failedStateTracker struct {
	// hasStoredResult is whether a result has been stored for the endpoint since monitoring started
	hasStoredResult bool

	// lastStoredResultSuccess is whether the last stored result was successful.
	// Only meaningful if hasStoredResult is true.
	lastStoredResultSuccess bool
}

// recordStoredResult must be called every time a result is stored for the endpoint.
// Passing nil (i.e. the execution was skipped) is a no-op.
func (t *failedStateTracker) recordStoredResult(result *endpoint.Result) {
	if result == nil {
		return
	}
	t.hasStoredResult = true
	t.lastStoredResultSuccess = result.Success
}

// isFailing returns whether the endpoint is in a failed state, which is the case if either:
//   - the last stored result was a failure
//   - alerting is being handled for the endpoint, and one of its alerts is still triggered (i.e. the endpoint has
//     recovered, but not for long enough to reach the alert's success-threshold yet)
//
// The second condition is why rechecks continue after the first successful recheck: without it, an alert with a
// success-threshold above 1 would have to wait for the next regular check(s) to be resolved.
//
// Triggered alerts are ignored when alerting isn't being handled (no alerting configuration, or under maintenance),
// because nothing could resolve them and the endpoint would otherwise be rechecked indefinitely while healthy.
func (t *failedStateTracker) isFailing(ep *endpoint.Endpoint, cfg *config.Config) bool {
	if t.hasStoredResult && !t.lastStoredResultSuccess {
		return true
	}
	return isHandlingAlerting(ep, cfg) && ep.HasTriggeredAlert()
}

// shouldScheduleRecheck returns whether a recheck should be scheduled ep.FailedState.Interval from now.
// A recheck is not scheduled if the endpoint isn't failing, or if the next regular check is due before the recheck
// would run anyway, so as not to hit the endpoint twice in a row.
func (t *failedStateTracker) shouldScheduleRecheck(ep *endpoint.Endpoint, cfg *config.Config, now, nextRegularCheck time.Time) bool {
	if ep.FailedState == nil || !t.isFailing(ep, cfg) {
		return false
	}
	return now.Add(ep.FailedState.Interval).Before(nextRegularCheck)
}

// isHandlingAlerting returns whether the alerts of an endpoint are currently handled
func isHandlingAlerting(ep *endpoint.Endpoint, cfg *config.Config) bool {
	return cfg.Alerting != nil && !isUnderMaintenance(ep, cfg)
}

// monitorEndpointWithFailedState monitors an endpoint that has a failed-state configuration.
//
// Regular checks run every ep.Interval, exactly like they do in monitorEndpoint. On top of that, while the endpoint
// is failing, it is rechecked every ep.FailedState.Interval (counted from the last check, regular or not).
func monitorEndpointWithFailedState(ep *endpoint.Endpoint, cfg *config.Config, extraLabels []string, ctx context.Context) {
	tracker := &failedStateTracker{}
	// Run it immediately on start
	tracker.recordStoredResult(executeEndpoint(ep, cfg, extraLabels))
	// Loop for the next executions
	ticker := time.NewTicker(ep.Interval)
	defer ticker.Stop()
	nextRegularCheck := time.Now().Add(ep.Interval)
	recheckTimer := time.NewTimer(ep.FailedState.Interval)
	recheckTimer.Stop()
	defer recheckTimer.Stop()
	for {
		// A nil channel is never ready, which disables the recheck case of the select below
		var recheckChannel <-chan time.Time
		if tracker.shouldScheduleRecheck(ep, cfg, time.Now(), nextRegularCheck) {
			logr.Debugf("[watchdog.monitorEndpointWithFailedState] Endpoint with key=%s is failing; rechecking in %s", ep.Key(), ep.FailedState.Interval)
			recheckTimer.Reset(ep.FailedState.Interval)
			recheckChannel = recheckTimer.C
		} else {
			recheckTimer.Stop()
		}
		select {
		case <-ctx.Done():
			logr.Warnf("[watchdog.monitorEndpointWithFailedState] Canceling current execution of group=%s; endpoint=%s; key=%s", ep.Group, ep.Name, ep.Key())
			return
		case tick := <-ticker.C:
			nextRegularCheck = tick.Add(ep.Interval)
			tracker.recordStoredResult(executeEndpoint(ep, cfg, extraLabels))
		case <-recheckChannel:
			executeFailedStateRecheck(ep, cfg, extraLabels, tracker)
		}
	}
}

// executeFailedStateRecheck evaluates the health of an endpoint that is in a failed state and handles the result
func executeFailedStateRecheck(ep *endpoint.Endpoint, cfg *config.Config, extraLabels []string, tracker *failedStateTracker) {
	// Acquire semaphore to limit concurrent endpoint monitoring
	if err := monitoringSemaphore.Acquire(ctx, 1); err != nil {
		// Only fails if context is cancelled (during shutdown)
		logr.Debugf("[watchdog.executeFailedStateRecheck] Context cancelled, skipping execution: %s", err.Error())
		return
	}
	defer monitoringSemaphore.Release(1)
	// If there's a connectivity checker configured, check if Gatus has internet connectivity
	if cfg.Connectivity != nil && cfg.Connectivity.Checker != nil && !cfg.Connectivity.Checker.IsConnected() {
		logr.Infof("[watchdog.executeFailedStateRecheck] No connectivity; skipping execution")
		return
	}
	logr.Debugf("[watchdog.executeFailedStateRecheck] Rechecking failing group=%s; endpoint=%s; key=%s", ep.Group, ep.Name, ep.Key())
	handleFailedStateRecheckResult(ep, cfg, extraLabels, ep.EvaluateHealth(), tracker)
}

// handleFailedStateRecheckResult handles the result of a recheck made while the endpoint was in a failed state.
//
// Rechecks only exist to notice recovery sooner, so they must never make alerting noisier:
//   - A successful recheck is handled exactly like a regular successful check: its metrics are published, it is
//     stored, and it counts toward the alerts' success-threshold, which may resolve them.
//   - A failed recheck does not count toward failure-threshold and never sends an alert or a reminder. It does reset
//     the number of successes in a row, so that success-threshold still means "that many successes in a row".
//     It is not stored (nor are its metrics published) unless the last stored result was a success, in which case it
//     is, so that an endpoint which went down again while recovering isn't displayed as healthy.
func handleFailedStateRecheckResult(ep *endpoint.Endpoint, cfg *config.Config, extraLabels []string, result *endpoint.Result, tracker *failedStateTracker) {
	if result.Success {
		logr.Infof("[watchdog.handleFailedStateRecheckResult] Failed-state recheck of group=%s; endpoint=%s; key=%s succeeded", ep.Group, ep.Name, ep.Key())
		handleEndpointResult(ep, cfg, extraLabels, result)
		tracker.recordStoredResult(result)
		return
	}
	if tracker.hasStoredResult && tracker.lastStoredResultSuccess {
		if cfg.Metrics {
			metrics.PublishMetricsForEndpoint(ep, result, extraLabels)
		}
		UpdateEndpointStatus(ep, result)
		tracker.recordStoredResult(result)
		logr.Infof("[watchdog.handleFailedStateRecheckResult] Failed-state recheck of group=%s; endpoint=%s; key=%s failed; storing result because the previous result was successful; errors=%d; duration=%s", ep.Group, ep.Name, ep.Key(), len(result.Errors), result.Duration.Round(time.Millisecond))
	} else {
		logr.Infof("[watchdog.handleFailedStateRecheckResult] Failed-state recheck of group=%s; endpoint=%s; key=%s failed; not storing result; errors=%d; duration=%s", ep.Group, ep.Name, ep.Key(), len(result.Errors), result.Duration.Round(time.Millisecond))
	}
	// Counters are only maintained while alerting is handled (see HandleAlerting), so the same applies here
	if isHandlingAlerting(ep, cfg) {
		resetNumberOfSuccessesInARow(ep)
	}
}

// resetNumberOfSuccessesInARow resets the number of successes in a row of an endpoint without counting a failure,
// and persists the new value for the endpoint's triggered alerts, just like handleAlertsToResolve does.
func resetNumberOfSuccessesInARow(ep *endpoint.Endpoint) {
	if ep.NumberOfSuccessesInARow == 0 {
		return
	}
	ep.NumberOfSuccessesInARow = 0
	for _, endpointAlert := range ep.Alerts {
		if !endpointAlert.IsEnabled() || !endpointAlert.Triggered {
			continue
		}
		if err := store.Get().UpsertTriggeredEndpointAlert(ep, endpointAlert); err != nil {
			logr.Errorf("[watchdog.resetNumberOfSuccessesInARow] Failed to update triggered endpoint alert for endpoint with key=%s: %s", ep.Key(), err.Error())
		}
	}
}
