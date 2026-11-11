package watchdog

import (
	"context"
	"time"

	"github.com/TwiN/gatus/v5/config"
	"github.com/TwiN/gatus/v5/config/endpoint"
	"github.com/TwiN/logr"
)

// checkKind describes why an endpoint is being checked
type checkKind int

const (
	// regularCheck is a check executed on the endpoint's regular interval.
	// Regular checks are stored and go through the normal alerting logic, exactly like they do for endpoints without
	// a failed-state.
	regularCheck checkKind = iota

	// recoveryCheck is an additional check executed on the endpoint's failed-state.interval while the endpoint is
	// failing. Its only purpose is to notice a recovery sooner:
	//   - a successful recovery check is handled exactly like a successful regular check (stored, counts toward
	//     success-threshold, may resolve alerts and send the resolved notification)
	//   - a failed recovery check never counts toward failure-threshold, never triggers an alert and never sends
	//     a reminder. It is only stored if the last stored result was a success (i.e. the endpoint went from
	//     healthy back to unhealthy), in which case it also resets the success streak.
	recoveryCheck
)

// logSuffix returns a suffix to append to log messages so that recovery checks can be told apart from regular checks.
// Returns an empty string for regular checks so that their log messages are unchanged.
func (kind checkKind) logSuffix() string {
	if kind == recoveryCheck {
		return "; check=recovery"
	}
	return ""
}

// failedStateTracker keeps track of what is needed to determine whether an endpoint with a failed-state is failing
type failedStateTracker struct {
	// lastStoredResultFailed is whether the last result stored for the endpoint was a failure.
	// Dropped recovery checks and skipped checks (e.g. no connectivity) don't change this value.
	lastStoredResultFailed bool
}

// observe updates the tracker with the outcome of a check
func (tracker *failedStateTracker) observe(result *endpoint.Result, stored bool) {
	if result != nil && stored {
		tracker.lastStoredResultFailed = !result.Success
	}
}

// isFailing returns whether the endpoint is currently failing, in which case recovery checks should be executed.
//
// An endpoint is considered failing if the last stored result was a failure, or if it still has at least one
// triggered alert (e.g. the endpoint started succeeding again, but success-threshold hasn't been reached yet, or
// a triggered alert was loaded from the storage on start).
func (tracker *failedStateTracker) isFailing(ep *endpoint.Endpoint) bool {
	return tracker.lastStoredResultFailed || hasTriggeredAlert(ep)
}

// hasTriggeredAlert returns whether the endpoint has at least one enabled alert that is currently triggered
func hasTriggeredAlert(ep *endpoint.Endpoint) bool {
	for _, endpointAlert := range ep.Alerts {
		if endpointAlert.IsEnabled() && endpointAlert.Triggered {
			return true
		}
	}
	return false
}

// shouldScheduleRecoveryCheck returns whether a recovery check should be scheduled failedStateInterval from now.
//
// A recovery check is only scheduled if it would run before the next regular check. Otherwise, the next regular
// check will be executed first, and there's no point in checking the endpoint twice in a row within a short period.
func shouldScheduleRecoveryCheck(now, nextRegularCheck time.Time, failedStateInterval time.Duration) bool {
	return now.Add(failedStateInterval).Before(nextRegularCheck)
}

// monitorEndpointWithFailedState monitors an endpoint that has a failed-state configured.
//
// Regular checks are executed on the endpoint's interval, exactly like they are for endpoints without a failed-state;
// the regular schedule is never shifted by recovery checks.
// While the endpoint is failing, recovery checks are additionally executed every failed-state.interval, for as long as
// the endpoint is failing. All checks are executed from this goroutine, so they never overlap.
func monitorEndpointWithFailedState(ep *endpoint.Endpoint, cfg *config.Config, extraLabels []string, ctx context.Context) {
	tracker := &failedStateTracker{}
	// Run it immediately on start
	tracker.observe(executeEndpointCheck(ep, cfg, extraLabels, regularCheck, tracker.lastStoredResultFailed))
	// Loop for the next executions
	ticker := time.NewTicker(ep.Interval)
	defer ticker.Stop()
	nextRegularCheck := time.Now().Add(ep.Interval)
	var recoveryTimer *time.Timer
	var recoveryTimerChannel <-chan time.Time // nil (blocks forever) when no recovery check is scheduled
	stopRecoveryTimer := func() {
		if recoveryTimer != nil {
			recoveryTimer.Stop()
			recoveryTimer, recoveryTimerChannel = nil, nil
		}
	}
	defer stopRecoveryTimer()
	scheduleRecoveryCheckIfFailing := func() {
		stopRecoveryTimer()
		if !tracker.isFailing(ep) {
			return
		}
		if !shouldScheduleRecoveryCheck(time.Now(), nextRegularCheck, ep.FailedState.Interval) {
			logr.Debugf("[watchdog.monitorEndpointWithFailedState] Not scheduling recovery check for group=%s; endpoint=%s; key=%s, because the next regular check is due first", ep.Group, ep.Name, ep.Key())
			return
		}
		logr.Debugf("[watchdog.monitorEndpointWithFailedState] Endpoint is failing; waiting for failed-state.interval=%s before monitoring group=%s endpoint=%s (key=%s) again", ep.FailedState.Interval, ep.Group, ep.Name, ep.Key())
		recoveryTimer = time.NewTimer(ep.FailedState.Interval)
		recoveryTimerChannel = recoveryTimer.C
	}
	scheduleRecoveryCheckIfFailing()
	for {
		select {
		case <-ctx.Done():
			logr.Warnf("[watchdog.monitorEndpointWithFailedState] Canceling current execution of group=%s; endpoint=%s; key=%s", ep.Group, ep.Name, ep.Key())
			return
		case tick := <-ticker.C:
			nextRegularCheck = tick.Add(ep.Interval)
			tracker.observe(executeEndpointCheck(ep, cfg, extraLabels, regularCheck, tracker.lastStoredResultFailed))
			scheduleRecoveryCheckIfFailing()
		case <-recoveryTimerChannel:
			tracker.observe(executeEndpointCheck(ep, cfg, extraLabels, recoveryCheck, tracker.lastStoredResultFailed))
			scheduleRecoveryCheckIfFailing()
		}
	}
}
