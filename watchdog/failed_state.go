package watchdog

import (
	"context"
	"time"

	"github.com/TwiN/gatus/v5/config"
	"github.com/TwiN/gatus/v5/config/endpoint"
	"github.com/TwiN/gatus/v5/metrics"
	"github.com/TwiN/logr"
)

// This file implements endpoint.FailedStateConfig: additional, more frequent checks of an endpoint while it is
// failing, so that a recovery is noticed sooner than the endpoint's regular interval would allow.
//
// Rules (documented in the README under "Checking failing endpoints more often"):
//   - Regular checks keep their schedule and are handled exactly like for endpoints without failed-state.
//   - A failed-state check ("fast check") is only due while the last stored result is a failure, or while an
//     enabled alert is still triggered. It is due failed-state.interval after the previous check of any kind, and
//     is skipped if that falls at or after the next regular check.
//   - A successful fast check is handled like a regular check: stored, published as metrics and passed to alerting
//     (unless under maintenance).
//   - A failed fast check never counts toward failure-threshold and never triggers an alert or a reminder. It only
//     resets the success streak so that success-threshold still means "in a row". It is only stored (and published
//     as metrics) if it flips the stored status from up to down.
//   - During a maintenance window, fast checks only run while the last stored result is a failure. Once a success
//     has been stored, they stop until the window is over, and then resume if an alert is still triggered.
//   - Fast checks go through the same concurrency limit and connectivity check as regular checks.

// clock abstracts time for the failed-state scheduler so that tests can drive it without sleeping.
type clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type checkKind int

const (
	regularCheck checkKind = iota
	failedStateCheck
)

func (k checkKind) String() string {
	if k == failedStateCheck {
		return "failed-state"
	}
	return "regular"
}

// failedStateScheduler decides when the next check of an endpoint with a failed-state configuration is due.
// It holds no endpoint state; whether failed-state checks should be considered is passed to next.
type failedStateScheduler struct {
	interval            time.Duration
	failedStateInterval time.Duration
	// nextRegularCheck is when the next regular check is due. Regular checks keep the same cadence as the ticker
	// used for endpoints without failed-state: every interval after the end of the initial check.
	nextRegularCheck time.Time
	// lastCheck is when the last check of any kind (or skipped failed-state check) started, except for the initial
	// check, for which it is when it ended (see monitorEndpointWithFailedState). The next failed-state check is due
	// failedStateInterval after it.
	lastCheck time.Time
}

// next returns when the next check is due and what kind of check it is.
// A failed-state check is only returned if considerFailedStateChecks is true and it falls strictly before the next
// regular check; otherwise the next regular check is returned.
func (s *failedStateScheduler) next(considerFailedStateChecks bool) (time.Time, checkKind) {
	if considerFailedStateChecks {
		if nextFailedStateCheck := s.lastCheck.Add(s.failedStateInterval); nextFailedStateCheck.Before(s.nextRegularCheck) {
			return nextFailedStateCheck, failedStateCheck
		}
	}
	return s.nextRegularCheck, regularCheck
}

// regularCheckDone moves nextRegularCheck forward by one interval. Like time.Ticker, regular checks that were missed
// because a check took longer than the interval are dropped rather than run back to back.
func (s *failedStateScheduler) regularCheckDone(now time.Time) {
	s.nextRegularCheck = s.nextRegularCheck.Add(s.interval)
	for !s.nextRegularCheck.After(now) {
		s.nextRegularCheck = s.nextRegularCheck.Add(s.interval)
	}
}

// monitorEndpointWithFailedState monitors an endpoint that has a failed-state configuration.
// It replaces the plain ticker loop of monitorEndpoint for such endpoints.
func monitorEndpointWithFailedState(ep *endpoint.Endpoint, cfg *config.Config, extraLabels []string, monitoringCtx context.Context, clk clock) {
	// lastStoredSuccess mirrors the status shown on the dashboard: the success of the last result that was stored.
	// It starts as true so that nothing is considered failing until a check says so.
	lastStoredSuccess := true
	// Run it immediately on start, like monitorEndpoint
	if result := executeEndpoint(ep, cfg, extraLabels); result != nil {
		lastStoredSuccess = result.Success
	}
	// Like the ticker of monitorEndpoint, regular checks are scheduled every interval from the end of the initial
	// check. Failed-state checks must be counted from the same time: counting them from the start of the initial
	// check would let the last failed-state check of the first cycle land right before the first regular check
	// (by the duration of the initial check) instead of being skipped.
	endOfInitialCheck := clk.Now()
	scheduler := &failedStateScheduler{
		interval:            ep.Interval,
		failedStateInterval: ep.FailedState.Interval,
		nextRegularCheck:    endOfInitialCheck.Add(ep.Interval),
		lastCheck:           endOfInitialCheck,
	}
	for {
		// Failed-state checks are considered while the stored status is down or an alert is still open. Whether a
		// failed-state check actually runs when it is due also depends on maintenance windows; see
		// shouldRunFailedStateCheck. When it doesn't run, the slot is still consumed so that the decision is
		// re-evaluated failed-state.interval later (e.g. once a maintenance window is over).
		dueAt, kind := scheduler.next(!lastStoredSuccess || hasTriggeredAlert(ep))
		logr.Debugf("[watchdog.monitorEndpointWithFailedState] Next check of group=%s; endpoint=%s; key=%s is a %s check in %s", ep.Group, ep.Name, ep.Key(), kind, dueAt.Sub(clk.Now()).Round(time.Millisecond))
		if wait := dueAt.Sub(clk.Now()); wait > 0 {
			select {
			case <-monitoringCtx.Done():
				logr.Warnf("[watchdog.monitorEndpointWithFailedState] Canceling current execution of group=%s; endpoint=%s; key=%s", ep.Group, ep.Name, ep.Key())
				return
			case <-clk.After(wait):
			}
		}
		if monitoringCtx.Err() != nil {
			logr.Warnf("[watchdog.monitorEndpointWithFailedState] Canceling current execution of group=%s; endpoint=%s; key=%s", ep.Group, ep.Name, ep.Key())
			return
		}
		scheduler.lastCheck = clk.Now()
		switch kind {
		case regularCheck:
			if result := executeEndpoint(ep, cfg, extraLabels); result != nil {
				lastStoredSuccess = result.Success
			}
			scheduler.regularCheckDone(clk.Now())
		case failedStateCheck:
			if !shouldRunFailedStateCheck(ep, cfg, lastStoredSuccess) {
				logr.Debugf("[watchdog.monitorEndpointWithFailedState] Skipping failed-state check of group=%s; endpoint=%s; key=%s because the last stored result is a success and the endpoint is under maintenance", ep.Group, ep.Name, ep.Key())
				continue
			}
			if result, stored := executeFailedStateCheck(ep, cfg, extraLabels, lastStoredSuccess); stored {
				lastStoredSuccess = result.Success
			}
		}
	}
}

// shouldRunFailedStateCheck returns whether a due failed-state check should run.
//
// While the last stored result is a failure, it always runs, including during maintenance windows, so that a
// recovery shows up on the dashboard. Otherwise it only runs to resolve a triggered alert, which cannot happen
// during a maintenance window, since alerting is not handled then.
func shouldRunFailedStateCheck(ep *endpoint.Endpoint, cfg *config.Config, lastStoredSuccess bool) bool {
	if !lastStoredSuccess {
		return true
	}
	return hasTriggeredAlert(ep) && !isUnderMaintenance(ep, cfg)
}

// executeFailedStateCheck runs a failed-state check of the endpoint.
//
// It returns the result and whether the result was stored. The result is nil if the check was skipped because of
// a shutdown or lack of connectivity. lastStoredSuccess is the success of the last stored result of the endpoint.
func executeFailedStateCheck(ep *endpoint.Endpoint, cfg *config.Config, extraLabels []string, lastStoredSuccess bool) (*endpoint.Result, bool) {
	// Same concurrency limit as regular checks
	if err := monitoringSemaphore.Acquire(ctx, 1); err != nil {
		// Only fails if context is cancelled (during shutdown)
		logr.Debugf("[watchdog.executeFailedStateCheck] Context cancelled, skipping execution: %s", err.Error())
		return nil, false
	}
	defer monitoringSemaphore.Release(1)
	// Same connectivity check as regular checks
	if cfg.Connectivity != nil && cfg.Connectivity.Checker != nil && !cfg.Connectivity.Checker.IsConnected() {
		logr.Infof("[watchdog.executeFailedStateCheck] No connectivity; skipping execution")
		return nil, false
	}
	logr.Debugf("[watchdog.executeFailedStateCheck] Monitoring group=%s; endpoint=%s; key=%s", ep.Group, ep.Name, ep.Key())
	result := ep.EvaluateHealth()
	// Store successes, which is what failed-state checks are for, and failures that flip the stored status from up
	// to down. Other failures are not stored so that they don't flood the history or drag down the uptime.
	stored := result.Success || lastStoredSuccess
	if stored {
		if cfg.Metrics {
			metrics.PublishMetricsForEndpoint(ep, result, extraLabels)
		}
		UpdateEndpointStatus(ep, result)
	}
	logr.Infof("[watchdog.executeFailedStateCheck] Monitored group=%s; endpoint=%s; key=%s; success=%v; errors=%d; duration=%s; stored=%v", ep.Group, ep.Name, ep.Key(), result.Success, len(result.Errors), result.Duration.Round(time.Millisecond), stored)
	if isUnderMaintenance(ep, cfg) {
		logr.Debug("[watchdog.executeFailedStateCheck] Not handling alerting because currently in the maintenance window")
		return result, stored
	}
	if result.Success {
		// A successful failed-state check counts like a regular one: it adds to the success streak, and resolves
		// alerts (and sends the resolved notification) once success-threshold is reached.
		HandleAlerting(ep, result, cfg.Alerting)
	} else {
		// A failed failed-state check must never trigger an alert or send a reminder.
		handleFailedStateCheckFailure(ep, cfg.Alerting)
	}
	return result, stored
}

// hasTriggeredAlert returns whether any enabled alert of the endpoint is currently triggered
func hasTriggeredAlert(ep *endpoint.Endpoint) bool {
	for _, endpointAlert := range ep.Alerts {
		if endpointAlert.IsEnabled() && endpointAlert.Triggered {
			return true
		}
	}
	return false
}

// isUnderMaintenance returns whether the endpoint is under a global or an endpoint-specific maintenance window
func isUnderMaintenance(ep *endpoint.Endpoint, cfg *config.Config) bool {
	if cfg.Maintenance != nil && cfg.Maintenance.IsUnderMaintenance() {
		return true
	}
	for _, maintenanceWindow := range ep.MaintenanceWindows {
		if maintenanceWindow.IsUnderMaintenance() {
			return true
		}
	}
	return false
}
