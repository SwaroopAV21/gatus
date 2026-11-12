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

// monitorEndpoint a single endpoint in a loop
func monitorEndpoint(ep *endpoint.Endpoint, cfg *config.Config, extraLabels []string, ctx context.Context) {
	// lastResultFailed is whether the last result stored for this endpoint was a failure.
	// It is only used to determine whether rechecks are needed for endpoints with a failed-state configuration.
	lastResultFailed := false
	// Run it immediately on start
	if result := executeEndpoint(ep, cfg, extraLabels); result != nil {
		lastResultFailed = !result.Success
	}
	// Loop for the next executions
	ticker := time.NewTicker(ep.Interval)
	defer ticker.Stop()
	// nextRegularCheckAt is used to avoid scheduling a recheck at the same time as (or after) the next regular check
	nextRegularCheckAt := time.Now().Add(ep.Interval)
	// Rechecks are only ever scheduled for endpoints with a failed-state configuration.
	// For every other endpoint, recheckChannel stays nil, and receiving from a nil channel blocks forever, so the select
	// below behaves exactly as if the recheck case didn't exist.
	var recheckTimer *time.Timer
	var recheckChannel <-chan time.Time
	if ep.FailedState != nil {
		recheckTimer = time.NewTimer(ep.FailedState.Interval)
		defer recheckTimer.Stop()
		recheckChannel = recheckTimer.C
		scheduleRecheck(ep, cfg, recheckTimer, lastResultFailed, nextRegularCheckAt)
	}
	for {
		select {
		case <-ctx.Done():
			logr.Warnf("[watchdog.monitorEndpoint] Canceling current execution of group=%s; endpoint=%s; key=%s", ep.Group, ep.Name, ep.Key())
			return
		case tickTime := <-ticker.C:
			if result := executeEndpoint(ep, cfg, extraLabels); result != nil {
				lastResultFailed = !result.Success
			}
			if recheckTimer != nil {
				nextRegularCheckAt = tickTime.Add(ep.Interval)
				// Rescheduling after every regular check ensures rechecks never run right after a regular check
				scheduleRecheck(ep, cfg, recheckTimer, lastResultFailed, nextRegularCheckAt)
			}
		case <-recheckChannel:
			if ctx.Err() != nil {
				// Both the context and the timer may be ready at the same time; don't start a recheck if we're stopping
				continue
			}
			if result := executeEndpointRecheck(ep, cfg, extraLabels, lastResultFailed); result != nil {
				lastResultFailed = !result.Success
			}
			scheduleRecheck(ep, cfg, recheckTimer, lastResultFailed, nextRegularCheckAt)
		}
	}
	// Just in case somebody wandered all the way to here and wonders, "what about ExternalEndpoints?"
	// Alerting is checked every time an external endpoint is pushed to Gatus, so they're not monitored
	// periodically like they are for normal endpoints.
}

// executeEndpoint performs a regular check of the endpoint.
// Returns the result that was stored, or nil if the endpoint was not checked (shutdown in progress, no connectivity).
func executeEndpoint(ep *endpoint.Endpoint, cfg *config.Config, extraLabels []string) *endpoint.Result {
	// Acquire semaphore to limit concurrent endpoint monitoring
	if err := monitoringSemaphore.Acquire(ctx, 1); err != nil {
		// Only fails if context is cancelled (during shutdown)
		logr.Debugf("[watchdog.executeEndpoint] Context cancelled, skipping execution: %s", err.Error())
		return nil
	}
	defer monitoringSemaphore.Release(1)
	// If there's a connectivity checker configured, check if Gatus has internet connectivity
	if cfg.Connectivity != nil && cfg.Connectivity.Checker != nil && !cfg.Connectivity.Checker.IsConnected() {
		logr.Infof("[watchdog.executeEndpoint] No connectivity; skipping execution")
		return nil
	}
	logr.Debugf("[watchdog.executeEndpoint] Monitoring group=%s; endpoint=%s; key=%s", ep.Group, ep.Name, ep.Key())
	result := ep.EvaluateHealth()
	if cfg.Metrics {
		metrics.PublishMetricsForEndpoint(ep, result, extraLabels)
	}
	UpdateEndpointStatus(ep, result)
	if logr.GetThreshold() == logr.LevelDebug && !result.Success {
		logr.Debugf("[watchdog.executeEndpoint] Monitored group=%s; endpoint=%s; key=%s; success=%v; errors=%d; duration=%s; body=%s", ep.Group, ep.Name, ep.Key(), result.Success, len(result.Errors), result.Duration.Round(time.Millisecond), result.Body)
	} else {
		logr.Infof("[watchdog.executeEndpoint] Monitored group=%s; endpoint=%s; key=%s; success=%v; errors=%d; duration=%s", ep.Group, ep.Name, ep.Key(), result.Success, len(result.Errors), result.Duration.Round(time.Millisecond))
	}
	inEndpointMaintenanceWindow := false
	for _, maintenanceWindow := range ep.MaintenanceWindows {
		if maintenanceWindow.IsUnderMaintenance() {
			logr.Debug("[watchdog.executeEndpoint] Under endpoint maintenance window")
			inEndpointMaintenanceWindow = true
		}
	}
	if !cfg.Maintenance.IsUnderMaintenance() && !inEndpointMaintenanceWindow {
		HandleAlerting(ep, result, cfg.Alerting)
	} else {
		logr.Debug("[watchdog.executeEndpoint] Not handling alerting because currently in the maintenance window")
	}
	logr.Debugf("[watchdog.executeEndpoint] Waiting for interval=%s before monitoring group=%s endpoint=%s (key=%s) again", ep.Interval, ep.Group, ep.Name, ep.Key())
	return result
}

// executeEndpointRecheck performs a failed-state recheck of the endpoint.
//
// Rechecks only exist to notice a recovery sooner, so they must never make alerting noisier:
//   - A failed recheck following a failed result is discarded entirely: it isn't stored, no metrics are published,
//     and alerting isn't handled at all (no failure counted, no alert triggered, no reminder sent).
//   - A successful recheck is handled exactly like a successful regular check.
//   - A failed recheck following a successful result (i.e. the endpoint was recovering, but alerts were still
//     triggered because the success-threshold hadn't been reached yet) is stored so that the UI reflects reality, but
//     it only resets the success streak; it doesn't count toward failure-threshold and never triggers alerts.
//
// lastResultFailed must be whether the last result stored for this endpoint was a failure.
// Returns the result that was stored, or nil if no result was stored.
func executeEndpointRecheck(ep *endpoint.Endpoint, cfg *config.Config, extraLabels []string, lastResultFailed bool) *endpoint.Result {
	// Rechecks are subject to the same concurrency limit as regular checks
	if err := monitoringSemaphore.Acquire(ctx, 1); err != nil {
		// Only fails if context is cancelled (during shutdown)
		logr.Debugf("[watchdog.executeEndpointRecheck] Context cancelled, skipping execution: %s", err.Error())
		return nil
	}
	defer monitoringSemaphore.Release(1)
	// If there's a connectivity checker configured, check if Gatus has internet connectivity
	if cfg.Connectivity != nil && cfg.Connectivity.Checker != nil && !cfg.Connectivity.Checker.IsConnected() {
		logr.Infof("[watchdog.executeEndpointRecheck] No connectivity; skipping execution")
		return nil
	}
	logr.Debugf("[watchdog.executeEndpointRecheck] Rechecking failing group=%s; endpoint=%s; key=%s", ep.Group, ep.Name, ep.Key())
	result := ep.EvaluateHealth()
	if !result.Success && lastResultFailed {
		logr.Infof("[watchdog.executeEndpointRecheck] Rechecked group=%s; endpoint=%s; key=%s; success=%v; errors=%d; duration=%s; still failing, discarding result", ep.Group, ep.Name, ep.Key(), result.Success, len(result.Errors), result.Duration.Round(time.Millisecond))
		return nil
	}
	if cfg.Metrics {
		metrics.PublishMetricsForEndpoint(ep, result, extraLabels)
	}
	UpdateEndpointStatus(ep, result)
	logr.Infof("[watchdog.executeEndpointRecheck] Rechecked group=%s; endpoint=%s; key=%s; success=%v; errors=%d; duration=%s", ep.Group, ep.Name, ep.Key(), result.Success, len(result.Errors), result.Duration.Round(time.Millisecond))
	if isUnderMaintenance(ep, cfg) {
		logr.Debug("[watchdog.executeEndpointRecheck] Not handling alerting because currently in the maintenance window")
		return result
	}
	if result.Success {
		HandleAlerting(ep, result, cfg.Alerting)
	} else {
		handleFailedStateRecheckFailure(ep, cfg.Alerting)
	}
	return result
}

// scheduleRecheck arms the recheck timer if the endpoint needs to be rechecked, and stops it otherwise.
//
// No recheck is scheduled if the next regular check is due at the same time or sooner, because the regular check will
// evaluate the endpoint anyway (and reschedule rechecks afterward if still needed). This prevents the target from
// being checked twice in a row.
func scheduleRecheck(ep *endpoint.Endpoint, cfg *config.Config, recheckTimer *time.Timer, lastResultFailed bool, nextRegularCheckAt time.Time) {
	if !needsRecheck(ep, cfg, lastResultFailed) {
		recheckTimer.Stop()
		return
	}
	if !time.Now().Add(ep.FailedState.Interval).Before(nextRegularCheckAt) {
		logr.Debugf("[watchdog.scheduleRecheck] Endpoint with key=%s is failing, but the next regular check is due before the next recheck would be", ep.Key())
		recheckTimer.Stop()
		return
	}
	logr.Debugf("[watchdog.scheduleRecheck] Endpoint with key=%s is failing; rechecking in failed-state.interval=%s", ep.Key(), ep.FailedState.Interval)
	recheckTimer.Reset(ep.FailedState.Interval)
}

// needsRecheck returns whether an endpoint with a failed-state configuration is considered failing, and therefore
// needs to be rechecked every failed-state.interval.
//
// An endpoint is failing if its last stored result was a failure, or if one of its alerts is still triggered (i.e. it
// has started recovering, but hasn't reached the success-threshold yet).
// The latter is ignored when alerting isn't handled (no alerting configuration, or under maintenance), because
// triggered alerts can't be resolved then, so rechecking would only store additional successful results.
func needsRecheck(ep *endpoint.Endpoint, cfg *config.Config, lastResultFailed bool) bool {
	if ep.FailedState == nil {
		return false
	}
	if lastResultFailed {
		return true
	}
	if cfg.Alerting == nil || isUnderMaintenance(ep, cfg) {
		return false
	}
	for _, endpointAlert := range ep.Alerts {
		if endpointAlert.IsEnabled() && endpointAlert.Triggered {
			return true
		}
	}
	return false
}

// isUnderMaintenance returns whether alerting should be skipped because of the global maintenance configuration or
// one of the endpoint's maintenance windows
func isUnderMaintenance(ep *endpoint.Endpoint, cfg *config.Config) bool {
	inEndpointMaintenanceWindow := false
	for _, maintenanceWindow := range ep.MaintenanceWindows {
		if maintenanceWindow.IsUnderMaintenance() {
			logr.Debug("[watchdog.isUnderMaintenance] Under endpoint maintenance window")
			inEndpointMaintenanceWindow = true
		}
	}
	return cfg.Maintenance.IsUnderMaintenance() || inEndpointMaintenanceWindow
}

// UpdateEndpointStatus persists the endpoint result in the storage
func UpdateEndpointStatus(ep *endpoint.Endpoint, result *endpoint.Result) {
	if err := store.Get().InsertEndpointResult(ep, result); err != nil {
		logr.Errorf("[watchdog.UpdateEndpointStatus] Failed to insert result in storage: %s", err.Error())
	}
}
