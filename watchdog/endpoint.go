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
	if ep.FailedState != nil {
		// Endpoints with a failed-state are checked more frequently while they're failing.
		// Endpoints without one go through the loop below, which is unchanged.
		monitorEndpointWithFailedState(ep, cfg, extraLabels, ctx)
		return
	}
	// Run it immediately on start
	executeEndpoint(ep, cfg, extraLabels)
	// Loop for the next executions
	ticker := time.NewTicker(ep.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logr.Warnf("[watchdog.monitorEndpoint] Canceling current execution of group=%s; endpoint=%s; key=%s", ep.Group, ep.Name, ep.Key())
			return
		case <-ticker.C:
			executeEndpoint(ep, cfg, extraLabels)
		}
	}
	// Just in case somebody wandered all the way to here and wonders, "what about ExternalEndpoints?"
	// Alerting is checked every time an external endpoint is pushed to Gatus, so they're not monitored
	// periodically like they are for normal endpoints.
}

// executeEndpoint executes a regular check of the endpoint
func executeEndpoint(ep *endpoint.Endpoint, cfg *config.Config, extraLabels []string) {
	executeEndpointCheck(ep, cfg, extraLabels, regularCheck, false)
}

// executeEndpointCheck evaluates the health of the endpoint, then stores the result and handles alerting.
//
// For regular checks, this always stores the result and calls HandleAlerting.
//
// For recovery checks (see failed_state.go):
//   - a failed recovery check is dropped (not stored, no metrics, no alerting) if lastStoredResultFailed is true,
//     because it would only confirm what the last stored result already says.
//   - a failed recovery check is stored if lastStoredResultFailed is false, because the endpoint went from healthy
//     back to unhealthy, but it only resets the success streak; it never goes through the trigger path.
//   - a successful recovery check is handled exactly like a successful regular check.
//
// Returns the result (nil if the check was skipped) and whether the result was stored.
func executeEndpointCheck(ep *endpoint.Endpoint, cfg *config.Config, extraLabels []string, kind checkKind, lastStoredResultFailed bool) (*endpoint.Result, bool) {
	// Acquire semaphore to limit concurrent endpoint monitoring
	if err := monitoringSemaphore.Acquire(ctx, 1); err != nil {
		// Only fails if context is cancelled (during shutdown)
		logr.Debugf("[watchdog.executeEndpoint] Context cancelled, skipping execution: %s", err.Error())
		return nil, false
	}
	defer monitoringSemaphore.Release(1)
	// If there's a connectivity checker configured, check if Gatus has internet connectivity
	if cfg.Connectivity != nil && cfg.Connectivity.Checker != nil && !cfg.Connectivity.Checker.IsConnected() {
		logr.Infof("[watchdog.executeEndpoint] No connectivity; skipping execution")
		return nil, false
	}
	logr.Debugf("[watchdog.executeEndpoint] Monitoring group=%s; endpoint=%s; key=%s%s", ep.Group, ep.Name, ep.Key(), kind.logSuffix())
	result := ep.EvaluateHealth()
	if kind == recoveryCheck && !result.Success && lastStoredResultFailed {
		logr.Infof("[watchdog.executeEndpoint] Monitored group=%s; endpoint=%s; key=%s; success=%v; errors=%d; duration=%s%s; endpoint is still failing, result not stored", ep.Group, ep.Name, ep.Key(), result.Success, len(result.Errors), result.Duration.Round(time.Millisecond), kind.logSuffix())
		return result, false
	}
	if cfg.Metrics {
		metrics.PublishMetricsForEndpoint(ep, result, extraLabels)
	}
	UpdateEndpointStatus(ep, result)
	if logr.GetThreshold() == logr.LevelDebug && !result.Success {
		logr.Debugf("[watchdog.executeEndpoint] Monitored group=%s; endpoint=%s; key=%s; success=%v; errors=%d; duration=%s%s; body=%s", ep.Group, ep.Name, ep.Key(), result.Success, len(result.Errors), result.Duration.Round(time.Millisecond), kind.logSuffix(), result.Body)
	} else {
		logr.Infof("[watchdog.executeEndpoint] Monitored group=%s; endpoint=%s; key=%s; success=%v; errors=%d; duration=%s%s", ep.Group, ep.Name, ep.Key(), result.Success, len(result.Errors), result.Duration.Round(time.Millisecond), kind.logSuffix())
	}
	inEndpointMaintenanceWindow := false
	for _, maintenanceWindow := range ep.MaintenanceWindows {
		if maintenanceWindow.IsUnderMaintenance() {
			logr.Debug("[watchdog.executeEndpoint] Under endpoint maintenance window")
			inEndpointMaintenanceWindow = true
		}
	}
	if !cfg.Maintenance.IsUnderMaintenance() && !inEndpointMaintenanceWindow {
		if kind == recoveryCheck && !result.Success {
			// Recovery checks may resolve alerts, but must never trigger alerts or send reminders
			handleFailedRecoveryCheck(ep, cfg.Alerting)
		} else {
			HandleAlerting(ep, result, cfg.Alerting)
		}
	} else {
		logr.Debug("[watchdog.executeEndpoint] Not handling alerting because currently in the maintenance window")
	}
	if kind == regularCheck {
		logr.Debugf("[watchdog.executeEndpoint] Waiting for interval=%s before monitoring group=%s endpoint=%s (key=%s) again", ep.Interval, ep.Group, ep.Name, ep.Key())
	}
	return result, true
}

// UpdateEndpointStatus persists the endpoint result in the storage
func UpdateEndpointStatus(ep *endpoint.Endpoint, result *endpoint.Result) {
	if err := store.Get().InsertEndpointResult(ep, result); err != nil {
		logr.Errorf("[watchdog.UpdateEndpointStatus] Failed to insert result in storage: %s", err.Error())
	}
}
