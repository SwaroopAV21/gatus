package endpoint

import (
	"errors"
	"time"
)

var (
	// ErrFailedStateWithInvalidInterval is the error with which Gatus will panic if an endpoint has a failed-state
	// block without a positive interval
	ErrFailedStateWithInvalidInterval = errors.New("failed-state.interval must be set to a positive duration")

	// ErrFailedStateIntervalNotShorterThanInterval is the error with which Gatus will panic if an endpoint's
	// failed-state.interval is not shorter than the endpoint's interval, in which case it would never be used
	ErrFailedStateIntervalNotShorterThanInterval = errors.New("failed-state.interval must be shorter than the endpoint's interval")

	// ErrFailedStateIntervalTooShortForDomainExpirationPlaceholder is the error with which Gatus will panic if an
	// endpoint with a condition using the DomainExpirationPlaceholder has a failed-state.interval under 5 minutes.
	// See ErrInvalidEndpointIntervalForDomainExpirationPlaceholder for the reasoning.
	ErrFailedStateIntervalTooShortForDomainExpirationPlaceholder = errors.New("the minimum failed-state.interval for an endpoint with a condition using the " + DomainExpirationPlaceholder + " placeholder is 300s (5m)")
)

// FailedState is the configuration of how an endpoint is checked while it is in a failed state.
//
// While an endpoint is in a failed state (its last result was a failure, or one of its alerts is still triggered),
// Gatus re-checks it every FailedState.Interval in addition to the regular checks every Endpoint.Interval, in order
// to notice recovery sooner. These rechecks only ever help an endpoint recover:
//   - a successful recheck is handled exactly like a regular successful check (stored, counted toward the alerts'
//     success-threshold, and able to resolve alerts)
//   - a failed recheck is not counted toward failure-threshold, never sends an alert or a reminder, and is not
//     stored, unless it is the first failure following a successful result
type FailedState struct {
	// Interval is the duration to wait between rechecks while the endpoint is in a failed state
	Interval time.Duration `yaml:"interval"`
}

// ValidateAndSetDefaults validates the failed-state configuration of an endpoint whose interval is endpointInterval
func (fs *FailedState) ValidateAndSetDefaults(endpointInterval time.Duration, conditions []Condition) error {
	if fs.Interval <= 0 {
		return ErrFailedStateWithInvalidInterval
	}
	if fs.Interval >= endpointInterval {
		return ErrFailedStateIntervalNotShorterThanInterval
	}
	if fs.Interval < 5*time.Minute {
		for _, c := range conditions {
			if c.hasDomainExpirationPlaceholder() {
				return ErrFailedStateIntervalTooShortForDomainExpirationPlaceholder
			}
		}
	}
	return nil
}
