package endpoint

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrFailedStateWithInvalidInterval is the error with which Gatus will panic if an endpoint has a failed-state
	// block whose interval is missing, zero or negative.
	ErrFailedStateWithInvalidInterval = errors.New("failed-state.interval must be set to a duration greater than 0 (e.g. 5m)")

	// ErrFailedStateIntervalNotLowerThanInterval is the error with which Gatus will panic if an endpoint's
	// failed-state.interval is not lower than the endpoint's interval, in which case it would never do anything.
	ErrFailedStateIntervalNotLowerThanInterval = errors.New("failed-state.interval must be lower than the endpoint's interval")

	// ErrFailedStateIntervalTooLowForDomainExpirationPlaceholder is the error with which Gatus will panic if an
	// endpoint has a condition using the DomainExpirationPlaceholder and a failed-state.interval lower than 5 minutes.
	// Failed-state checks evaluate every condition, including the WHOIS lookup, so the same minimum as for the
	// endpoint's interval applies (see ErrInvalidEndpointIntervalForDomainExpirationPlaceholder).
	ErrFailedStateIntervalTooLowForDomainExpirationPlaceholder = errors.New("the minimum failed-state.interval for an endpoint with a condition using the " + DomainExpirationPlaceholder + " placeholder is 300s (5m)")

	// ErrFailedStateNotSupportedForExternalEndpoint is the error with which Gatus will panic if an external endpoint
	// has a failed-state block. External endpoints are pushed to Gatus rather than checked by it, so there is
	// nothing to check more often.
	ErrFailedStateNotSupportedForExternalEndpoint = errors.New("failed-state is not supported for external endpoints, because Gatus does not check them itself; results are pushed to Gatus")

	// ErrFailedStateNotSupportedForSuiteEndpoint is the error with which Gatus will panic if an endpoint that is part
	// of a suite has a failed-state block. Suite endpoints are executed by their suite on the suite's interval.
	ErrFailedStateNotSupportedForSuiteEndpoint = errors.New("failed-state is not supported for endpoints that are part of a suite, because they are executed on the suite's interval")
)

// FailedStateConfig configures how an endpoint is checked while it is failing.
//
// While the last stored result of the endpoint is a failure, or while one of its alerts is still triggered, Gatus
// runs additional checks every Interval on top of the endpoint's regular checks, so that a recovery is noticed
// (and alerts are resolved) sooner. These additional checks never count toward an alert's failure-threshold and
// never trigger an alert or a reminder. See the README for the details.
type FailedStateConfig struct {
	// Interval is the duration to wait between additional checks while the endpoint is in a failed state.
	// Must be greater than 0 and lower than the endpoint's interval.
	Interval time.Duration `yaml:"interval"`
}

// validate validates the failed-state configuration against the configuration of the endpoint it belongs to.
// It must be called after the endpoint's interval has been defaulted.
func (c *FailedStateConfig) validate(endpointInterval time.Duration, usesDomainExpirationPlaceholder bool) error {
	if c.Interval <= 0 {
		return fmt.Errorf("%w, got %s", ErrFailedStateWithInvalidInterval, c.Interval)
	}
	if c.Interval >= endpointInterval {
		return fmt.Errorf("%w: failed-state.interval is %s and interval is %s", ErrFailedStateIntervalNotLowerThanInterval, c.Interval, endpointInterval)
	}
	if usesDomainExpirationPlaceholder && c.Interval < 5*time.Minute {
		return fmt.Errorf("%w, got %s", ErrFailedStateIntervalTooLowForDomainExpirationPlaceholder, c.Interval)
	}
	return nil
}
