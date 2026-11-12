package endpoint

import (
	"errors"
	"time"

	"gopkg.in/yaml.v3"
)

var (
	// ErrFailedStateWithInvalidInterval is the error with which Gatus will panic if an endpoint has a failed-state
	// block without a positive interval
	ErrFailedStateWithInvalidInterval = errors.New("failed-state.interval must be set to a duration greater than 0")

	// ErrFailedStateWithNoValue is the error with which Gatus will panic if an endpoint has a failed-state key with no
	// value (e.g. "failed-state:" followed by nothing, or by keys that aren't indented under it)
	ErrFailedStateWithNoValue = errors.New("failed-state is present but has no value: either remove it or set failed-state.interval (make sure it is indented under failed-state)")

	// ErrFailedStateIntervalNotShorterThanInterval is the error with which Gatus will panic if an endpoint's
	// failed-state.interval is not shorter than the endpoint's interval, since it would have no effect
	ErrFailedStateIntervalNotShorterThanInterval = errors.New("failed-state.interval must be shorter than the endpoint's interval")

	// ErrFailedStateIntervalTooLowForDomainExpirationPlaceholder is the error with which Gatus will panic if an
	// endpoint with a condition using the DomainExpirationPlaceholder has a failed-state.interval below 5 minutes.
	// Rechecks query the domain expiration just like regular checks do, so the same minimum applies.
	ErrFailedStateIntervalTooLowForDomainExpirationPlaceholder = errors.New("the minimum failed-state.interval for an endpoint with a condition using the " + DomainExpirationPlaceholder + " placeholder is 300s (5m)")

	// ErrFailedStateNotSupportedForSuiteEndpoints is the error with which Gatus will panic if an endpoint that is part
	// of a suite has a failed-state block. Suite endpoints are executed as part of their suite, not on their own.
	ErrFailedStateNotSupportedForSuiteEndpoints = errors.New("failed-state is not supported on suite endpoints, because they are executed as part of their suite")

	// ErrFailedStateNotSupportedForExternalEndpoints is the error with which Gatus will panic if an external endpoint
	// has a failed-state block. External endpoints are pushed to Gatus, so Gatus never rechecks them.
	ErrFailedStateNotSupportedForExternalEndpoints = errors.New("failed-state is not supported on external endpoints, because their results are pushed to Gatus rather than checked by it")
)

// FailedState is the configuration that only applies while an endpoint is failing.
//
// While an endpoint is failing (its last stored result was a failure, or one of its alerts is still triggered), Gatus
// rechecks it every FailedState.Interval in addition to the regular checks, so that a recovery is noticed sooner.
// Rechecks exist only to notice recovery:
//   - A failed recheck is discarded: it isn't stored, doesn't count toward any alert's failure-threshold and never
//     triggers an alert or sends a reminder.
//   - A successful recheck is handled exactly like a successful regular check: it is stored, counts toward
//     success-threshold and resolves alerts as usual.
//   - A failed recheck that follows a successful one while an alert is still triggered (i.e. the endpoint started
//     recovering, then failed again) is stored so that the UI reflects reality, but it only resets the success streak;
//     it still does not count toward failure-threshold and does not go through the triggering path.
type FailedState struct {
	// Interval is the duration to wait between rechecks while the endpoint is failing
	Interval time.Duration `yaml:"interval"`
}

// ValidateAndSetDefaults validates the failed-state configuration against the endpoint it belongs to
func (fs *FailedState) ValidateAndSetDefaults(ep *Endpoint) error {
	if fs.Interval <= 0 {
		return ErrFailedStateWithInvalidInterval
	}
	if fs.Interval >= ep.Interval {
		return ErrFailedStateIntervalNotShorterThanInterval
	}
	if fs.Interval < 5*time.Minute {
		for _, c := range ep.Conditions {
			if c.hasDomainExpirationPlaceholder() {
				return ErrFailedStateIntervalTooLowForDomainExpirationPlaceholder
			}
		}
	}
	return nil
}

// failedStateProbe is used to find out whether the failed-state key is present in an endpoint's YAML mapping.
// Unlike a pointer field, a yaml.Node field is populated even when the value is null.
type failedStateProbe struct {
	FailedState yaml.Node `yaml:"failed-state"`
}

// isFailedStateSetToNull returns whether the given endpoint YAML mapping has a failed-state key with a null value,
// such as a bare "failed-state:".
//
// The YAML decoder sets pointer fields whose value is null to nil without calling any unmarshaler, so without this, a
// bare failed-state key would be indistinguishable from an omitted one, and would silently be ignored. That's
// particularly dangerous if the keys meant to be under failed-state aren't indented, e.g. a failed-state "interval"
// would silently become the endpoint's interval instead.
//
// Anchors and merge keys are resolved by the decoder, so they're taken into account.
func isFailedStateSetToNull(node *yaml.Node) (bool, error) {
	var probe failedStateProbe
	if err := node.Decode(&probe); err != nil {
		return false, err
	}
	value := &probe.FailedState
	for value.Kind == yaml.AliasNode && value.Alias != nil {
		value = value.Alias
	}
	return value.Kind == yaml.ScalarNode && value.ShortTag() == "!!null", nil
}
