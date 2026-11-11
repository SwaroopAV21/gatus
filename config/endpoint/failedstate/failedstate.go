package failedstate

import "time"

// Config is the configuration for checking an endpoint more frequently while it is failing, so that recovery is
// noticed sooner than the endpoint's regular interval would allow.
//
// The validation of this configuration lives in the endpoint package, because it depends on the endpoint's own
// configuration (interval and conditions).
type Config struct {
	// Interval is the duration to wait between recovery checks while the endpoint is failing.
	// Must be greater than 0 and lower than the endpoint's interval.
	Interval time.Duration `yaml:"interval"`
}
