package endpoint

import (
	"errors"
	"testing"
	"time"

	"github.com/TwiN/gatus/v5/alerting/alert"
	"github.com/TwiN/gatus/v5/config/endpoint/dns"
)

func TestEndpoint_ValidateAndSetDefaultsWithFailedState(t *testing.T) {
	scenarios := []struct {
		name        string
		interval    time.Duration
		failedState *FailedState
		conditions  []Condition
		expectedErr error
	}{
		{
			name:        "no-failed-state",
			interval:    time.Hour,
			failedState: nil,
			expectedErr: nil,
		},
		{
			name:        "valid",
			interval:    time.Hour,
			failedState: &FailedState{Interval: 2 * time.Minute},
			expectedErr: nil,
		},
		{
			name:        "valid-with-default-endpoint-interval",
			interval:    0, // defaults to 1m
			failedState: &FailedState{Interval: 10 * time.Second},
			expectedErr: nil,
		},
		{
			name:        "missing-interval",
			interval:    time.Hour,
			failedState: &FailedState{},
			expectedErr: ErrFailedStateWithInvalidInterval,
		},
		{
			name:        "negative-interval",
			interval:    time.Hour,
			failedState: &FailedState{Interval: -time.Minute},
			expectedErr: ErrFailedStateWithInvalidInterval,
		},
		{
			name:        "interval-equal-to-endpoint-interval",
			interval:    time.Hour,
			failedState: &FailedState{Interval: time.Hour},
			expectedErr: ErrFailedStateIntervalNotShorterThanInterval,
		},
		{
			name:        "interval-longer-than-endpoint-interval",
			interval:    5 * time.Minute,
			failedState: &FailedState{Interval: 10 * time.Minute},
			expectedErr: ErrFailedStateIntervalNotShorterThanInterval,
		},
		{
			name:        "interval-longer-than-default-endpoint-interval",
			interval:    0, // defaults to 1m
			failedState: &FailedState{Interval: 2 * time.Minute},
			expectedErr: ErrFailedStateIntervalNotShorterThanInterval,
		},
		{
			name:        "domain-expiration-with-interval-under-5m",
			interval:    time.Hour,
			failedState: &FailedState{Interval: 4 * time.Minute},
			conditions:  []Condition{"[STATUS] == 200", "[DOMAIN_EXPIRATION] > 720h"},
			expectedErr: ErrFailedStateIntervalTooShortForDomainExpirationPlaceholder,
		},
		{
			name:        "domain-expiration-with-interval-of-5m",
			interval:    time.Hour,
			failedState: &FailedState{Interval: 5 * time.Minute},
			conditions:  []Condition{"[STATUS] == 200", "[DOMAIN_EXPIRATION] > 720h"},
			expectedErr: nil,
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			conditions := scenario.conditions
			if conditions == nil {
				conditions = []Condition{"[STATUS] == 200"}
			}
			ep := &Endpoint{
				Name:        "website-health",
				URL:         "https://twin.sh/health",
				Interval:    scenario.interval,
				FailedState: scenario.failedState,
				Conditions:  conditions,
			}
			err := ep.ValidateAndSetDefaults()
			if !errors.Is(err, scenario.expectedErr) {
				t.Errorf("expected error %v, got %v", scenario.expectedErr, err)
			}
		})
	}
}

func TestEndpoint_ValidateAndSetDefaultsWithFailedStateOnDNSEndpoint(t *testing.T) {
	// DNS endpoints return early from ValidateAndSetDefaults, so make sure failed-state is still validated for them
	ep := &Endpoint{
		Name:        "dns",
		URL:         "8.8.8.8",
		Interval:    time.Hour,
		FailedState: &FailedState{Interval: 2 * time.Hour},
		DNSConfig:   &dns.Config{QueryType: "A", QueryName: "example.com"},
		Conditions:  []Condition{"[DNS_RCODE] == NOERROR"},
	}
	if err := ep.ValidateAndSetDefaults(); !errors.Is(err, ErrFailedStateIntervalNotShorterThanInterval) {
		t.Errorf("expected error %v, got %v", ErrFailedStateIntervalNotShorterThanInterval, err)
	}
}

func TestEndpoint_HasTriggeredAlert(t *testing.T) {
	enabled, disabled := true, false
	scenarios := []struct {
		name     string
		alerts   []*alert.Alert
		expected bool
	}{
		{name: "no-alerts", alerts: nil, expected: false},
		{name: "triggered-but-disabled", alerts: []*alert.Alert{{Type: alert.TypeCustom, Enabled: &disabled, Triggered: true}}, expected: false},
		{name: "enabled-not-triggered", alerts: []*alert.Alert{{Type: alert.TypeCustom, Enabled: &enabled, Triggered: false}}, expected: false},
		{name: "enabled-by-default-and-triggered", alerts: []*alert.Alert{{Type: alert.TypeCustom, Triggered: true}}, expected: true},
		{
			name: "one-of-several-triggered",
			alerts: []*alert.Alert{
				{Type: alert.TypeCustom, Triggered: false},
				{Type: alert.TypeSlack, Triggered: true},
			},
			expected: true,
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			ep := &Endpoint{Alerts: scenario.alerts}
			if actual := ep.HasTriggeredAlert(); actual != scenario.expected {
				t.Errorf("expected %v, got %v", scenario.expected, actual)
			}
		})
	}
}
