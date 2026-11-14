package endpoint

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/TwiN/gatus/v5/config/endpoint/dns"
	"github.com/TwiN/gatus/v5/config/endpoint/ssh"
)

func TestEndpoint_ValidateAndSetDefaultsWithFailedState(t *testing.T) {
	scenarios := []struct {
		name             string
		interval         time.Duration
		failedState      *FailedStateConfig
		conditions       []Condition
		expectedErr      error
		expectedErrParts []string
	}{
		{
			name:        "no-failed-state",
			interval:    time.Hour,
			failedState: nil,
		},
		{
			name:        "valid",
			interval:    time.Hour,
			failedState: &FailedStateConfig{Interval: 5 * time.Minute},
		},
		{
			name:        "valid-just-below-interval",
			interval:    time.Hour,
			failedState: &FailedStateConfig{Interval: 59 * time.Minute},
		},
		{
			name:        "valid-with-default-interval",
			interval:    0, // defaults to 1m
			failedState: &FailedStateConfig{Interval: 30 * time.Second},
		},
		{
			name:             "missing-interval",
			interval:         time.Hour,
			failedState:      &FailedStateConfig{},
			expectedErr:      ErrFailedStateWithInvalidInterval,
			expectedErrParts: []string{"failed-state.interval must be set", "got 0s"},
		},
		{
			name:             "negative-interval",
			interval:         time.Hour,
			failedState:      &FailedStateConfig{Interval: -5 * time.Minute},
			expectedErr:      ErrFailedStateWithInvalidInterval,
			expectedErrParts: []string{"got -5m0s"},
		},
		{
			name:             "equal-to-interval",
			interval:         time.Hour,
			failedState:      &FailedStateConfig{Interval: time.Hour},
			expectedErr:      ErrFailedStateIntervalNotLowerThanInterval,
			expectedErrParts: []string{"must be lower than the endpoint's interval", "failed-state.interval is 1h0m0s and interval is 1h0m0s"},
		},
		{
			name:             "greater-than-interval",
			interval:         5 * time.Minute,
			failedState:      &FailedStateConfig{Interval: 10 * time.Minute},
			expectedErr:      ErrFailedStateIntervalNotLowerThanInterval,
			expectedErrParts: []string{"failed-state.interval is 10m0s and interval is 5m0s"},
		},
		{
			name:             "greater-than-default-interval",
			interval:         0, // defaults to 1m
			failedState:      &FailedStateConfig{Interval: 2 * time.Minute},
			expectedErr:      ErrFailedStateIntervalNotLowerThanInterval,
			expectedErrParts: []string{"failed-state.interval is 2m0s and interval is 1m0s"},
		},
		{
			name:        "domain-expiration-with-5m",
			interval:    time.Hour,
			failedState: &FailedStateConfig{Interval: 5 * time.Minute},
			conditions:  []Condition{"[STATUS] == 200", "[DOMAIN_EXPIRATION] > 720h"},
		},
		{
			name:             "domain-expiration-below-5m",
			interval:         time.Hour,
			failedState:      &FailedStateConfig{Interval: 4 * time.Minute},
			conditions:       []Condition{"[STATUS] == 200", "[DOMAIN_EXPIRATION] > 720h"},
			expectedErr:      ErrFailedStateIntervalTooLowForDomainExpirationPlaceholder,
			expectedErrParts: []string{"[DOMAIN_EXPIRATION]", "300s (5m)", "got 4m0s"},
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			conditions := scenario.conditions
			if conditions == nil {
				conditions = []Condition{"[STATUS] == 200"}
			}
			ep := &Endpoint{
				Name:        "test",
				URL:         "https://example.org",
				Interval:    scenario.interval,
				Conditions:  conditions,
				FailedState: scenario.failedState,
			}
			err := ep.ValidateAndSetDefaults()
			if scenario.expectedErr == nil {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if !errors.Is(err, scenario.expectedErr) {
				t.Fatalf("expected error %v, got %v", scenario.expectedErr, err)
			}
			for _, part := range scenario.expectedErrParts {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("expected error %q to contain %q", err.Error(), part)
				}
			}
		})
	}
}

func TestEndpoint_ValidateAndSetDefaultsWithFailedStateOnDNSAndSSHEndpoints(t *testing.T) {
	// ValidateAndSetDefaults returns early for DNS and SSH endpoints, so failed-state must be validated before that.
	newDNSEndpoint := func(failedState *FailedStateConfig) *Endpoint {
		return &Endpoint{
			Name:        "dns",
			URL:         "8.8.8.8",
			Interval:    time.Hour,
			Conditions:  []Condition{"[DNS_RCODE] == NOERROR"},
			DNSConfig:   &dns.Config{QueryType: "A", QueryName: "example.com"},
			FailedState: failedState,
		}
	}
	newSSHEndpoint := func(failedState *FailedStateConfig) *Endpoint {
		return &Endpoint{
			Name:        "ssh",
			URL:         "ssh://example.com:22",
			Interval:    time.Hour,
			Conditions:  []Condition{"[CONNECTED] == true"},
			SSHConfig:   &ssh.Config{Username: "user", Password: "pass"},
			FailedState: failedState,
		}
	}
	for name, newEndpoint := range map[string]func(*FailedStateConfig) *Endpoint{"dns": newDNSEndpoint, "ssh": newSSHEndpoint} {
		t.Run(name, func(t *testing.T) {
			if err := newEndpoint(&FailedStateConfig{Interval: 5 * time.Minute}).ValidateAndSetDefaults(); err != nil {
				t.Errorf("expected no error for a valid failed-state, got %v", err)
			}
			if err := newEndpoint(&FailedStateConfig{Interval: 2 * time.Hour}).ValidateAndSetDefaults(); !errors.Is(err, ErrFailedStateIntervalNotLowerThanInterval) {
				t.Errorf("expected %v, got %v", ErrFailedStateIntervalNotLowerThanInterval, err)
			}
			if err := newEndpoint(&FailedStateConfig{}).ValidateAndSetDefaults(); !errors.Is(err, ErrFailedStateWithInvalidInterval) {
				t.Errorf("expected %v, got %v", ErrFailedStateWithInvalidInterval, err)
			}
		})
	}
}

func TestExternalEndpoint_ValidateAndSetDefaultsWithFailedState(t *testing.T) {
	ee := &ExternalEndpoint{
		Name:        "external",
		Token:       "secret",
		FailedState: &FailedStateConfig{Interval: 5 * time.Minute},
	}
	err := ee.ValidateAndSetDefaults()
	if !errors.Is(err, ErrFailedStateNotSupportedForExternalEndpoint) {
		t.Fatalf("expected %v, got %v", ErrFailedStateNotSupportedForExternalEndpoint, err)
	}
	// Even an empty block is rejected, since it can't mean anything for an external endpoint
	ee.FailedState = &FailedStateConfig{}
	if err := ee.ValidateAndSetDefaults(); !errors.Is(err, ErrFailedStateNotSupportedForExternalEndpoint) {
		t.Fatalf("expected %v, got %v", ErrFailedStateNotSupportedForExternalEndpoint, err)
	}
	ee.FailedState = nil
	if err := ee.ValidateAndSetDefaults(); err != nil {
		t.Fatalf("expected no error without failed-state, got %v", err)
	}
}
