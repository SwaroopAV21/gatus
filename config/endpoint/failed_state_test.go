package endpoint

import (
	"errors"
	"testing"
	"time"

	"github.com/TwiN/gatus/v5/config/endpoint/dns"
	"github.com/TwiN/gatus/v5/config/endpoint/failedstate"
)

func TestEndpoint_ValidateAndSetDefaultsWithFailedState(t *testing.T) {
	scenarios := []struct {
		name        string
		endpoint    *Endpoint
		expectedErr error
	}{
		{
			name: "no-failed-state",
			endpoint: &Endpoint{
				Name:       "no-failed-state",
				URL:        "https://example.com",
				Interval:   time.Hour,
				Conditions: []Condition{"[STATUS] == 200"},
			},
			expectedErr: nil,
		},
		{
			name: "valid-failed-state",
			endpoint: &Endpoint{
				Name:        "valid-failed-state",
				URL:         "https://example.com",
				Interval:    time.Hour,
				FailedState: &failedstate.Config{Interval: 5 * time.Minute},
				Conditions:  []Condition{"[STATUS] == 200"},
			},
			expectedErr: nil,
		},
		{
			name: "very-short-failed-state-interval-is-allowed",
			endpoint: &Endpoint{
				Name:        "very-short-failed-state-interval-is-allowed",
				URL:         "https://example.com",
				Interval:    time.Hour,
				FailedState: &failedstate.Config{Interval: time.Millisecond},
				Conditions:  []Condition{"[STATUS] == 200"},
			},
			expectedErr: nil,
		},
		{
			name: "failed-state-without-interval",
			endpoint: &Endpoint{
				Name:        "failed-state-without-interval",
				URL:         "https://example.com",
				Interval:    time.Hour,
				FailedState: &failedstate.Config{},
				Conditions:  []Condition{"[STATUS] == 200"},
			},
			expectedErr: ErrInvalidFailedStateInterval,
		},
		{
			name: "failed-state-with-negative-interval",
			endpoint: &Endpoint{
				Name:        "failed-state-with-negative-interval",
				URL:         "https://example.com",
				Interval:    time.Hour,
				FailedState: &failedstate.Config{Interval: -time.Minute},
				Conditions:  []Condition{"[STATUS] == 200"},
			},
			expectedErr: ErrInvalidFailedStateInterval,
		},
		{
			name: "failed-state-interval-equal-to-interval",
			endpoint: &Endpoint{
				Name:        "failed-state-interval-equal-to-interval",
				URL:         "https://example.com",
				Interval:    10 * time.Minute,
				FailedState: &failedstate.Config{Interval: 10 * time.Minute},
				Conditions:  []Condition{"[STATUS] == 200"},
			},
			expectedErr: ErrFailedStateIntervalNotLowerThanInterval,
		},
		{
			name: "failed-state-interval-greater-than-interval",
			endpoint: &Endpoint{
				Name:        "failed-state-interval-greater-than-interval",
				URL:         "https://example.com",
				Interval:    10 * time.Minute,
				FailedState: &failedstate.Config{Interval: time.Hour},
				Conditions:  []Condition{"[STATUS] == 200"},
			},
			expectedErr: ErrFailedStateIntervalNotLowerThanInterval,
		},
		{
			name: "failed-state-interval-compared-to-default-interval",
			endpoint: &Endpoint{
				Name:        "failed-state-interval-compared-to-default-interval",
				URL:         "https://example.com",
				FailedState: &failedstate.Config{Interval: 2 * time.Minute}, // default interval is 1m
				Conditions:  []Condition{"[STATUS] == 200"},
			},
			expectedErr: ErrFailedStateIntervalNotLowerThanInterval,
		},
		{
			name: "failed-state-interval-too-low-for-domain-expiration",
			endpoint: &Endpoint{
				Name:        "failed-state-interval-too-low-for-domain-expiration",
				URL:         "https://example.com",
				Interval:    time.Hour,
				FailedState: &failedstate.Config{Interval: 4 * time.Minute},
				Conditions:  []Condition{"[STATUS] == 200", "[DOMAIN_EXPIRATION] > 720h"},
			},
			expectedErr: ErrInvalidFailedStateIntervalForDomainExpirationPlaceholder,
		},
		{
			name: "failed-state-interval-of-5m-with-domain-expiration",
			endpoint: &Endpoint{
				Name:        "failed-state-interval-of-5m-with-domain-expiration",
				URL:         "https://example.com",
				Interval:    time.Hour,
				FailedState: &failedstate.Config{Interval: 5 * time.Minute},
				Conditions:  []Condition{"[DOMAIN_EXPIRATION] > 720h"},
			},
			expectedErr: nil,
		},
		{
			// DNS endpoints return early from ValidateAndSetDefaults, so make sure failed-state is validated before that
			name: "dns-endpoint-with-invalid-failed-state",
			endpoint: &Endpoint{
				Name:        "dns-endpoint-with-invalid-failed-state",
				URL:         "8.8.8.8",
				Interval:    time.Minute,
				FailedState: &failedstate.Config{Interval: time.Hour},
				DNSConfig:   &dns.Config{QueryType: "A", QueryName: "example.com"},
				Conditions:  []Condition{"[DNS_RCODE] == NOERROR"},
			},
			expectedErr: ErrFailedStateIntervalNotLowerThanInterval,
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			err := scenario.endpoint.ValidateAndSetDefaults()
			if !errors.Is(err, scenario.expectedErr) {
				t.Fatalf("expected error %v, got %v", scenario.expectedErr, err)
			}
		})
	}
}

func TestEndpoint_ValidateAndSetDefaultsWithFailedStateErrorIncludesValues(t *testing.T) {
	ep := &Endpoint{
		Name:        "name",
		URL:         "https://example.com",
		Interval:    10 * time.Minute,
		FailedState: &failedstate.Config{Interval: 15 * time.Minute},
		Conditions:  []Condition{"[STATUS] == 200"},
	}
	err := ep.ValidateAndSetDefaults()
	if err == nil {
		t.Fatal("expected an error")
	}
	expected := "failed-state.interval must be lower than the endpoint's interval (failed-state.interval=15m0s, interval=10m0s)"
	if err.Error() != expected {
		t.Errorf("expected error message %q, got %q", expected, err.Error())
	}
}

func TestExternalEndpoint_ValidateAndSetDefaultsWithFailedState(t *testing.T) {
	externalEndpoint := &ExternalEndpoint{
		Name:        "external",
		Token:       "token",
		FailedState: &failedstate.Config{Interval: time.Minute},
	}
	if err := externalEndpoint.ValidateAndSetDefaults(); !errors.Is(err, ErrExternalEndpointWithFailedState) {
		t.Errorf("expected error %v, got %v", ErrExternalEndpointWithFailedState, err)
	}
	// An empty failed-state block must also be rejected rather than ignored
	externalEndpoint.FailedState = &failedstate.Config{}
	if err := externalEndpoint.ValidateAndSetDefaults(); !errors.Is(err, ErrExternalEndpointWithFailedState) {
		t.Errorf("expected error %v, got %v", ErrExternalEndpointWithFailedState, err)
	}
}
