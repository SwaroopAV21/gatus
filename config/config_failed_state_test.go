package config

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/TwiN/gatus/v5/config/endpoint"
)

func TestParseAndValidateConfigBytesWithFailedState(t *testing.T) {
	cfg, err := parseAndValidateConfigBytes([]byte(`
endpoints:
  - name: hourly
    url: https://example.org
    interval: 1h
    failed-state:
      interval: 5m
    conditions:
      - "[STATUS] == 200"
  - name: without-failed-state
    url: https://example.org
    interval: 1h
    conditions:
      - "[STATUS] == 200"
`))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cfg.Endpoints[0].FailedState == nil || cfg.Endpoints[0].FailedState.Interval != 5*time.Minute {
		t.Errorf("expected failed-state.interval to be 5m, got %+v", cfg.Endpoints[0].FailedState)
	}
	if cfg.Endpoints[0].Interval != time.Hour {
		t.Errorf("expected interval to be 1h, got %s", cfg.Endpoints[0].Interval)
	}
	if cfg.Endpoints[1].FailedState != nil {
		t.Errorf("expected failed-state to be nil when not configured, got %+v", cfg.Endpoints[1].FailedState)
	}
}

func TestParseAndValidateConfigBytesWithInvalidFailedState(t *testing.T) {
	scenarios := []struct {
		name             string
		yaml             string
		expectedErr      error
		expectedErrParts []string
	}{
		{
			name: "empty-block",
			yaml: `
endpoints:
  - name: website
    url: https://example.org
    interval: 1h
    failed-state: {}
    conditions:
      - "[STATUS] == 200"`,
			expectedErr:      endpoint.ErrFailedStateWithInvalidInterval,
			expectedErrParts: []string{"invalid endpoint _website:", "failed-state.interval must be set"},
		},
		{
			name: "not-lower-than-interval",
			yaml: `
endpoints:
  - name: website
    url: https://example.org
    interval: 10m
    failed-state:
      interval: 10m
    conditions:
      - "[STATUS] == 200"`,
			expectedErr:      endpoint.ErrFailedStateIntervalNotLowerThanInterval,
			expectedErrParts: []string{"invalid endpoint _website:", "failed-state.interval is 10m0s and interval is 10m0s"},
		},
		{
			name: "not-lower-than-default-interval",
			yaml: `
endpoints:
  - name: website
    url: https://example.org
    failed-state:
      interval: 5m
    conditions:
      - "[STATUS] == 200"`,
			expectedErr:      endpoint.ErrFailedStateIntervalNotLowerThanInterval,
			expectedErrParts: []string{"failed-state.interval is 5m0s and interval is 1m0s"},
		},
		{
			name: "domain-expiration",
			yaml: `
endpoints:
  - name: website
    url: https://example.org
    interval: 1h
    failed-state:
      interval: 1m
    conditions:
      - "[DOMAIN_EXPIRATION] > 720h"`,
			expectedErr:      endpoint.ErrFailedStateIntervalTooLowForDomainExpirationPlaceholder,
			expectedErrParts: []string{"invalid endpoint _website:", "300s (5m)"},
		},
		{
			name: "external-endpoint",
			yaml: `
endpoints:
  - name: website
    url: https://example.org
    conditions:
      - "[STATUS] == 200"
external-endpoints:
  - name: pushed
    token: secret
    failed-state:
      interval: 5m`,
			expectedErr:      endpoint.ErrFailedStateNotSupportedForExternalEndpoint,
			expectedErrParts: []string{"invalid external endpoint _pushed:", "not supported for external endpoints"},
		},
		{
			name: "suite-endpoint",
			yaml: `
suites:
  - name: flow
    endpoints:
      - name: step
        url: https://example.org
        interval: 1h
        failed-state:
          interval: 5m
        conditions:
          - "[STATUS] == 200"`,
			expectedErr:      endpoint.ErrFailedStateNotSupportedForSuiteEndpoint,
			expectedErrParts: []string{"invalid endpoint 'step'", "not supported for endpoints that are part of a suite"},
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			_, err := parseAndValidateConfigBytes([]byte(scenario.yaml))
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
