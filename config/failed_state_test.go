package config

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/TwiN/gatus/v5/config/endpoint"
	"github.com/TwiN/gatus/v5/config/suite"
)

func TestParseAndValidateConfigBytesWithFailedState(t *testing.T) {
	config, err := parseAndValidateConfigBytes([]byte(`
endpoints:
  - name: expensive
    url: https://example.org/health
    interval: 1h
    failed-state:
      interval: 5m
    conditions:
      - "[STATUS] == 200"
  - name: regular
    url: https://example.org/health
    interval: 1h
    conditions:
      - "[STATUS] == 200"
`))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if config.Endpoints[0].FailedState == nil || config.Endpoints[0].FailedState.Interval != 5*time.Minute {
		t.Errorf("expected failed-state.interval to be 5m, got %+v", config.Endpoints[0].FailedState)
	}
	if config.Endpoints[1].FailedState != nil {
		t.Errorf("expected endpoint without failed-state to have a nil FailedState, got %+v", config.Endpoints[1].FailedState)
	}
}

func TestParseAndValidateConfigBytesWithInvalidFailedState(t *testing.T) {
	scenarios := []struct {
		name                 string
		yaml                 string
		expectedErr          error
		expectedErrToContain string
	}{
		{
			name: "empty-failed-state-block",
			yaml: `
endpoints:
  - name: ep
    url: https://example.org/health
    interval: 1h
    failed-state: {}
    conditions:
      - "[STATUS] == 200"
`,
			expectedErr:          endpoint.ErrInvalidFailedStateInterval,
			expectedErrToContain: "invalid endpoint _ep:",
		},
		{
			name: "failed-state-interval-not-lower-than-interval",
			yaml: `
endpoints:
  - name: ep
    url: https://example.org/health
    interval: 5m
    failed-state:
      interval: 5m
    conditions:
      - "[STATUS] == 200"
`,
			expectedErr:          endpoint.ErrFailedStateIntervalNotLowerThanInterval,
			expectedErrToContain: "(failed-state.interval=5m0s, interval=5m0s)",
		},
		{
			name: "failed-state-interval-too-low-for-domain-expiration",
			yaml: `
endpoints:
  - name: ep
    url: https://example.org/health
    interval: 24h
    failed-state:
      interval: 1m
    conditions:
      - "[DOMAIN_EXPIRATION] > 720h"
`,
			expectedErr: endpoint.ErrInvalidFailedStateIntervalForDomainExpirationPlaceholder,
		},
		{
			name: "failed-state-on-external-endpoint",
			yaml: `
endpoints:
  - name: ep
    url: https://example.org/health
    conditions:
      - "[STATUS] == 200"
external-endpoints:
  - name: ext
    token: potato
    failed-state:
      interval: 1m
`,
			expectedErr:          endpoint.ErrExternalEndpointWithFailedState,
			expectedErrToContain: "invalid external endpoint _ext:",
		},
		{
			name: "failed-state-on-suite-endpoint",
			yaml: `
suites:
  - name: suite
    endpoints:
      - name: step
        url: https://example.org/health
        interval: 1h
        failed-state:
          interval: 1m
        conditions:
          - "[STATUS] == 200"
`,
			expectedErr:          suite.ErrSuiteEndpointWithFailedState,
			expectedErrToContain: "invalid endpoint 'step'",
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			_, err := parseAndValidateConfigBytes([]byte(scenario.yaml))
			if !errors.Is(err, scenario.expectedErr) {
				t.Fatalf("expected error %v, got %v", scenario.expectedErr, err)
			}
			if !strings.Contains(err.Error(), scenario.expectedErrToContain) {
				t.Errorf("expected error %q to contain %q", err.Error(), scenario.expectedErrToContain)
			}
		})
	}
}
