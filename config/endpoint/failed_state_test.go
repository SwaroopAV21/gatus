package endpoint

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/TwiN/gatus/v5/config/endpoint/dns"
	sshconfig "github.com/TwiN/gatus/v5/config/endpoint/ssh"
	"gopkg.in/yaml.v3"
)

func TestEndpoint_ValidateAndSetDefaultsWithFailedState(t *testing.T) {
	scenarios := []struct {
		name        string
		endpoint    *Endpoint
		expectedErr error
	}{
		{
			name:        "no-failed-state",
			endpoint:    &Endpoint{Name: "a", URL: "https://example.org", Interval: time.Hour, Conditions: []Condition{"[STATUS] == 200"}},
			expectedErr: nil,
		},
		{
			name:        "valid",
			endpoint:    &Endpoint{Name: "a", URL: "https://example.org", Interval: time.Hour, FailedState: &FailedState{Interval: 2 * time.Minute}, Conditions: []Condition{"[STATUS] == 200"}},
			expectedErr: nil,
		},
		{
			name:        "valid-with-default-interval",
			endpoint:    &Endpoint{Name: "a", URL: "https://example.org", FailedState: &FailedState{Interval: 10 * time.Second}, Conditions: []Condition{"[STATUS] == 200"}},
			expectedErr: nil,
		},
		{
			name:        "empty-block",
			endpoint:    &Endpoint{Name: "a", URL: "https://example.org", Interval: time.Hour, FailedState: &FailedState{}, Conditions: []Condition{"[STATUS] == 200"}},
			expectedErr: ErrFailedStateWithInvalidInterval,
		},
		{
			name:        "negative-interval",
			endpoint:    &Endpoint{Name: "a", URL: "https://example.org", Interval: time.Hour, FailedState: &FailedState{Interval: -time.Minute}, Conditions: []Condition{"[STATUS] == 200"}},
			expectedErr: ErrFailedStateWithInvalidInterval,
		},
		{
			name:        "interval-equal-to-endpoint-interval",
			endpoint:    &Endpoint{Name: "a", URL: "https://example.org", Interval: time.Hour, FailedState: &FailedState{Interval: time.Hour}, Conditions: []Condition{"[STATUS] == 200"}},
			expectedErr: ErrFailedStateIntervalNotShorterThanInterval,
		},
		{
			name:        "interval-longer-than-endpoint-interval",
			endpoint:    &Endpoint{Name: "a", URL: "https://example.org", Interval: time.Minute, FailedState: &FailedState{Interval: 5 * time.Minute}, Conditions: []Condition{"[STATUS] == 200"}},
			expectedErr: ErrFailedStateIntervalNotShorterThanInterval,
		},
		{
			name:        "interval-not-shorter-than-default-endpoint-interval",
			endpoint:    &Endpoint{Name: "a", URL: "https://example.org", FailedState: &FailedState{Interval: time.Minute}, Conditions: []Condition{"[STATUS] == 200"}},
			expectedErr: ErrFailedStateIntervalNotShorterThanInterval,
		},
		{
			name:        "domain-expiration-with-interval-below-5m",
			endpoint:    &Endpoint{Name: "a", URL: "https://example.org", Interval: time.Hour, FailedState: &FailedState{Interval: 4 * time.Minute}, Conditions: []Condition{"[STATUS] == 200", "[DOMAIN_EXPIRATION] > 720h"}},
			expectedErr: ErrFailedStateIntervalTooLowForDomainExpirationPlaceholder,
		},
		{
			name:        "domain-expiration-with-interval-of-5m",
			endpoint:    &Endpoint{Name: "a", URL: "https://example.org", Interval: time.Hour, FailedState: &FailedState{Interval: 5 * time.Minute}, Conditions: []Condition{"[DOMAIN_EXPIRATION] > 720h"}},
			expectedErr: nil,
		},
		{
			// DNS endpoints return early from ValidateAndSetDefaults, so make sure failed-state is validated before that
			name:        "dns-with-invalid-failed-state",
			endpoint:    &Endpoint{Name: "a", URL: "8.8.8.8", Interval: time.Hour, DNSConfig: &dns.Config{QueryType: "A", QueryName: "example.org"}, FailedState: &FailedState{}, Conditions: []Condition{"[DNS_RCODE] == NOERROR"}},
			expectedErr: ErrFailedStateWithInvalidInterval,
		},
		{
			// SSH endpoints return early from ValidateAndSetDefaults, so make sure failed-state is validated before that
			name:        "ssh-with-invalid-failed-state",
			endpoint:    &Endpoint{Name: "a", URL: "ssh://example.org", Interval: time.Minute, SSHConfig: &sshconfig.Config{Username: "u", Password: "p"}, FailedState: &FailedState{Interval: time.Hour}, Conditions: []Condition{"[STATUS] == 0"}},
			expectedErr: ErrFailedStateIntervalNotShorterThanInterval,
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			err := scenario.endpoint.ValidateAndSetDefaults()
			if scenario.expectedErr == nil {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if !errors.Is(err, scenario.expectedErr) {
				t.Fatalf("expected error %v, got %v", scenario.expectedErr, err)
			}
		})
	}
}

func TestEndpoint_UnmarshalYAMLWithFailedState(t *testing.T) {
	scenarios := []struct {
		name                string
		yaml                string
		expectedErr         error
		expectedInterval    time.Duration // the endpoint's interval after validation, if expectedErr is nil
		expectedFailedState *FailedState
	}{
		{
			name: "omitted",
			yaml: `
name: website
url: https://example.org
interval: 1h
conditions: ["[STATUS] == 200"]`,
			expectedInterval: time.Hour,
		},
		{
			name: "configured",
			yaml: `
name: website
url: https://example.org
interval: 1h
failed-state:
  interval: 5m
conditions: ["[STATUS] == 200"]`,
			expectedInterval:    time.Hour,
			expectedFailedState: &FailedState{Interval: 5 * time.Minute},
		},
		{
			name: "bare-key",
			yaml: `
name: website
url: https://example.org
interval: 1h
failed-state:
conditions: ["[STATUS] == 200"]`,
			expectedErr: ErrFailedStateWithNoValue,
		},
		{
			name: "tilde",
			yaml: `
name: website
url: https://example.org
interval: 1h
failed-state: ~
conditions: ["[STATUS] == 200"]`,
			expectedErr: ErrFailedStateWithNoValue,
		},
		{
			name: "explicit-null",
			yaml: `
name: website
url: https://example.org
interval: 1h
failed-state: null
conditions: ["[STATUS] == 200"]`,
			expectedErr: ErrFailedStateWithNoValue,
		},
		{
			// Without rejecting the bare key, this would silently check the endpoint every 5m instead of the default 1m,
			// without any failed-state configuration
			name: "interval-not-indented-under-failed-state",
			yaml: `
name: website
url: https://example.org
failed-state:
interval: 5m
conditions: ["[STATUS] == 200"]`,
			expectedErr: ErrFailedStateWithNoValue,
		},
		{
			name: "null-overriding-merged-value",
			yaml: `
defaults: &defaults
  failed-state:
    interval: 5m
endpoint:
  <<: *defaults
  name: website
  url: https://example.org
  interval: 1h
  failed-state:
  conditions: ["[STATUS] == 200"]`,
			expectedErr: ErrFailedStateWithNoValue,
		},
		{
			name: "null-from-merge-key",
			yaml: `
defaults: &defaults
  failed-state:
endpoint:
  <<: *defaults
  name: website
  url: https://example.org
  interval: 1h
  conditions: ["[STATUS] == 200"]`,
			expectedErr: ErrFailedStateWithNoValue,
		},
		{
			name: "configured-from-merge-key",
			yaml: `
defaults: &defaults
  failed-state:
    interval: 5m
endpoint:
  <<: *defaults
  name: website
  url: https://example.org
  interval: 1h
  conditions: ["[STATUS] == 200"]`,
			expectedInterval:    time.Hour,
			expectedFailedState: &FailedState{Interval: 5 * time.Minute},
		},
		{
			name: "null-through-alias",
			yaml: `
nothing: &nothing ~
endpoint:
  name: website
  url: https://example.org
  interval: 1h
  failed-state: *nothing
  conditions: ["[STATUS] == 200"]`,
			expectedErr: ErrFailedStateWithNoValue,
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			var ep *Endpoint
			var document struct {
				Endpoint *Endpoint `yaml:"endpoint"`
			}
			if err := yaml.Unmarshal([]byte(scenario.yaml), &document); err != nil {
				t.Fatal("failed to unmarshal:", err)
			}
			if document.Endpoint != nil {
				ep = document.Endpoint
			} else if err := yaml.Unmarshal([]byte(scenario.yaml), &ep); err != nil {
				t.Fatal("failed to unmarshal:", err)
			}
			err := ep.ValidateAndSetDefaults()
			if scenario.expectedErr != nil {
				if !errors.Is(err, scenario.expectedErr) {
					t.Fatalf("expected error %v, got %v", scenario.expectedErr, err)
				}
				return
			}
			if err != nil {
				t.Fatal("expected no error, got", err)
			}
			if ep.Interval != scenario.expectedInterval {
				t.Errorf("expected interval %s, got %s", scenario.expectedInterval, ep.Interval)
			}
			if !reflect.DeepEqual(ep.FailedState, scenario.expectedFailedState) {
				t.Errorf("expected failed-state %+v, got %+v", scenario.expectedFailedState, ep.FailedState)
			}
		})
	}
}

// TestEndpoint_UnmarshalYAMLDecodesLikeTheDefaultDecoder makes sure that Endpoint.UnmarshalYAML, which only exists to
// detect a failed-state key with no value, doesn't change how anything else is decoded.
func TestEndpoint_UnmarshalYAMLDecodesLikeTheDefaultDecoder(t *testing.T) {
	document := []byte(`
defaults: &defaults
  interval: 2h
  headers:
    X-Default: "yes"
endpoint:
  <<: *defaults
  enabled: false
  name: website
  group: core
  url: https://example.org/graphql
  method: POST
  body: '{"a": 1}'
  graphql: true
  headers:
    Authorization: Bearer token
  extra-labels:
    env: prod
  failed-state:
    interval: 5m
  conditions:
    - "[STATUS] == 200"
    - "[RESPONSE_TIME] < 500"
  alerts:
    - type: slack
      failure-threshold: 4
      success-threshold: 3
      minimum-reminder-interval: 30m
      send-on-resolved: true
      description: "down"
      provider-override:
        webhook-url: https://hooks.example.org
  maintenance-windows:
    - start: "23:00"
      duration: 1h
      every: [Monday]
  client:
    insecure: true
    timeout: 5s
  ui:
    hide-url: true
  store:
    token: "[BODY].token"
  always-run: true
`)
	type endpointWithoutUnmarshaler Endpoint
	var withUnmarshaler struct {
		Endpoint Endpoint `yaml:"endpoint"`
	}
	var withoutUnmarshaler struct {
		Endpoint endpointWithoutUnmarshaler `yaml:"endpoint"`
	}
	if err := yaml.Unmarshal(document, &withUnmarshaler); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(document, &withoutUnmarshaler); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(withUnmarshaler.Endpoint, Endpoint(withoutUnmarshaler.Endpoint)) {
		t.Errorf("expected both decoders to produce the same endpoint\nwith UnmarshalYAML:    %+v\nwithout UnmarshalYAML: %+v", withUnmarshaler.Endpoint, Endpoint(withoutUnmarshaler.Endpoint))
	}
	if withUnmarshaler.Endpoint.Interval != 2*time.Hour || withUnmarshaler.Endpoint.Headers["Authorization"] != "Bearer token" || withUnmarshaler.Endpoint.FailedState == nil {
		t.Errorf("unexpected decoded endpoint: %+v", withUnmarshaler.Endpoint)
	}
	// Type errors must still be reported
	var invalid struct {
		Endpoint Endpoint `yaml:"endpoint"`
	}
	if err := yaml.Unmarshal([]byte("endpoint:\n  name: [not, a, string]\n"), &invalid); err == nil {
		t.Error("expected a type error, got nil")
	}
}
