package endpoint

import (
	"testing"

	"gopkg.in/yaml.v3"
)

// expectedEndpointWithoutFailedState is what the original code, before failed-state was added (commit d17f919),
// produces for the endpoint below: the YAML serialization of the Endpoint after ValidateAndSetDefaults. It contains
// every configurable field with its default (method, headers, interval, alert thresholds, client, UI) and no
// failed-state block.
const expectedEndpointWithoutFailedState = `name: website
url: https://example.org
method: GET
headers:
    User-Agent: Gatus/1.0
interval: 1m0s
conditions:
    - '[STATUS] == 200'
alerts:
    - type: custom
      failure-threshold: 3
      success-threshold: 2
client:
    timeout: 10s
    network: ip
ui:
    hide-conditions: false
    hide-hostname: false
    hide-url: false
    hide-port: false
    hide-errors: false
    dont-resolve-failed-conditions: false
    resolve-successful-conditions: false
    badge:
        response-time:
            thresholds:
                - 50
                - 200
                - 300
                - 500
                - 750
`

// TestEndpoint_WithoutFailedStateLoadsWithTheSameDefaultsAsBefore checks that an endpoint without a failed-state block
// loads exactly as it did before failed-state existed, not only without an error: the whole endpoint, after YAML
// decoding and ValidateAndSetDefaults, must serialize to what the original code produced. This file doesn't refer to
// anything added for failed-state, so the same test also compiles and passes against the original code.
func TestEndpoint_WithoutFailedStateLoadsWithTheSameDefaultsAsBefore(t *testing.T) {
	var ep Endpoint
	if err := yaml.Unmarshal([]byte(`
name: website
url: https://example.org
conditions:
  - "[STATUS] == 200"
alerts:
  - type: custom
`), &ep); err != nil {
		t.Fatal(err)
	}
	if err := ep.ValidateAndSetDefaults(); err != nil {
		t.Fatal(err)
	}
	actual, err := yaml.Marshal(&ep)
	if err != nil {
		t.Fatal(err)
	}
	if string(actual) != expectedEndpointWithoutFailedState {
		t.Errorf("endpoint without failed-state doesn't load as before\nexpected:\n%s\nactual:\n%s", expectedEndpointWithoutFailedState, actual)
	}
	if ep.NumberOfFailuresInARow != 0 || ep.NumberOfSuccessesInARow != 0 || !ep.LastReminderSent.IsZero() {
		t.Errorf("expected runtime counters to start at zero, got failures=%d successes=%d lastReminderSent=%s", ep.NumberOfFailuresInARow, ep.NumberOfSuccessesInARow, ep.LastReminderSent)
	}
}
