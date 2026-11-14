package suite

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/TwiN/gatus/v5/config/endpoint"
)

func TestSuite_ValidateAndSetDefaultsWithFailedStateOnEndpoint(t *testing.T) {
	s := &Suite{
		Name: "test-suite",
		Endpoints: []*endpoint.Endpoint{
			{
				Name:       "first",
				URL:        "https://example.org",
				Conditions: []endpoint.Condition{"[STATUS] == 200"},
			},
			{
				Name:        "second",
				URL:         "https://example.org",
				Interval:    time.Hour,
				Conditions:  []endpoint.Condition{"[STATUS] == 200"},
				FailedState: &endpoint.FailedStateConfig{Interval: 5 * time.Minute},
			},
		},
	}
	err := s.ValidateAndSetDefaults()
	if !errors.Is(err, endpoint.ErrFailedStateNotSupportedForSuiteEndpoint) {
		t.Fatalf("expected %v, got %v", endpoint.ErrFailedStateNotSupportedForSuiteEndpoint, err)
	}
	if !strings.Contains(err.Error(), "invalid endpoint 'second'") {
		t.Errorf("expected error to name the endpoint, got %q", err.Error())
	}
}
