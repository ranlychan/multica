package main

import (
	"net/http"
	"testing"
)

func TestWorkflowControlRoutesAreDedicatedAndFailClosed(t *testing.T) {
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"GET", "/api/daemon/workflows/revisions", 503},
		{"POST", "/api/daemon/workflows/rotate", 503},
		{"POST", "/api/daemon/workflows/ack", 503},
		{"POST", "/api/daemon/workflows/prepare", 503},
		{"POST", "/api/daemon/workflows/retire", 503},
		{"POST", "/api/daemon/workflows/revoke", 503},
		{"POST", "/api/daemon/workflows/validation-failed", 503},
		{"POST", "/api/workflow-runs/10000000-0000-0000-0000-000000000001/actions", 503},
		{"GET", "/api/workspaces", 401},
	} {
		r, err := http.NewRequest(tc.method, testServer.URL+tc.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer wdc_unknown")
		response, err := testServer.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != tc.status {
			t.Fatalf("%s %s: status=%d want=%d", tc.method, tc.path, response.StatusCode, tc.status)
		}
	}
}
