package hatchet

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testTenant = "12345678-1234-4234-8234-123456789abc"
const testRun = "23456789-1234-4234-8234-123456789abc"

func TestHatchetClientTenantBoundaryEnvelopeAndNoRetry(t *testing.T) {
	calls := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer tenant-secret" || r.URL.Path != "/api/v1/stable/tenants/"+testTenant+"/workflow-runs/trigger" {
			t.Errorf("wrong boundary %s %s", r.URL.Path, r.Header.Get("Authorization"))
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		input := body["input"].(map[string]any)
		if input["multica_run_id"] != testRun || input["input"].(map[string]any)["multica_run_id"] != "user-value" {
			t.Errorf("wrong envelope: %#v", body)
		}
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`tenant-secret private diagnostics`))
	}))
	defer s.Close()
	c, err := NewClient(s.URL, s.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Submit(context.Background(), TenantCredentials{TenantID: testTenant, Token: "tenant-secret"}, SubmitRequest{RunID: testRun, WorkflowName: "mc-revision-1", Input: json.RawMessage(`{"multica_run_id":"user-value"}`)})
	if err == nil || strings.Contains(err.Error(), "tenant-secret") || calls != 1 {
		t.Fatalf("submit error/retry: %v calls=%d", err, calls)
	}
	if !errors.Is(err, ErrAmbiguousSubmission) {
		t.Fatalf("lost ambiguity: %v", err)
	}
	_, err = c.Status(context.Background(), TenantCredentials{TenantID: "../other", Token: "tenant-secret"}, testRun)
	if err == nil || calls != 1 {
		t.Fatal("invalid tenant made request")
	}
}

func TestHatchetClientRoutesAndRetryWindow(t *testing.T) {
	paths := []string{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.RequestURI())
		if strings.HasSuffix(r.URL.Path, "/status") {
			_, _ = w.Write([]byte(`"RUNNING"`))
			return
		}
		_, _ = w.Write([]byte(`{"rows":[]}`))
	}))
	defer s.Close()
	c, _ := NewClient(s.URL, s.Client())
	cred := TenantCredentials{TenantID: testTenant, Token: "worker-token"}
	ctx := context.Background()
	if _, err := c.Detail(ctx, cred, testRun); err != nil {
		t.Fatal(err)
	}
	if status, err := c.Status(ctx, cred, testRun); err != nil || status != "RUNNING" {
		t.Fatal(status, err)
	}
	if _, err := c.Reconcile(ctx, cred, testRun, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Logs(ctx, cred, testRun, 10); err != nil {
		t.Fatal(err)
	}
	if err := c.Cancel(ctx, cred, testRun); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 5 || !strings.Contains(paths[2], "idempotency_keys="+testRun) || !strings.Contains(paths[4], "/tasks/cancel") {
		t.Fatalf("routes: %v", paths)
	}
	policy := RetryPolicy{Deadline: time.Unix(20, 0), MaxAttempts: 3}
	if !policy.AllowSubmit(time.Unix(19, 0), 2) || policy.AllowSubmit(time.Unix(20, 0), 2) || policy.AllowSubmit(time.Unix(19, 0), 3) {
		t.Fatal("unbounded retry")
	}
}
