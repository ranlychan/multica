package hatchet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

var ErrAmbiguousSubmission = errors.New("hatchet: submission outcome ambiguous")
var ErrTenantDeletionUnsupported = errors.New("hatchet: tenant_deletion_unsupported")

// TenantCredentials are server-only. Do not serialize these into product APIs.
type TenantCredentials struct {
	TenantID string
	Token    string
}
type SubmitRequest struct {
	RunID        string
	WorkflowName string
	Input        json.RawMessage
}

// RetryPolicy is evaluated against Task 4's persisted deadline and attempts.
// This client never automatically retries a submission or renews its deadline.
type RetryPolicy struct {
	Deadline    time.Time
	MaxAttempts int
}

func (p RetryPolicy) AllowSubmit(now time.Time, attempts int) bool {
	return attempts >= 0 && attempts < p.MaxAttempts && now.Before(p.Deadline)
}

type Client struct {
	origin string
	http   *http.Client
}

func NewClient(origin string, h *http.Client) (*Client, error) {
	u, e := url.Parse(origin)
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || !privateHost(u.Hostname()) || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("hatchet: private API origin required")
	}
	if h == nil {
		h = &http.Client{Timeout: 30 * time.Second}
	}
	copyHTTP := *h
	copyHTTP.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{origin: strings.TrimRight(origin, "/"), http: &copyHTTP}, nil
}
func validID(id string) bool { _, e := uuid.Parse(id); return e == nil && len(id) == 36 }
func (c *Client) request(ctx context.Context, method, path, token string, body any) (json.RawMessage, error) {
	var data []byte
	var e error
	if body != nil {
		data, e = json.Marshal(body)
		if e != nil {
			return nil, errors.New("hatchet: invalid JSON request")
		}
	}
	req, e := http.NewRequestWithContext(ctx, method, c.origin+path, bytes.NewReader(data))
	if e != nil {
		return nil, errors.New("hatchet: invalid request")
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, e := c.http.Do(req)
	if e != nil {
		return nil, errors.New("hatchet: request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("hatchet: HTTP %d", response.StatusCode)
	}
	data, e = io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if e != nil {
		return nil, errors.New("hatchet: response read failed")
	}
	return data, nil
}
func checkCredentials(cred TenantCredentials) error {
	if !validID(cred.TenantID) || strings.TrimSpace(cred.Token) == "" || strings.ContainsAny(cred.Token, "\r\n") {
		return errors.New("hatchet: invalid tenant credentials")
	}
	return nil
}
func (c *Client) Submit(ctx context.Context, cred TenantCredentials, s SubmitRequest) (json.RawMessage, error) {
	if e := checkCredentials(cred); e != nil {
		return nil, e
	}
	if !validID(s.RunID) || s.WorkflowName == "" || !json.Valid(s.Input) {
		return nil, errors.New("hatchet: invalid submission")
	}
	raw, e := c.request(ctx, "POST", "/api/v1/stable/tenants/"+cred.TenantID+"/workflow-runs/trigger", cred.Token, map[string]any{"workflowName": s.WorkflowName, "input": map[string]any{"multica_run_id": s.RunID, "input": s.Input}, "return_only_id": true})
	if e != nil {
		return nil, fmt.Errorf("%w: %v", ErrAmbiguousSubmission, e)
	}
	return raw, nil
}
func (c *Client) Detail(ctx context.Context, cred TenantCredentials, id string) (json.RawMessage, error) {
	return c.runGet(ctx, cred, id, "")
}
func (c *Client) runGet(ctx context.Context, cred TenantCredentials, id, suffix string) (json.RawMessage, error) {
	if e := checkCredentials(cred); e != nil {
		return nil, e
	}
	if !validID(id) {
		return nil, errors.New("hatchet: invalid run ID")
	}
	return c.request(ctx, "GET", "/api/v1/stable/workflow-runs/"+id+suffix, cred.Token, nil)
}
func (c *Client) Status(ctx context.Context, cred TenantCredentials, id string) (string, error) {
	raw, e := c.runGet(ctx, cred, id, "/status")
	if e != nil {
		return "", e
	}
	var status string
	if json.Unmarshal(raw, &status) != nil {
		return "", errors.New("hatchet: invalid status response")
	}
	switch status {
	case "QUEUED", "RUNNING", "COMPLETED", "FAILED", "CANCELLED":
		return status, nil
	}
	return "", errors.New("hatchet: unknown run status")
}
func (c *Client) Reconcile(ctx context.Context, cred TenantCredentials, id string, since time.Time) (json.RawMessage, error) {
	if e := checkCredentials(cred); e != nil {
		return nil, e
	}
	if !validID(id) {
		return nil, errors.New("hatchet: invalid local run ID")
	}
	q := url.Values{"idempotency_keys": {id}, "only_tasks": {"false"}, "limit": {"100"}, "since": {since.UTC().Format(time.RFC3339)}}
	return c.request(ctx, "GET", "/api/v1/stable/tenants/"+cred.TenantID+"/workflow-runs?"+q.Encode(), cred.Token, nil)
}
func (c *Client) Logs(ctx context.Context, cred TenantCredentials, taskID string, limit int) (json.RawMessage, error) {
	if e := checkCredentials(cred); e != nil {
		return nil, e
	}
	if !validID(taskID) || limit < 1 || limit > 1000 {
		return nil, errors.New("hatchet: invalid log query")
	}
	return c.request(ctx, "GET", fmt.Sprintf("/api/v1/stable/tasks/%s/logs?limit=%d", taskID, limit), cred.Token, nil)
}
func (c *Client) Cancel(ctx context.Context, cred TenantCredentials, id string) error {
	if e := checkCredentials(cred); e != nil {
		return e
	}
	if !validID(id) {
		return errors.New("hatchet: invalid run ID")
	}
	_, e := c.request(ctx, "POST", "/api/v1/stable/tenants/"+cred.TenantID+"/tasks/cancel", cred.Token, map[string]any{"externalIds": []string{id}})
	return e
}
