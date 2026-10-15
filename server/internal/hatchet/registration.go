package hatchet

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"slices"

	wr "github.com/multica-ai/multica/server/internal/workflowrunner"
)

// VerifyRegistration reads the exact version through server-private tenant REST.
// The daemon-supplied workflow/version IDs never establish registration alone.
func (c *Client) VerifyRegistration(ctx context.Context, cred TenantCredentials, e wr.Registration) error {
	mismatch := errors.New("workflow_registration_mismatch")
	if checkCredentials(cred) != nil || !validID(e.EngineWorkflowID) || !validID(e.EngineVersionID) {
		return mismatch
	}
	raw, err := c.request(ctx, "GET", "/api/v1/workflows/"+e.EngineWorkflowID+"/versions?version="+url.QueryEscape(e.EngineVersionID), cred.Token, nil)
	if err != nil {
		return mismatch
	}
	var version struct {
		Metadata struct {
			ID string `json:"id"`
		} `json:"metadata"`
		Workflow struct {
			Metadata struct {
				ID string `json:"id"`
			} `json:"metadata"`
			Name string `json:"name"`
		} `json:"workflow"`
		Config struct {
			Name          string
			EventTriggers []string
			CronTriggers  []string
			Tasks         []struct {
				ReadableID string
				Action     string
				Parents    []string
			}
			Idempotency struct {
				Expression string `json:"expression"`
				TTL        int64  `json:"ttlMs"`
				Method     string `json:"method"`
			} `json:"idempotency"`
		} `json:"workflowConfig"`
	}
	if json.Unmarshal(raw, &version) != nil || version.Metadata.ID != e.EngineVersionID || version.Workflow.Metadata.ID != e.EngineWorkflowID || version.Workflow.Name != e.EngineName || version.Config.Name != e.EngineName || len(version.Config.EventTriggers) != 0 || len(version.Config.CronTriggers) != 0 || version.Config.Idempotency.Expression != "input.multica_run_id" || version.Config.Idempotency.TTL != 3600000 || version.Config.Idempotency.Method != "TTL" || len(version.Config.Tasks) != len(e.Actions) {
		return mismatch
	}
	aliases := map[string]wr.Action{}
	for _, a := range e.Actions {
		aliases[a.StepID] = a
	}
	for _, step := range e.Graph.Steps {
		alias, ok := aliases[step.ID]
		if !ok {
			return mismatch
		}
		found := false
		for _, task := range version.Config.Tasks {
			if task.ReadableID != alias.TaskName {
				continue
			}
			if found || task.Action != alias.ActionID {
				return mismatch
			}
			found = true
			parents := make([]string, 0, len(step.Parents))
			for _, p := range step.Parents {
				parents = append(parents, aliases[p].TaskName)
			}
			slices.Sort(parents)
			slices.Sort(task.Parents)
			if !slices.Equal(parents, task.Parents) {
				return mismatch
			}
		}
		if !found {
			return mismatch
		}
	}
	return nil
}

// RevokeWorkerCredentials targets only a token minted for a failed/retired generation.
func (s *TenantService) RevokeWorkerCredentials(ctx context.Context, cred TenantCredentials) error {
	id, err := workerTokenID(cred.Token)
	if err != nil {
		return err
	}
	receipt := &WorkerTokenRevocationError{TenantID: cred.TenantID, TokenID: id}
	err = s.RetryWorkerTokenRevocation(ctx, receipt)
	if err != nil {
		s.mu.Lock()
		if s.pendingRevocations == nil {
			s.pendingRevocations = map[string]*WorkerTokenRevocationError{}
		}
		s.pendingRevocations[id] = receipt
		s.mu.Unlock()
	}
	return err
}
