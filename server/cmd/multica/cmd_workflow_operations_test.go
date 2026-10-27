package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const workflowChildID = "22222222-2222-2222-2222-222222222222"

func TestWorkflowAdditionalOperationsUseExistingContracts(t *testing.T) {
	cases := []struct {
		args                 []string
		method, suffix, body string
	}{
		{[]string{"delete", workflowTestID}, "DELETE", "", ""},
		{[]string{"revisions", workflowTestID, "--limit", "7", "--offset", "3"}, "GET", "/revisions?limit=7&offset=3", ""},
		{[]string{"trigger", "update", workflowTestID, workflowChildID, "--enabled=false", "--label", ""}, "PATCH", "/triggers/" + workflowChildID, `{"enabled":false,"label":""}`},
		{[]string{"trigger", "update", workflowTestID, workflowChildID, "--cron", "0 * * * *", "--timezone", "Asia/Shanghai", "--input-file", workflowSourceFile(t, `{"ticket":1}`)}, "PATCH", "/triggers/" + workflowChildID, `{"cron_expression":"0 * * * *","timezone":"Asia/Shanghai","input_template":{"ticket":1}}`},
		{[]string{"collaborator", "list", workflowTestID}, "GET", "/collaborators", ""},
		{[]string{"collaborator", "add", workflowTestID, workflowChildID, "--role", "editor"}, "POST", "/collaborators", `{"user_id":"22222222-2222-2222-2222-222222222222","role":"editor"}`},
		{[]string{"collaborator", "remove", workflowTestID, workflowChildID}, "DELETE", "/collaborators/" + workflowChildID, ""},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args[:2], "/"), func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != tc.method || r.URL.RequestURI() != "/api/workflows/"+workflowTestID+tc.suffix {
					t.Errorf("request=%s %s", r.Method, r.URL.RequestURI())
				}
				if r.Header.Get("Authorization") != "Bearer mat_operation-test" || r.Header.Get("X-Task-ID") != "task-a" || r.Header.Get("X-Agent-ID") != "agent-a" {
					t.Errorf("missing task auth: %v", r.Header)
				}
				if tc.body != "" {
					var got, want any
					_ = json.NewDecoder(r.Body).Decode(&got)
					_ = json.Unmarshal([]byte(tc.body), &want)
					a, _ := json.Marshal(got)
					b, _ := json.Marshal(want)
					if string(a) != string(b) {
						t.Errorf("body=%s want=%s", a, b)
					}
				}
				if tc.method == "DELETE" {
					w.WriteHeader(204)
				} else {
					fmt.Fprint(w, `{"id":"result"}`)
				}
			}))
			defer srv.Close()
			setCLITestServerEnv(t, srv.URL)
			t.Setenv("MULTICA_TOKEN", "mat_operation-test")
			t.Setenv("MULTICA_TASK_ID", "task-a")
			t.Setenv("MULTICA_AGENT_ID", "agent-a")
			out, err := workflowTestExecute(t, nil, tc.args...)
			if err != nil || calls != 1 || !json.Valid([]byte(out)) {
				t.Fatalf("calls=%d output=%q err=%v", calls, out, err)
			}
		})
	}
}

func TestWorkflowRevisionSourceIsExplicitAndOtherSecretsStayRedacted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"id":"revision","definition":"const password = 'authored-source';","token":"hidden-token","input":{"password":"hidden-input"}}`)
	}))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	for _, show := range []bool{false, true} {
		args := []string{"get", workflowTestID, "--revision", workflowChildID}
		if show {
			args = append(args, "--show-source")
		}
		out, err := workflowTestExecute(t, nil, args...)
		if err != nil || strings.Contains(out, "authored-source") != show || strings.Contains(out, "hidden-token") || strings.Contains(out, "hidden-input") {
			t.Fatalf("show=%v output=%q err=%v", show, out, err)
		}
	}
}

func TestWorkflowAdditionalOperationsRejectInvalidInputBeforeHTTP(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; fmt.Fprint(w, `{}`) }))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	for _, args := range [][]string{
		{"delete", "bad-id"}, {"revisions", workflowTestID, "--limit", "101"}, {"revisions", workflowTestID, "--offset", "-1"},
		{"trigger", "update", workflowTestID, workflowChildID}, {"trigger", "update", workflowTestID, workflowChildID, "--input-file", workflowSourceFile(t, `invalid`)},
		{"collaborator", "add", workflowTestID, workflowChildID}, {"collaborator", "add", workflowTestID, workflowChildID, "--role", "owner"},
		{"collaborator", "remove", workflowTestID, "bad-id"}, {"get", workflowTestID, "--show-source"}, {"get", workflowTestID, "--revision", workflowChildID, "--show-source", "--output", "table"},
	} {
		if _, err := workflowTestExecute(t, nil, args...); err == nil {
			t.Errorf("accepted %v", args)
		}
	}
	if calls != 0 {
		t.Fatalf("invalid input made %d HTTP calls", calls)
	}
}

func TestWorkflowAdditionalOperationPropagatesServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		fmt.Fprint(w, `{"error":"workflow_forbidden","code":"workflow_forbidden"}`)
	}))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	out, err := workflowTestExecute(t, nil, "collaborator", "add", workflowTestID, workflowChildID, "--role", "viewer")
	if err == nil || out != "" || !strings.Contains(err.Error(), "workflow_forbidden") {
		t.Fatalf("output=%q err=%v", out, err)
	}
}
