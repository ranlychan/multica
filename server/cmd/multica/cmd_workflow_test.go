package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mattn/go-shellwords"
	"github.com/multica-ai/multica/server/internal/cli"
	wr "github.com/multica-ai/multica/server/internal/workflowrunner"
)

const workflowTestID = "11111111-1111-1111-1111-111111111111"
const workflowTestSource = "export default {steps:[{id:'first',run:async()=>null}]}"

func workflowSourceFile(t *testing.T, source string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "workflow.ts")
	if err := os.WriteFile(path, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func workflowTestExecute(t *testing.T, validate func(context.Context, string) (wr.Validation, error), args ...string) (string, error) {
	t.Helper()
	cmd := newWorkflowCommand(validate)
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetArgs(args)
	return captureStdout(t, cmd.Execute)
}

func workflowTestValidation(_ context.Context, source string) (wr.Validation, error) {
	graph := wr.Graph{Steps: []wr.Step{{ID: "first", Parents: []string{}}}}
	return wr.Validation{Hash: wr.Hash(source), Graph: graph, GraphDigest: wr.GraphDigest(graph)}, nil
}

func TestWorkflowValidationFailureHasNoWritesOrSourceLeak(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(500) }))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	file := workflowSourceFile(t, "source contains private-secret")
	for _, args := range [][]string{
		{"validate", "--file", file, "--output", "json"},
		{"create", "--name", "Example", "--slug", "example", "--file", file},
		{"update", workflowTestID, "--file", file, "--expected-version", "1"},
	} {
		out, err := workflowTestExecute(t, func(context.Context, string) (wr.Validation, error) {
			return wr.Validation{}, errors.New("workflow_validation_failed")
		}, args...)
		if err == nil || !strings.Contains(err.Error(), "workflow_validation_failed") || strings.Contains(err.Error()+out, "private-secret") || out != "" {
			t.Fatalf("output=%q err=%v", out, err)
		}
	}
	if calls != 0 {
		t.Fatalf("validation failure made %d HTTP requests", calls)
	}
}

func TestWorkflowCreateReadsOneSnapshotAndPublishesExtractedGraph(t *testing.T) {
	file := workflowSourceFile(t, workflowTestSource)
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		switch r.URL.Path {
		case "/api/workflows":
			if body["name"] != "Example" || body["slug"] != "example" {
				t.Errorf("body=%v", body)
			}
			fmt.Fprintf(w, `{"id":%q,"name":"Example","definition_version":0}`, workflowTestID)
		case "/api/workflows/" + workflowTestID + "/revisions":
			if body["definition"] != workflowTestSource || body["expected_version"] != float64(0) {
				t.Errorf("body=%v", body)
			}
			graph, _ := json.Marshal(body["definition_graph"])
			if string(graph) != `{"steps":[{"id":"first","parents":[]}]}` {
				t.Errorf("graph=%s", graph)
			}
			fmt.Fprint(w, `{"revision":{"id":"revision-1","validation_status":"pending"},"registration_state":"pending_validation"}`)
		default:
			t.Errorf("unexpected path=%s", r.URL.Path)
		}
	}))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	out, err := workflowTestExecute(t, func(ctx context.Context, source string) (wr.Validation, error) {
		if err := os.Remove(file); err != nil {
			t.Fatal(err)
		}
		return workflowTestValidation(ctx, source)
	}, "create", "--name", "Example", "--slug", "example", "--file", file, "--output", "json")
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if json.Unmarshal([]byte(out), &result) != nil || result["registration_state"] != "pending_validation" || result["workflow"] == nil {
		t.Fatalf("output=%s", out)
	}
	if len(paths) != 2 || paths[1] != "POST /api/workflows/"+workflowTestID+"/revisions" {
		t.Fatalf("requests=%v", paths)
	}
}

func TestWorkflowConflictPreservesExplicitCASAndTypedError(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["expected_version"] != float64(7) {
			t.Errorf("body=%v", body)
		}
		w.WriteHeader(409)
		fmt.Fprint(w, `{"error":"workflow_version_conflict","code":"workflow_version_conflict"}`)
	}))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	out, err := workflowTestExecute(t, workflowTestValidation, "update", workflowTestID, "--file", workflowSourceFile(t, workflowTestSource), "--expected-version", "7", "--output", "json")
	var httpErr *cli.HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != 409 || calls != 1 || out != "" {
		t.Fatalf("calls=%d out=%q err=%v", calls, out, err)
	}
	message := cli.FormatError(err, false)
	if !strings.Contains(message, "workflow_version_conflict") || !strings.Contains(message, "reconcile") {
		t.Fatalf("message=%s", message)
	}
}

func TestWorkflowMetadataAndArchiveRequireObservedTimestamp(t *testing.T) {
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PATCH" || r.URL.Path != "/api/workflows/"+workflowTestID {
			t.Errorf("request=%s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		bodies = append(bodies, body)
		fmt.Fprint(w, `{"id":"workflow-1","status":"archived"}`)
	}))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	for _, args := range [][]string{{"update", workflowTestID, "--name", "Changed"}, {"archive", workflowTestID}} {
		if _, err := workflowTestExecute(t, nil, args...); err == nil {
			t.Fatalf("accepted missing CAS: %v", args)
		}
	}
	for _, args := range [][]string{{"update", workflowTestID, "--name", "Changed", "--expected-updated-at", "2026-10-17T01:02:03.123456Z"}, {"archive", workflowTestID, "--expected-updated-at", "2026-10-17T01:02:03.123456Z"}} {
		if _, err := workflowTestExecute(t, nil, args...); err != nil {
			t.Fatal(err)
		}
	}
	if len(bodies) != 2 || bodies[0]["expected_updated_at"] != "2026-10-17T01:02:03.123456Z" || bodies[0]["name"] != "Changed" || bodies[1]["status"] != "archived" {
		t.Fatalf("bodies=%v", bodies)
	}
}

func TestWorkflowTaskIdentityReplayCancelAndRunInput(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		if r.Header.Get("Authorization") != "Bearer mat_owned-test" || r.Header.Get("X-Agent-ID") != "agent-a" || r.Header.Get("X-Task-ID") != "task-a" || r.Header.Get("X-Workspace-ID") != "ws-1" {
			t.Errorf("headers=%v", r.Header)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if strings.HasSuffix(r.URL.Path, "/run") && body["input"] == nil {
			t.Errorf("missing input=%v", body)
		}
		if body["principal_user_id"] != nil || body["agent_id"] != nil {
			t.Errorf("forged identity=%v", body)
		}
		fmt.Fprint(w, `{"id":"new-local-run","status":"queued","cancel_requested_at":null}`)
	}))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	t.Setenv("MULTICA_TOKEN", "mat_owned-test")
	t.Setenv("MULTICA_AGENT_ID", "agent-a")
	t.Setenv("MULTICA_TASK_ID", "task-a")
	for _, args := range [][]string{{"run", workflowTestID, "--input-file", workflowSourceFile(t, `{"ticket":1}`)}, {"replay", workflowTestID, "22222222-2222-2222-2222-222222222222"}, {"cancel", workflowTestID, "22222222-2222-2222-2222-222222222222"}} {
		out, err := workflowTestExecute(t, nil, append(args, "--output", "json")...)
		if err != nil || !json.Valid([]byte(out)) {
			t.Fatalf("out=%q err=%v", out, err)
		}
	}
	if len(paths) != 3 || paths[1] != "POST /api/workflows/"+workflowTestID+"/runs/22222222-2222-2222-2222-222222222222/replay" || !strings.HasSuffix(paths[2], "/cancel") {
		t.Fatalf("requests=%v", paths)
	}
}

func TestWorkflowSecretsAreOptInOnlyForOneTimeTriggerResponses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"id":"trigger-a","kind":"api","token":"wft_verysecret","input_template":{"password":"hidden"}}`)
	}))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	for _, operation := range [][]string{{"trigger", "add", workflowTestID, "--kind", "api"}, {"trigger", "rotate-token", workflowTestID, "22222222-2222-2222-2222-222222222222"}} {
		out, err := workflowTestExecute(t, nil, operation...)
		if err != nil || strings.Contains(out, "wft_verysecret") || strings.Contains(out, "hidden") {
			t.Fatalf("out=%q err=%v", out, err)
		}
		out, err = workflowTestExecute(t, nil, append(operation, "--show-secrets", "--output", "json")...)
		if err != nil || !strings.Contains(out, "wft_verysecret") || !strings.Contains(out, srv.URL+"/api/workflow-triggers/wft_verysecret") || strings.Contains(out, "hidden") {
			t.Fatalf("out=%q err=%v", out, err)
		}
	}
}

func TestWorkflowBoundedLogsPreserveAvailabilityAndIncompleteReason(t *testing.T) {
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.RawQuery)
		if strings.HasSuffix(r.URL.Path, "/logs") {
			entries := make([]map[string]any, 300)
			for i := range entries {
				entries[i] = map[string]any{"step_id": "first", "level": "info", "message": "Bearer hidden " + strings.Repeat("x", 4096), "created_at": "2026-10-17T00:00:00Z"}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"available": true, "entries": entries, "run_id": "run-a"})
		} else {
			fmt.Fprint(w, `{"id":"run-a","status":"completed","reason_code":"workflow_final_logs_incomplete"}`)
		}
	}))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	out, err := workflowTestExecute(t, nil, "logs", workflowTestID, "22222222-2222-2222-2222-222222222222", "--limit", "100", "--offset", "50", "--output", "json")
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Available bool   `json:"available"`
		Entries   []any  `json:"entries"`
		Reason    string `json:"reason_code"`
	}
	if json.Unmarshal([]byte(out), &result) != nil || !result.Available || result.Reason != "workflow_final_logs_incomplete" || len(result.Entries) == 0 || len(result.Entries) > 200 || len(out) > 70000 || strings.Contains(out, "hidden") {
		t.Fatalf("entries=%d bytes=%d reason=%q err=%v", len(result.Entries), len(out), result.Reason, err)
	}
	if len(queries) != 2 || queries[0] != "limit=100&offset=50" {
		t.Fatalf("queries=%v", queries)
	}
	for _, flags := range [][]string{{"--limit", "101"}, {"--offset", "100001"}, {"--limit", "0"}} {
		if _, err := workflowTestExecute(t, nil, append([]string{"logs", workflowTestID, "22222222-2222-2222-2222-222222222222"}, flags...)...); err == nil {
			t.Errorf("accepted %v", flags)
		}
	}
	if len(queries) != 2 {
		t.Fatalf("invalid pagination made requests=%v", queries)
	}
}

func TestWorkflowCommandSurfaceRejectsUnsafeOrAmbiguousInputs(t *testing.T) {
	for _, args := range [][]string{
		{"validate", "--source", "secret"}, {"validate", "--file", "-"},
		{"update", workflowTestID, "--file", "missing.ts"},
		{"update", workflowTestID, "--name", "Mixed", "--file", "missing.ts", "--expected-version", "1"},
		{"get", "../escape"}, {"list", "--output", "yaml"},
		{"trigger", "add", workflowTestID, "--kind", "webhook"},
		{"trigger", "add", workflowTestID, "--kind", "schedule"},
		{"trigger", "add", workflowTestID, "--kind", "api", "--show-secrets", "--output", "table"},
	} {
		if _, err := workflowTestExecute(t, workflowTestValidation, args...); err == nil {
			t.Errorf("accepted %v", args)
		}
	}
	if cmd, _, err := rootCmd.Find([]string{"workflow", "run"}); err != nil || cmd.Name() != "run" || cmd.Parent().Name() != "workflow" {
		t.Fatalf("workflow missing from root: %v", err)
	}
	for _, name := range []string{"validate", "create", "update", "get", "list", "archive", "run", "runs", "logs", "replay", "cancel"} {
		cmd, _, err := newWorkflowCommand(nil).Find([]string{name})
		if err != nil || cmd.Name() != name || cmd.Flags().Lookup("output") == nil {
			t.Errorf("missing command=%s", name)
		}
	}
}

func TestWorkflowJSONPreservesLargeInputNumbersAndSourceReadSafety(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"id":"run-a","input":{"ticket":9007199254740993,"nested":{"api_key":"secret"}}}`)
	}))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	out, err := workflowTestExecute(t, nil, "get", workflowTestID, "--run", "22222222-2222-2222-2222-222222222222", "--output", "json")
	if err != nil || !strings.Contains(out, "9007199254740993") || strings.Contains(out, "secret") {
		t.Fatalf("out=%s err=%v", out, err)
	}
	for _, source := range []string{"", strings.Repeat("x", (256<<10)+1), string([]byte{0xff})} {
		if _, err := workflowTestExecute(t, workflowTestValidation, "validate", "--file", workflowSourceFile(t, source)); err == nil {
			t.Errorf("accepted invalid source bytes=%d", len(source))
		}
	}
}

func TestWorkflowValidationUsesCredentialFreeLocalRunner(t *testing.T) {
	source := workflowTestSource
	validation, _ := workflowTestValidation(context.Background(), source)
	message, _ := json.Marshal(map[string]any{"type": "validated", "result": validation})
	entry := workflowSourceFile(t, "packaged-entry-placeholder")
	node := filepath.Join(t.TempDir(), "owned-node")
	// The real runner owns fd3 and scrubs credentials. The test executable
	// speaks that actual IPC protocol without loading any installed runtime.
	script := "#!/bin/sh\n[ -z \"$MULTICA_TOKEN$HATCHET_CLIENT_TOKEN\" ] || exit 9\nread -r request <&3\nprintf '%s\\n' '" + string(message) + "' >&3\n"
	if err := os.WriteFile(node, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MULTICA_WORKFLOW_NODE", node)
	t.Setenv("MULTICA_WORKFLOW_RUNNER", entry)
	t.Setenv("MULTICA_TOKEN", "mat_secret")
	t.Setenv("HATCHET_CLIENT_TOKEN", "engine-secret")
	got, err := validateWorkflowSource(context.Background(), source)
	if err != nil || got.Hash != validation.Hash || len(got.Graph.Steps) != 1 {
		t.Fatalf("got=%v err=%v", got, err)
	}
}

func TestWorkflowReadAndTriggerRemoveRoutes(t *testing.T) {
	for _, test := range []struct {
		args         []string
		method, path string
	}{
		{[]string{"list", "--limit", "2", "--offset", "3"}, "GET", "/api/workflows?limit=2&offset=3"},
		{[]string{"get", workflowTestID}, "GET", "/api/workflows/" + workflowTestID},
		{[]string{"get", workflowTestID, "--revision", "22222222-2222-2222-2222-222222222222"}, "GET", "/api/workflows/" + workflowTestID + "/revisions/22222222-2222-2222-2222-222222222222"},
		{[]string{"runs", workflowTestID}, "GET", "/api/workflows/" + workflowTestID + "/runs?limit=50&offset=0"},
		{[]string{"trigger", "list", workflowTestID}, "GET", "/api/workflows/" + workflowTestID + "/triggers"},
		{[]string{"trigger", "remove", workflowTestID, "22222222-2222-2222-2222-222222222222"}, "DELETE", "/api/workflows/" + workflowTestID + "/triggers/22222222-2222-2222-2222-222222222222"},
	} {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != test.method || r.URL.RequestURI() != test.path {
					t.Errorf("request=%s %s", r.Method, r.URL.RequestURI())
				}
				if r.Method == "DELETE" {
					w.WriteHeader(204)
				} else {
					fmt.Fprint(w, `[]`)
				}
			}))
			defer srv.Close()
			setCLITestServerEnv(t, srv.URL)
			out, err := workflowTestExecute(t, nil, test.args...)
			if err != nil || !json.Valid([]byte(out)) {
				t.Fatalf("out=%q err=%v", out, err)
			}
		})
	}
}

func TestWorkflowLogsUnavailableDoesNotBecomeEmptyCompleteHistory(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/logs") {
			fmt.Fprint(w, `{"run_id":"run-a","entries":[],"available":false,"reason_code":"workflow_log_transport_pending"}`)
		} else {
			fmt.Fprint(w, `{"id":"run-a","status":"running","reason_code":null}`)
		}
	}))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	out, err := workflowTestExecute(t, nil, "logs", workflowTestID, "22222222-2222-2222-2222-222222222222")
	if err != nil || !strings.Contains(out, `"available": false`) || !strings.Contains(out, "workflow_log_transport_pending") {
		t.Fatalf("out=%s err=%v", out, err)
	}
}

func TestWorkflowTaskRefusalsKeepSharedExitClassification(t *testing.T) {
	for _, test := range []struct {
		status   int
		code     string
		wantExit int
	}{
		{403, "workflow_no_originator", cli.ExitAuth}, {503, "workflow_engine_unavailable", cli.ExitGeneric}, {401, "unauthorized", cli.ExitAuth},
	} {
		t.Run(test.code, func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(test.status)
				fmt.Fprintf(w, `{"code":%q,"error":%q}`, test.code, test.code)
			}))
			defer srv.Close()
			setCLITestServerEnv(t, srv.URL)
			t.Setenv("MULTICA_TOKEN", "mat_owned-test")
			t.Setenv("MULTICA_TASK_ID", "task-a")
			out, err := workflowTestExecute(t, nil, "run", workflowTestID)
			if calls != 1 || out != "" || cli.ExitCodeFor(err) != test.wantExit {
				t.Fatalf("calls=%d out=%q err=%v", calls, out, err)
			}
			message := cli.FormatError(err, false)
			if test.status == 401 {
				if !strings.Contains(message, "do not fall back") {
					t.Fatalf("message=%s", message)
				}
			} else if !strings.Contains(message, test.code) {
				t.Fatalf("message=%s", message)
			}
		})
	}
}

func TestWorkflowCreatePublicationFailureNamesPersistedIdentity(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			fmt.Fprintf(w, `{"id":%q,"definition_version":0}`, workflowTestID)
		} else {
			w.WriteHeader(409)
			fmt.Fprint(w, `{"code":"workflow_version_conflict","error":"workflow_version_conflict"}`)
		}
	}))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	out, err := workflowTestExecute(t, workflowTestValidation, "create", "--name", "Example", "--slug", "example", "--file", workflowSourceFile(t, workflowTestSource))
	if calls != 2 || out != "" || err == nil || !strings.Contains(cli.FormatError(err, false), workflowTestID) || !strings.Contains(cli.FormatError(err, false), "do not repeat create") {
		t.Fatalf("calls=%d out=%q err=%v", calls, out, err)
	}
}

func TestWorkflowRevisionReadRetainsMetadataWithoutEchoingAuthoredSource(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "revision-a", "validation_status": "pending", "definition": strings.Repeat("s", 200<<10)})
	}))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	out, err := workflowTestExecute(t, nil, "get", workflowTestID, "--revision", "22222222-2222-2222-2222-222222222222")
	if err != nil || !strings.Contains(out, "revision-a") || !strings.Contains(out, `"definition": "[redacted]"`) || strings.Contains(out, strings.Repeat("s", 100)) {
		t.Fatalf("bytes=%d out-prefix=%s err=%v", len(out), out[:min(len(out), 200)], err)
	}
}

func TestWorkflowOversizedHistoricalInputKeepsInputField(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "run-a", "input": map[string]any{"text": strings.Repeat("x", 65<<10)}})
	}))
	defer srv.Close()
	setCLITestServerEnv(t, srv.URL)
	out, err := workflowTestExecute(t, nil, "get", workflowTestID, "--run", "22222222-2222-2222-2222-222222222222")
	var result map[string]any
	_ = json.Unmarshal([]byte(out), &result)
	input, _ := result["input"].(map[string]any)
	if err != nil || input["_redacted"] != true || result["id"] != "run-a" {
		t.Fatalf("out=%s err=%v", out, err)
	}
}

func TestWorkflowShippedCommandExamplesMatchCLI(t *testing.T) {
	raw, err := os.ReadFile("../../internal/service/builtin_skills/multica-platform/references/workflows.md")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "multica workflow ") {
			continue
		}
		count++
		// Replace documentation metavariables before shell tokenization:
		// angle brackets are shell redirection, and version flags are integers.
		normalized := strings.ReplaceAll(line, "<observed-definition-version>", "1")
		normalized = strings.NewReplacer("<", "", ">", "").Replace(normalized)
		parser := shellwords.NewParser()
		parser.ParseEnv, parser.ParseBacktick = false, false
		words, err := parser.Parse(normalized)
		if err != nil {
			t.Fatal(err)
		}
		cmd, args, err := newWorkflowCommand(nil).Find(words[2:])
		if err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		if err = cmd.ParseFlags(args); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		if err = cmd.ValidateArgs(cmd.Flags().Args()); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		if workflowFlag(cmd, "output") != "json" {
			t.Fatalf("example lacks JSON: %s", line)
		}
	}
	if count < 18 {
		t.Fatalf("expected authoring/trigger/run examples, got %d", count)
	}
}

func TestWorkflowLocalPackagedValidation(t *testing.T) {
	node := os.Getenv("MULTICA_WORKFLOW_TEST_NODE")
	if node == "" {
		t.Skip("explicit Node22 executable required")
	}
	entry, err := filepath.Abs("../../../packages/workflow-runner/dist/cli.js")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("MULTICA_WORKFLOW_NODE", node)
	t.Setenv("MULTICA_WORKFLOW_RUNNER", entry)
	t.Setenv("MULTICA_TOKEN", "mat_must-not-reach-authored-code")
	source := `import {defineWorkflow} from '@multica/workflow-sdk'; if(process.env.MULTICA_TOKEN) throw Error('credential'); export default defineWorkflow({steps:[{id:'first',run:async()=>null}]});`
	got, err := validateWorkflowSource(context.Background(), source)
	if err != nil || got.Hash != wr.Hash(source) || len(got.Graph.Steps) != 1 || got.Graph.Steps[0].ID != "first" {
		t.Fatalf("got=%v err=%v", got, err)
	}
	for _, invalid := range []string{`import {defineWorkflow} from '@multica/workflow-sdk'; export default defineWorkflow({cron:'* * * * *',steps:[{id:'first',run:async()=>null}]});`, `import fs from 'node:fs'; export default {};`} {
		if _, err := validateWorkflowSource(context.Background(), invalid); err == nil {
			t.Fatalf("accepted invalid definition")
		}
	}
}
