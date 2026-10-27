package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/multica-ai/multica/server/internal/cli"
	"github.com/multica-ai/multica/server/internal/daemon"
	"github.com/multica-ai/multica/server/internal/service"
	wr "github.com/multica-ai/multica/server/internal/workflowrunner"
)

type workflowValidator func(context.Context, string) (wr.Validation, error)

var workflowCmd = newWorkflowCommand(validateWorkflowSource)

func newWorkflowCommand(validate workflowValidator) *cobra.Command {
	root := &cobra.Command{Use: "workflow", Short: "Author and operate durable TypeScript workflows"}
	add := func(parent *cobra.Command, use, short string, count int) *cobra.Command {
		cmd := &cobra.Command{Use: use, Short: short, Args: cobra.ExactArgs(count)}
		cmd.Flags().String("output", "json", "Output format: json or table")
		cmd.RunE = func(cmd *cobra.Command, args []string) error { return runWorkflow(cmd, args, validate) }
		parent.AddCommand(cmd)
		return cmd
	}
	file := func(cmd *cobra.Command) {
		cmd.Flags().String("file", "", "Local TypeScript source file (required; no stdin or inline source)")
	}
	page := func(cmd *cobra.Command) {
		cmd.Flags().Int("limit", 50, "Page size (1–100)")
		cmd.Flags().Int("offset", 0, "Page offset (0–100000); fetch one page per command")
	}
	metadata := func(cmd *cobra.Command) {
		cmd.Flags().String("name", "", "Workflow name")
		cmd.Flags().String("slug", "", "Lowercase workflow slug")
		cmd.Flags().String("description", "", "Workflow description")
	}
	cas := func(cmd *cobra.Command) {
		cmd.Flags().String("expected-updated-at", "", "Exact updated_at from workflow get (required for metadata/archive)")
	}
	file(add(root, "validate", "Validate source locally; executes code with your OS-user privileges", 0))
	create := add(root, "create", "Create identity and submit a source revision for trusted daemon validation", 0)
	file(create)
	metadata(create)
	update := add(root, "update <workflow-id>", "Publish source OR update metadata using explicit optimistic concurrency", 1)
	file(update)
	update.Flags().Lookup("file").Usage = "Local TypeScript source file for publication; omit for metadata updates"
	metadata(update)
	cas(update)
	update.Flags().Int32("expected-version", -1, "Observed definition_version (required with --file; 0 before first publication)")
	update.Flags().String("status", "", "active, paused, or archived")
	update.Flags().String("project-id", "", "Workspace project UUID; empty string clears")
	update.Flags().String("default-agent-id", "", "Workspace agent UUID; empty string clears")
	get := add(root, "get <workflow-id>", "Get workflow, revision validation status, or run details", 1)
	get.Flags().String("revision", "", "Revision UUID to inspect")
	get.Flags().Bool("show-source", false, "Reveal authored definition; requires --revision, JSON, and private output")
	get.Flags().String("run", "", "Run UUID to inspect, including bounded input and step state")
	page(add(root, "list", "List workflow identities", 0))
	cas(add(root, "archive <workflow-id>", "Archive using the observed updated_at", 1))
	add(root, "delete <workflow-id>", "Permanently delete workflow and its history; use archive for reversible status changes", 1)
	page(add(root, "revisions <workflow-id>", "List one bounded page of source revisions", 1))
	run := add(root, "run <workflow-id>", "Request a manual run as the requesting human; may incur real work and budget", 1)
	run.Flags().String("input-file", "", "Optional local JSON input file (64 KiB, depth 32)")
	page(add(root, "runs <workflow-id>", "List persisted local run history", 1))
	page(add(root, "logs <workflow-id> <run-id>", "Read one page of bounded redacted logs; complete history is not guaranteed", 2))
	add(root, "replay <workflow-id> <run-id>", "Request a new run of the current revision with the earlier input; may incur real work and budget", 2)
	add(root, "cancel <workflow-id> <run-id>", "Record a durable cancellation request; inspect run state for completion", 2)
	trigger := &cobra.Command{Use: "trigger", Short: "Manage workspace schedule/API triggers (source cannot declare triggers)"}
	root.AddCommand(trigger)
	triggerAdd := add(trigger, "add <workflow-id>", "Add a schedule or API trigger with a frozen human principal", 1)
	triggerAdd.Flags().String("kind", "", "Trigger kind: schedule or api (required)")
	triggerAdd.Flags().String("cron", "", "Five-field cron expression (required for schedule)")
	triggerAdd.Flags().String("timezone", "UTC", "IANA timezone (schedule only)")
	triggerAdd.Flags().String("label", "", "Optional trigger label")
	triggerAdd.Flags().Bool("enabled", true, "Enable this trigger")
	triggerAdd.Flags().String("input-file", "", "Optional local JSON input template file")
	triggerUpdate := add(trigger, "update <workflow-id> <trigger-id>", "Update explicitly supplied trigger fields", 2)
	triggerUpdate.Flags().Bool("enabled", true, "Enable or disable this trigger")
	for _, flag := range []string{"label", "cron", "timezone", "input-file"} {
		triggerUpdate.Flags().String(flag, "", "Replacement "+flag+" (only sent when supplied)")
	}
	add(trigger, "list <workflow-id>", "List triggers without bearer credentials", 1)
	add(trigger, "remove <workflow-id> <trigger-id>", "Remove a trigger", 2)
	rotate := add(trigger, "rotate-token <workflow-id> <trigger-id>", "Rotate an API bearer token; immediately invalidates the old token", 2)
	for _, cmd := range []*cobra.Command{triggerAdd, rotate} {
		cmd.Flags().Bool("show-secrets", false, "Reveal one-time API token and invocation URL; requires JSON and private output")
	}
	collaborator := &cobra.Command{Use: "collaborator", Short: "Manage human workflow collaborators"}
	root.AddCommand(collaborator)
	add(collaborator, "list <workflow-id>", "List workflow collaborators", 1)
	add(collaborator, "add <workflow-id> <user-id>", "Grant a workspace member viewer or editor access", 2).Flags().String("role", "", "Required role: viewer or editor")
	add(collaborator, "remove <workflow-id> <user-id>", "Remove a workflow collaborator", 2)
	return root
}

// The same bounded, credential-free packaged validator is used by the daemon.
// This is local authoring, never an application-server code execution fallback.
func validateWorkflowSource(ctx context.Context, source string) (wr.Validation, error) {
	node := os.Getenv("MULTICA_WORKFLOW_NODE")
	if node == "" {
		var err error
		node, err = exec.LookPath("node")
		if err != nil {
			return wr.Validation{}, errors.New("workflow_node_unavailable: configure MULTICA_WORKFLOW_NODE for Node.js 22+")
		}
	}
	entry := os.Getenv("MULTICA_WORKFLOW_RUNNER")
	if stat, err := os.Stat(entry); err != nil || !stat.Mode().IsRegular() {
		return wr.Validation{}, errors.New("workflow_runner_unavailable: configure MULTICA_WORKFLOW_RUNNER to the packaged runner entrypoint")
	}
	fmt.Fprintln(os.Stderr, "Warning: local workflow validation executes authored code with your OS-user privileges. Validate only trusted source.")
	runner := daemon.WorkflowRunner{Config: daemon.WorkflowRunnerConfig{Node: node, Entry: entry}}
	return runner.Validate(ctx, source)
}

// Read exactly one opened regular-file snapshot, with a bound before allocation.
func readWorkflowFile(path string, max int) ([]byte, error) {
	if path == "" || path == "-" {
		return nil, errors.New("a local --file/--input-file path is required; stdin and inline source are unsupported")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot open workflow file")
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() > int64(max) {
		return nil, errors.New("workflow file must be regular and within its byte limit")
	}
	raw, err := io.ReadAll(io.LimitReader(f, int64(max)+1))
	if err != nil || len(raw) > max || !utf8.Valid(raw) {
		return nil, errors.New("workflow file unreadable, oversized, or not UTF-8")
	}
	return raw, nil
}

func workflowFlag(cmd *cobra.Command, name string) string {
	v, _ := cmd.Flags().GetString(name)
	return v
}

func runWorkflow(cmd *cobra.Command, args []string, validate workflowValidator) error {
	output := workflowFlag(cmd, "output")
	if output != "json" && output != "table" {
		return errors.New("--output must be json or table")
	}
	showSecrets, _ := cmd.Flags().GetBool("show-secrets")
	showSource, _ := cmd.Flags().GetBool("show-source")
	if showSource && (output != "json" || workflowFlag(cmd, "revision") == "") {
		return errors.New("--show-source requires --revision and --output json; use private output")
	}
	if showSecrets && output != "json" {
		return errors.New("--show-secrets requires --output json")
	}
	for _, id := range args {
		if _, err := uuid.Parse(id); err != nil {
			return errors.New("workflow, revision, trigger, run, and collaborator user IDs must be UUIDs from documented reads")
		}
	}
	operation := cmd.Name()
	trigger := cmd.Parent().Name() == "trigger"
	collaborator := cmd.Parent().Name() == "collaborator"
	ctx, cancel := cli.APIContext(cmd.Context())
	defer cancel()
	body := map[string]any{}
	var source string
	var validation wr.Validation
	if operation == "validate" || operation == "create" || operation == "update" && !trigger && cmd.Flags().Changed("file") {
		if operation == "update" {
			v, _ := cmd.Flags().GetInt32("expected-version")
			if !cmd.Flags().Changed("expected-version") || v < 0 {
				return errors.New("--file requires an explicit --expected-version from workflow get")
			}
			for _, flag := range []string{"name", "slug", "description", "status", "project-id", "default-agent-id", "expected-updated-at"} {
				if cmd.Flags().Changed(flag) {
					return errors.New("publish source and update metadata in separate commands")
				}
			}
		}
		if operation == "create" && (strings.TrimSpace(workflowFlag(cmd, "name")) == "" || !service.ValidWorkflowSlug(workflowFlag(cmd, "slug"))) {
			return errors.New("--name and a valid lowercase --slug are required")
		}
		raw, err := readWorkflowFile(workflowFlag(cmd, "file"), service.WorkflowMaxSourceBytes)
		if err != nil {
			return err
		}
		source = string(raw)
		if source == "" {
			return errors.New("workflow_invalid_source")
		}
		validation, err = validate(ctx, source)
		if err != nil {
			return err
		}
		if operation == "validate" {
			return printWorkflowResult(output, validation)
		}
	}
	if operation == "archive" || operation == "update" && !trigger && source == "" {
		if cmd.Flags().Changed("expected-version") {
			return errors.New("--expected-version requires --file")
		}
		stamp := workflowFlag(cmd, "expected-updated-at")
		if _, err := time.Parse(time.RFC3339Nano, stamp); err != nil {
			return errors.New("--expected-updated-at must be the exact updated_at from workflow get")
		}
		body["expected_updated_at"] = stamp
		if operation == "archive" {
			body["status"] = "archived"
		} else {
			for _, flag := range []string{"name", "slug", "description", "status", "project-id", "default-agent-id"} {
				if cmd.Flags().Changed(flag) {
					body[strings.ReplaceAll(flag, "-", "_")] = workflowFlag(cmd, flag)
				}
			}
			if len(body) == 1 {
				return errors.New("no metadata fields to update")
			}
		}
	}
	if cmd.Flags().Lookup("limit") != nil {
		limit, _ := cmd.Flags().GetInt("limit")
		offset, _ := cmd.Flags().GetInt("offset")
		if limit < 1 || limit > 100 || offset < 0 || offset > 100000 {
			return errors.New("workflow_invalid_pagination: limit 1–100; offset 0–100000")
		}
	}
	if trigger && operation == "add" {
		kind := workflowFlag(cmd, "kind")
		if kind != "schedule" && kind != "api" {
			return errors.New("--kind must be schedule or api")
		}
		if kind == "schedule" && strings.TrimSpace(workflowFlag(cmd, "cron")) == "" {
			return errors.New("schedule triggers require --cron")
		}
		if kind == "api" && (cmd.Flags().Changed("cron") || cmd.Flags().Changed("timezone")) {
			return errors.New("--cron and --timezone require --kind schedule")
		}
		body["kind"] = kind
		body["label"] = workflowFlag(cmd, "label")
		body["enabled"], _ = cmd.Flags().GetBool("enabled")
		if kind == "schedule" {
			body["cron_expression"] = workflowFlag(cmd, "cron")
			body["timezone"] = workflowFlag(cmd, "timezone")
		}
	}
	if trigger && operation == "update" {
		for _, flag := range []string{"enabled", "label", "cron", "timezone"} {
			if cmd.Flags().Changed(flag) {
				if flag == "enabled" {
					body[flag], _ = cmd.Flags().GetBool(flag)
				} else if flag == "cron" {
					body["cron_expression"] = workflowFlag(cmd, flag)
				} else {
					body[flag] = workflowFlag(cmd, flag)
				}
			}
		}
		if len(body) == 0 && !cmd.Flags().Changed("input-file") {
			return errors.New("no trigger fields to update")
		}
	}
	if collaborator && operation == "add" {
		role := workflowFlag(cmd, "role")
		if role != "viewer" && role != "editor" {
			return errors.New("--role must be viewer or editor")
		}
		body["user_id"], body["role"] = args[1], role
	}
	if cmd.Flags().Changed("input-file") {
		raw, err := readWorkflowFile(workflowFlag(cmd, "input-file"), service.WorkflowMaxPayloadBytes)
		if err != nil {
			return err
		}
		if err = service.ValidateWorkflowPayload(raw); err != nil {
			return err
		}
		key := "input"
		if trigger {
			key = "input_template"
		}
		body[key] = json.RawMessage(raw)
	}
	if operation == "get" {
		if workflowFlag(cmd, "revision") != "" && workflowFlag(cmd, "run") != "" {
			return errors.New("--revision and --run are mutually exclusive")
		}
		for _, flag := range []string{"revision", "run"} {
			if value := workflowFlag(cmd, flag); value != "" {
				if _, err := uuid.Parse(value); err != nil {
					return fmt.Errorf("--%s requires a UUID", flag)
				}
			}
		}
	}
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	if _, err = requireWorkspaceID(cmd); err != nil {
		return err
	}
	path := "/api/workflows"
	if len(args) > 0 {
		path += "/" + args[0]
	}
	method := "GET"
	if trigger {
		path += "/triggers"
		if len(args) > 1 {
			path += "/" + args[1]
		}
		switch operation {
		case "add":
			method = "POST"
		case "remove":
			method = "DELETE"
		case "update":
			method = "PATCH"
		case "rotate-token":
			method = "POST"
			path += "/rotate-token"
		}
	} else if collaborator {
		path += "/collaborators"
		if operation == "add" {
			method = "POST"
		} else if operation == "remove" {
			method = "DELETE"
			path += "/" + args[1]
		}
	} else {
		switch operation {
		case "create":
			method = "POST"
			for _, key := range []string{"name", "slug", "description"} {
				body[key] = workflowFlag(cmd, key)
			}
		case "update", "archive":
			method = "PATCH"
		case "delete":
			method = "DELETE"
		case "revisions":
			path += "/revisions"
		case "run":
			path += "/run"
			method = "POST"
		case "runs":
			path += "/runs"
		case "logs", "replay", "cancel":
			path += "/runs/" + args[1] + "/" + operation
			if operation != "logs" {
				method = "POST"
			}
		case "get":
			if v := workflowFlag(cmd, "revision"); v != "" {
				path += "/revisions/" + v
			}
			if v := workflowFlag(cmd, "run"); v != "" {
				path += "/runs/" + v
			}
		}
	}
	if cmd.Flags().Lookup("limit") != nil {
		limit, _ := cmd.Flags().GetInt("limit")
		offset, _ := cmd.Flags().GetInt("offset")
		path += "?" + url.Values{"limit": {fmt.Sprint(limit)}, "offset": {fmt.Sprint(offset)}}.Encode()
	}
	var result any
	var response json.RawMessage
	if operation == "update" && source != "" {
		version, _ := cmd.Flags().GetInt32("expected-version")
		err = client.PublishWorkflowRevision(ctx, args[0], version, source, validation.Graph, &response)
	} else {
		switch method {
		case "GET":
			err = client.GetJSON(ctx, path, &response)
		case "POST":
			err = client.PostJSON(ctx, path, body, &response)
		case "PATCH":
			err = client.PatchJSON(ctx, path, body, &response)
		case "DELETE":
			err = client.DeleteJSON(ctx, path)
			result = map[string]any{"removed": true, "id": args[len(args)-1]}
		}
	}
	if err != nil {
		return cli.WorkflowRequestError(err)
	}
	if method != "DELETE" {
		result, err = decodeWorkflowJSON(response)
		if err != nil {
			return err
		}
	}
	if operation == "create" {
		workflow, ok := result.(map[string]any)
		id, _ := workflow["id"].(string)
		if !ok || id == "" {
			return errors.New("create workflow response missing id")
		}
		if err = client.PublishWorkflowRevision(ctx, id, 0, source, validation.Graph, &response); err != nil {
			return cli.WithUserMessage(fmt.Sprintf("Workflow identity %s was created, but revision publication failed. Inspect workflow get before retrying update; do not repeat create. %s", id, cli.FormatError(cli.WorkflowRequestError(err), false)), err)
		}
		decoded, decodeErr := decodeWorkflowJSON(response)
		published, valid := decoded.(map[string]any)
		if decodeErr != nil || !valid {
			return fmt.Errorf("workflow identity %s was created, but publication returned an invalid response; inspect workflow get before retrying", id)
		}
		published["workflow"] = workflow
		result = published
	}
	if operation == "logs" {
		view, ok := result.(map[string]any)
		if !ok {
			return errors.New("invalid workflow log response")
		}
		raw, _ := json.Marshal(view["entries"])
		var entries []service.WorkflowLogEntry
		if json.Unmarshal(raw, &entries) != nil {
			return errors.New("invalid workflow log entries")
		}
		view["entries"] = service.SanitizeWorkflowLogs(entries)
		var run map[string]any
		if err = client.GetJSON(ctx, "/api/workflows/"+args[0]+"/runs/"+args[1], &run); err != nil {
			return cli.WorkflowRequestError(err)
		}
		if reason, ok := run["reason_code"].(string); ok && reason != "" {
			view["reason_code"] = reason
		}
		if run["reason_code"] == "workflow_final_logs_incomplete" {
			fmt.Fprintln(os.Stderr, "Warning: workflow_final_logs_incomplete; known bounded logs are preserved, but final log recovery was exhausted.")
		}
	}
	var token string
	if trigger && (operation == "add" || operation == "rotate-token") {
		if view, ok := result.(map[string]any); ok {
			token, _ = view["token"].(string)
		}
		if token != "" {
			fmt.Fprintln(os.Stderr, "Warning: API bearer credentials are returned once. Use --show-secrets --output json only with private output; rotate again if the token was discarded.")
		}
	}
	// Reuse the product's recursive credential projection. Lists are sanitized
	// item by item so normal multi-row reads do not hit the input-size bound.
	var definition any
	if showSource {
		if view, ok := result.(map[string]any); ok {
			definition = view["definition"]
		}
	}
	result = sanitizeWorkflowResult(result)
	if showSource && definition != nil {
		result.(map[string]any)["definition"] = definition
	}
	if showSecrets && token != "" {
		if view, ok := result.(map[string]any); ok {
			view["token"] = token
			view["invocation_url"] = client.WorkflowTriggerURL(token)
		}
	}
	return printWorkflowResult(output, result)
}

func sanitizeWorkflowResult(value any) any {
	if rows, ok := value.([]any); ok {
		for i, row := range rows {
			rows[i] = sanitizeWorkflowResult(row)
		}
		return rows
	}
	// Log envelopes may exceed input64KiB after JSON formatting; their entries
	// have their own sanitization and byte limit above.
	if view, ok := value.(map[string]any); ok {
		clean := map[string]any{}
		for key, child := range view {
			if key == "input" || key == "input_template" {
				raw, _ := json.Marshal(child)
				clean[key], _ = decodeWorkflowJSON(service.SanitizeWorkflowInput(raw))
				continue
			}
			if key == "definition" {
				clean[key] = "[redacted]"
				continue
			}
			if key == "revision" || key == "workflow" {
				child = sanitizeWorkflowResult(child)
			}
			if entries, ok := child.([]service.WorkflowLogEntry); ok && key == "entries" {
				clean[key] = entries
				continue
			}
			raw, _ := json.Marshal(map[string]any{key: child})
			value, _ := decodeWorkflowJSON(service.SanitizeWorkflowInput(raw))
			item, _ := value.(map[string]any)
			if item["_redacted"] == true {
				clean[key] = item
				continue
			}
			for k, v := range item {
				clean[k] = v
			}
		}
		return clean
	}
	return value
}

func decodeWorkflowJSON(raw []byte) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, errors.New("invalid workflow JSON response")
	}
	return value, nil
}

func printWorkflowResult(output string, result any) error {
	if output == "json" {
		return cli.PrintJSON(os.Stdout, result)
	}
	rows := [][]string{}
	appendRow := func(value any) {
		if view, ok := value.(map[string]any); ok {
			rows = append(rows, []string{strVal(view, "id"), strVal(view, "name"), strVal(view, "status"), strVal(view, "reason_code")})
		}
	}
	if list, ok := result.([]any); ok {
		for _, v := range list {
			appendRow(v)
		}
	} else {
		appendRow(result)
	}
	// Compound authoring/validation/log results are best read as JSON; retain
	// every field rather than print an empty or misleading summary table.
	if len(rows) == 0 || len(rows) == 1 && strings.Join(rows[0], "") == "" {
		return cli.PrintJSON(os.Stdout, result)
	}
	cli.PrintTable(os.Stdout, []string{"ID", "NAME", "STATUS", "REASON"}, rows)
	return nil
}
