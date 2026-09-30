package hatchet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// These are source-contract checks, not an engine integration test. In particular,
// a green offline run cannot prove duplicate suppression or restart durability.
type contractFixture struct {
	Tag     string
	Commit  string
	Sources map[string]sourceFixture
}

type sourceFixture struct {
	Path   string
	SHA256 string // Whole upstream file, before excerpting.
	Start  int    // One-based inclusive; zero means the whole file.
	End    int
	Text   string
}

func TestPinnedCompatibility(t *testing.T) {
	data, err := os.ReadFile("testdata/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture contractFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Tag != UpstreamVersion || fixture.Commit != UpstreamCommit {
		t.Fatalf("VERSION differs from captured upstream: %s %s", fixture.Tag, fixture.Commit)
	}
	source := func(t *testing.T, name string) string {
		t.Helper()
		s, ok := fixture.Sources[name]
		if !ok || s.Text == "" {
			t.Fatalf("missing upstream evidence %q", name)
		}
		return s.Text
	}
	t.Run("REST_routes", func(t *testing.T) {
		file := parseSource(t, "package evidence\n"+source(t, "routes"))
		got := map[string]string{}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 2 {
				return true
			}
			method, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			path, ok := call.Args[0].(*ast.BinaryExpr)
			if !ok {
				return true
			}
			literal, ok := path.Y.(*ast.BasicLit)
			if !ok {
				return true
			}
			handler, ok := call.Args[1].(*ast.SelectorExpr)
			if !ok {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err == nil {
				got[handler.Sel.Name] = method.Sel.Name + " " + value
			}
			return true
		})
		for handler, want := range map[string]string{
			"UserUpdateLogin":           "POST /api/v1/users/login",
			"TenantCreate":              "POST /api/v1/tenants",
			"WorkflowList":              "GET /api/v1/tenants/:tenant/workflows",
			"V1DurableTaskEventLogList": "GET /api/v1/stable/tenants/:tenant/durable-tasks/:durable-task",
			"ApiTokenCreate":            "POST /api/v1/tenants/:tenant/api-tokens",
			"V1WorkflowRunCreate":       "POST /api/v1/stable/tenants/:tenant/workflow-runs/trigger",
			"V1WorkflowRunList":         "GET /api/v1/stable/tenants/:tenant/workflow-runs",
			"V1WorkflowRunGet":          "GET /api/v1/stable/workflow-runs/:v1-workflow-run",
			"V1WorkflowRunGetStatus":    "GET /api/v1/stable/workflow-runs/:v1-workflow-run/status",
			"V1TaskCancel":              "POST /api/v1/stable/tenants/:tenant/tasks/cancel",
			"V1TaskReplay":              "POST /api/v1/stable/tenants/:tenant/tasks/replay",
			"V1LogLineList":             "GET /api/v1/stable/tasks/:task/logs",
		} {
			if got[handler] != want {
				t.Errorf("%s: got %q, want %q", handler, got[handler], want)
			}
		}
	})
	t.Run("JSON_fields", func(t *testing.T) {
		for name, want := range map[string][]string{
			"CreateTenantRequest":         {"engineVersion", "environment", "name", "onboardingData", "slug"},
			"CreateAPITokenRequest":       {"expiresIn", "name"},
			"CreateAPITokenResponse":      {"token"},
			"V1TriggerWorkflowRunRequest": {"additionalMetadata", "input", "priority", "return_only_id", "workflowName"},
			"V1CancelTaskRequest":         {"externalIds", "filter"},
			"V1ReplayTaskRequest":         {"externalIds", "filter"},
			"V1WorkflowRunDetails":        {"run", "shape", "taskEvents", "tasks", "workflowConfig"},
		} {
			got := jsonFields(t, source(t, name), name)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("%s JSON fields: got %v, want %v", name, got, want)
			}
		}
	})
	t.Run("binary_flags", func(t *testing.T) {
		for name, flags := range requiredFlags() {
			got := sourceFlags(t, source(t, name))
			for _, flag := range flags {
				if !slices.Contains(got, flag) {
					t.Errorf("%s missing flag --%s", name, flag)
				}
			}
		}
		if !slices.Equal(BinaryNames, []string{"hatchet-admin", "hatchet-migrate", "hatchet-api", "hatchet-engine"}) {
			t.Fatalf("unsupported binaries: %v", BinaryNames)
		}
	})
	t.Run("captured_binary_help", func(t *testing.T) {
		for name, flags := range requiredFlags() {
			data, err := os.ReadFile(filepath.Join("testdata", "help", name+".txt"))
			if err != nil {
				t.Fatal(err)
			}
			for _, flag := range flags {
				if !hasHelpFlag(string(data), flag) {
					t.Errorf("captured %s help missing --%s", name, flag)
				}
			}
		}
	})
	t.Run("reconciliation_logs_and_state", func(t *testing.T) {
		for name, required := range map[string][]string{
			"V1WorkflowRunListParams": {"idempotency_keys", "additional_metadata", "since", "limit", "offset"},
			"V1WorkflowRun":           {"metadata", "status", "workflowVersionId", "tenantId", "input", "output"},
			"V1LogLine":               {"message", "createdAt", "attempt", "retryCount", "taskExternalId"},
			"V1LogLineListParams":     {"limit", "since", "until", "attempt", "order_by_direction"},
		} {
			got := jsonFields(t, source(t, name), name)
			for _, field := range required {
				if !slices.Contains(got, field) {
					t.Errorf("%s missing %s", name, field)
				}
			}
		}
		file := parseSource(t, "package evidence\n"+source(t, "statuses"))
		var statuses []string
		ast.Inspect(file, func(n ast.Node) bool {
			value, ok := n.(*ast.BasicLit)
			if ok && value.Kind == token.STRING {
				s, err := strconv.Unquote(value.Value)
				if err != nil {
					t.Fatal(err)
				}
				statuses = append(statuses, s)
			}
			return true
		})
		slices.Sort(statuses)
		if !slices.Equal(statuses, []string{"CANCELLED", "COMPLETED", "FAILED", "QUEUED", "RUNNING"}) {
			t.Fatalf("run statuses changed: %v", statuses)
		}
	})
	t.Run("registration_and_durable_sleep", func(t *testing.T) {
		// RPC declarations are wire contracts; these assertions are not tests of prose.
		for _, required := range []string{
			"rpc PutWorkflow(CreateWorkflowVersionRequest) returns (CreateWorkflowVersionResponse);",
			"string version = 3;", "optional IdempotencyConfig idempotency = 15;",
			"string expression = 1;", "int64 ttl_ms = 2;", "bool is_durable = 14;",
			"string existing_run_external_id = 1;",
		} {
			if !strings.Contains(source(t, "workflows.proto"), required) {
				t.Errorf("missing RPC contract %q", required)
			}
		}
		for _, required := range []string{"rpc DurableTask(stream DurableTaskRequest) returns (stream DurableTaskResponse)", "message DurableTaskWaitForRequest"} {
			if !strings.Contains(source(t, "dispatcher.proto"), required) {
				t.Errorf("missing durable RPC %q", required)
			}
		}
	})
	t.Run("local_tag_and_source", func(t *testing.T) {
		dir := os.Getenv("HATCHET_UPSTREAM_DIR")
		if dir == "" {
			t.Skip("set HATCHET_UPSTREAM_DIR to verify local tag and original source blobs")
		}
		got := command(t, dir, "git", "rev-parse", fixture.Tag+"^{commit}")
		if strings.TrimSpace(got) != fixture.Commit {
			t.Fatalf("tag resolves to %q, want %s", got, fixture.Commit)
		}
		for name, s := range fixture.Sources {
			original := command(t, dir, "git", "show", fixture.Commit+":"+s.Path)
			sum := sha256.Sum256([]byte(original))
			if hex.EncodeToString(sum[:]) != s.SHA256 {
				t.Fatalf("%s original source hash changed", name)
			}
			if s.Start != 0 {
				lines := strings.SplitAfter(original, "\n")
				if s.Start < 1 || s.End > len(lines) || s.End < s.Start {
					t.Fatalf("%s invalid source range", name)
				}
				original = strings.Join(lines[s.Start-1:s.End], "")
			}
			if original != s.Text {
				t.Errorf("%s excerpt differs from pinned upstream", name)
			}
		}
	})
	t.Run("remote_tag", func(t *testing.T) {
		if os.Getenv("HATCHET_VERIFY_REMOTE_TAG") != "1" {
			t.Skip("set HATCHET_VERIFY_REMOTE_TAG=1 to detect a moved remote tag")
		}
		out := command(t, "", "git", "ls-remote", UpstreamRepo+".git", "refs/tags/"+fixture.Tag, "refs/tags/"+fixture.Tag+"^{}")
		refs := map[string]string{}
		for _, line := range strings.Split(out, "\n") {
			parts := strings.Fields(line)
			if len(parts) == 2 {
				refs[parts[1]] = parts[0]
			}
		}
		ref := "refs/tags/" + fixture.Tag
		got := refs[ref+"^{}"]
		if got == "" {
			got = refs[ref]
		}
		if got != fixture.Commit {
			t.Fatalf("remote tag resolves to %q, want %s", got, fixture.Commit)
		}
	})
	t.Run("built_binary_help", func(t *testing.T) {
		dir := os.Getenv("HATCHET_BIN_DIR")
		if dir == "" {
			t.Skip("binaries not built: set HATCHET_BIN_DIR for actual --help checks")
		}
		for name, flags := range requiredFlags() {
			binary := name
			args := []string{"--help"}
			if name == "token" {
				binary = "hatchet-admin"
				args = []string{"token", "create", "--help"}
			}
			if name == "quickstart" {
				binary = "hatchet-admin"
				args = []string{"quickstart", "--help"}
			}
			out := command(t, "", filepath.Join(dir, binary), args...)
			for _, flag := range flags {
				if !hasHelpFlag(out, flag) {
					t.Errorf("%s help missing --%s", name, flag)
				}
			}
		}
	})
}

func TestHelpFlagRequiresCompleteOptionName(t *testing.T) {
	for _, tc := range []struct {
		name, help, flag string
		want             bool
	}{
		{"longer option is not a match", "      --up-to-penultimate   migrate to penultimate version\n", "up-to", false},
		{"exact option", "      --up-to string   migrate to a version\n", "up-to", true},
		{"short alias", "  -e, --expiresIn duration   token expiration\n", "expiresIn", true},
		{"description is not a flag", "      --other string   replaces --up-to\n", "up-to", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasHelpFlag(tc.help, tc.flag); got != tc.want {
				t.Fatalf("hasHelpFlag(%q, %q) = %v, want %v", tc.help, tc.flag, got, tc.want)
			}
		})
	}
}

func hasHelpFlag(help, flag string) bool {
	pattern := `(?m)^[ \t]+(?:-[[:alnum:]],\s+)?--` + regexp.QuoteMeta(flag) + `(?:[ \t=]|$)`
	return regexp.MustCompile(pattern).MatchString(help)
}

func requiredFlags() map[string][]string {
	return map[string][]string{
		"hatchet-admin": {"config", "version"}, "token": {"tenant-id", "name", "expiresIn"},
		"quickstart":  {"cert-dir", "generated-config-dir", "skip", "overwrite"},
		"hatchet-api": {"config", "version"}, "hatchet-engine": {"config", "version", "no-graceful-shutdown"},
		"hatchet-migrate": {"version", "down", "up-to", "up-to-penultimate"},
	}
}

func parseSource(t *testing.T, text string) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "upstream.go", text, 0)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func jsonFields(t *testing.T, text, name string) []string {
	t.Helper()
	fields := []string{}
	ast.Inspect(parseSource(t, "package evidence\n"+text), func(n ast.Node) bool {
		s, ok := n.(*ast.TypeSpec)
		if !ok || s.Name.Name != name {
			return true
		}
		st, ok := s.Type.(*ast.StructType)
		if !ok {
			t.Fatalf("%s is no longer a struct", name)
		}
		for _, field := range st.Fields.List {
			if field.Tag != nil {
				value, err := strconv.Unquote(field.Tag.Value)
				if err != nil {
					t.Fatal(err)
				}
				fields = append(fields, strings.Split(reflect.StructTag(value).Get("json"), ",")[0])
			}
		}
		return false
	})
	slices.Sort(fields)
	return fields
}

func sourceFlags(t *testing.T, text string) []string {
	t.Helper()
	flags := []string{}
	ast.Inspect(parseSource(t, text), func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) < 2 {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !strings.Contains(method.Sel.Name, "Var") {
			return true
		}
		literal, ok := call.Args[1].(*ast.BasicLit)
		if !ok {
			return true
		}
		name, err := strconv.Unquote(literal.Value)
		if err == nil {
			flags = append(flags, name)
		}
		return true
	})
	return flags
}

func command(t *testing.T, dir, name string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, out)
	}
	return string(out)
}
