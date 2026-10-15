package hatchet

import (
	"context"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"testing"

	wr "github.com/multica-ai/multica/server/internal/workflowrunner"
)

func TestWorkflowWorkerProvisioningRenewsCookieLoginWithoutRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := r.Cookie("session"); err == nil {
			// Exact pin redirects an already-authenticated POST /users/login.
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "session", Value: "private-session", Path: "/"})
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	jar, _ := cookiejar.New(nil)
	client, _ := NewClient(server.URL, &http.Client{Jar: jar})
	s := &TenantService{client: client, email: "test", password: "test"}
	for i := 0; i < 2; i++ {
		if err := s.login(context.Background()); err != nil {
			t.Fatalf("credential login %d: %v", i, err)
		}
	}
}

func TestWorkflowRegistrationChecksActualTenantVersionAndGraph(t *testing.T) {
	const tenant = "10000000-0000-0000-0000-000000000001"
	const workflow = "20000000-0000-0000-0000-000000000002"
	const version = "30000000-0000-0000-0000-000000000003"
	action := "immutable:immutable-step-123"
	body := `{"metadata":{"id":"` + version + `"},"workflow":{"metadata":{"id":"` + workflow + `"},"name":"immutable"},"workflowConfig":{"Name":"immutable","Tasks":[{"ReadableId":"immutable-step-123","Action":"` + action + `","Parents":[]}],"idempotency":{"expression":"input.multica_run_id","ttlMs":3600000,"method":"TTL"}}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tenant-server" {
			t.Error("credential not scoped")
		}
		if r.URL.Path != "/api/v1/workflows/"+workflow+"/versions" || r.URL.Query().Get("version") != version {
			t.Error("wrong immutable version query")
		}
		w.Write([]byte(body))
	}))
	defer server.Close()
	client, _ := NewClient(server.URL, nil)
	evidence := wr.Registration{Revision: wr.Revision{EngineName: "immutable", Graph: wr.Graph{Steps: []wr.Step{{ID: "a", Parents: []string{}}}}}, EngineWorkflowID: workflow, EngineVersionID: version, Actions: []wr.Action{{StepID: "a", TaskName: "immutable-step-123", ActionID: action}}}
	cred := TenantCredentials{TenantID: tenant, Token: "tenant-server"}
	if err := client.VerifyRegistration(context.Background(), cred, evidence); err != nil {
		t.Fatal(err)
	}
	body = `{"metadata":{"id":"` + version + `"},"workflow":{"metadata":{"id":"` + workflow + `"},"name":"other"}}`
	if err := client.VerifyRegistration(context.Background(), cred, evidence); err == nil {
		t.Fatal("forged registration accepted")
	}
}
