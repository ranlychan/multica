package hatchet

import (
	"strings"
	"testing"
)

func TestHatchetConfigDisabledAndStrict(t *testing.T) {
	c, err := ParseConfig(func(string) string { return "" }, "postgres://app@localhost/app")
	if err != nil || c.Enabled {
		t.Fatalf("disabled default: %+v %v", c, err)
	}
	for _, value := range []string{"yes", "1", "TRUE", " false "} {
		_, err := ParseConfig(func(k string) string {
			if k == "MULTICA_HATCHET_ENABLED" {
				return value
			}
			return ""
		}, "")
		if err == nil {
			t.Fatalf("accepted enabled=%q", value)
		}
	}
}

func TestHatchetConfigIsolationAndRedaction(t *testing.T) {
	env := map[string]string{"MULTICA_HATCHET_ENABLED": "true", "MULTICA_HATCHET_DATABASE_URL": "postgres://u:secret@localhost/engine", "MULTICA_HATCHET_API_URL": "http://127.0.0.1:8888", "MULTICA_HATCHET_GRPC_ADDRESS": "127.0.0.1:7070", "MULTICA_HATCHET_TLS_STRATEGY": "tls", "MULTICA_HATCHET_STATE_DIR": "/tmp/engine", "MULTICA_HATCHET_BINARY_DIR": "/tmp/bin", "MULTICA_HATCHET_PROVISIONING_EMAIL": "private@example.com", "MULTICA_HATCHET_PROVISIONING_PASSWORD": "private-password"}
	lookup := func(k string) string { return env[k] }
	if _, err := ParseConfig(lookup, "postgres://app@localhost/app"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ key, value string }{
		{"MULTICA_HATCHET_DATABASE_URL", "postgres://u:secret@127.0.0.1/app"},
		{"MULTICA_HATCHET_API_URL", "https://public.example.com"},
		{"MULTICA_HATCHET_GRPC_ADDRESS", "0.0.0.0:7070"},
		{"MULTICA_HATCHET_TLS_STRATEGY", "none"},
		{"MULTICA_HATCHET_STARTUP_TIMEOUT", "-1s"},
	} {
		previous := env[tc.key]
		env[tc.key] = tc.value
		_, err := ParseConfig(lookup, "postgres://app@localhost/app")
		if err == nil {
			t.Fatalf("accepted %s", tc.key)
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "private-password") {
			t.Fatalf("leaked: %v", err)
		}
		env[tc.key] = previous
	}
	env["MULTICA_HATCHET_TLS_STRATEGY"] = "mtls"
	env["MULTICA_HATCHET_GRPC_ADDRESS"] = "0.0.0.0:7070"
	if _, err := ParseConfig(lookup, "postgres://app@localhost/app"); err != nil {
		t.Fatal(err)
	}
}
