package hatchet

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Disabled startup must perform no filesystem, database or process work.
func TestHatchetSupervisorDisabled(t *testing.T) {
	s := NewSupervisor(Config{}, nil)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.Ready() {
		t.Fatal("disabled engine is ready")
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHatchetSupervisorRejectsUnpatchedBinaries(t *testing.T) {
	c := Config{Enabled: true, DatabaseURL: "postgres://x/engine", BinaryDir: t.TempDir(), StateDir: t.TempDir(), APIURL: "http://127.0.0.1:8901", GRPCAddress: "127.0.0.1:8902", TLSStrategy: "tls"}
	s := NewSupervisor(c, nil)
	if err := s.Start(context.Background()); err == nil {
		t.Fatal("unverified binaries started")
	}
}

func TestHatchetBundledReceipt(t *testing.T) {
	bin := os.Getenv("HATCHET_BIN_DIR")
	if bin == "" {
		t.Skip("opt-in packaged receipt verification")
	}
	if err := verifyBinaries(bin); err != nil {
		t.Fatal(err)
	}
}

func TestHatchetBootstrapRefusesPartialState(t *testing.T) {
	c := Config{Enabled: true, StateDir: t.TempDir()}
	if err := os.WriteFile(filepath.Join(c.StateDir, "server.yaml"), []byte("valuable-keyset"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap(context.Background(), c, nil); err == nil {
		t.Fatal("partial state would regenerate keys")
	}
	got, _ := os.ReadFile(filepath.Join(c.StateDir, "server.yaml"))
	if string(got) != "valuable-keyset" {
		t.Fatal("keyset changed")
	}
}

func TestHatchetSupervisorHealthTimeoutAndPartialCleanup(t *testing.T) {
	for _, failAPI := range []bool{false, true} {
		t.Run(map[bool]string{false: "health-timeout", true: "partial-startup"}[failAPI], func(t *testing.T) {
			c := supervisorFixture(t)
			c.StartupTimeout = 150 * time.Millisecond
			if failAPI {
				if err := os.WriteFile(filepath.Join(c.BinaryDir, "hatchet-api"), []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
					t.Fatal(err)
				}
				writeBinaryReceipt(t, c.BinaryDir)
			}
			s := NewSupervisor(c, nil)
			if err := s.Start(context.Background()); err == nil {
				t.Fatal("startup succeeded")
			}
			if s.Ready() {
				t.Fatal("failed startup ready")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := s.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func writeBinaryReceipt(t *testing.T, dir string) {
	t.Helper()
	patch := sha256.Sum256(listenerPatch)
	for name, raw := range map[string][]byte{"PIN": []byte(UpstreamCommit + "\n"), "PATCH_SHA256": []byte(hex.EncodeToString(patch[:]) + "\n")} {
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	var sums strings.Builder
	for _, name := range BinaryNames {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(raw)
		fmt.Fprintf(&sums, "%x  %s\n", hash, name)
	}
	if err := os.WriteFile(filepath.Join(dir, "BINARIES_SHA256"), []byte(sums.String()), 0600); err != nil {
		t.Fatal(err)
	}
}

func supervisorFixture(t *testing.T) Config {
	t.Helper()
	fixture := t.TempDir()
	bin := t.TempDir()
	state := t.TempDir()
	for _, dir := range []string{"config", "certs"} {
		if err := os.Mkdir(filepath.Join(fixture, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "cluster"}, DNSNames: []string{"cluster"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range preservedFiles {
		raw := []byte("database: fixture\n")
		if strings.HasSuffix(name, ".key") {
			raw = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: pk})
		}
		if strings.HasSuffix(name, ".pem") || strings.HasSuffix(name, ".cert") {
			raw = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		}
		if name == "config/server.yaml" {
			raw = []byte("encryption:\n  masterKeyset: fixture-master\n  jwt:\n    publicJWTKeyset: fixture-public\n    privateJWTKeyset: fixture-private\nauth:\n  cookie:\n    secrets: fixture-cookie\n")
		}
		if err := os.WriteFile(filepath.Join(fixture, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range BinaryNames {
		script := "#!/bin/sh\nexit 0\n"
		if name == "hatchet-admin" {
			script = fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = quickstart ]; then cp -R '%s/config' '%s/certs' .; echo bootstrap >> calls; else echo seed >> calls; fi\n", fixture, fixture)
		}
		if name == "hatchet-engine" || name == "hatchet-api" {
			script = "#!/bin/sh\ntrap 'exit 0' TERM INT\nwhile :; do sleep 0.05; done\n"
		}
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	writeBinaryReceipt(t, bin)
	return Config{Enabled: true, DatabaseURL: "postgres://fixture/engine", APIURL: "http://127.0.0.1:1", GRPCAddress: "127.0.0.1:7070", TLSStrategy: "tls", StateDir: state, BinaryDir: bin, StartupTimeout: time.Second, ShutdownTimeout: time.Second}
}

func TestHatchetBootstrapFirstAndPreservedRestart(t *testing.T) {
	c := supervisorFixture(t)
	env := []string{"PATH=" + os.Getenv("PATH")}
	if err := bootstrap(context.Background(), c, env); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(c.StateDir, "config/server.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap(context.Background(), c, env); err != nil {
		t.Fatal(err)
	}
	calls, _ := os.ReadFile(filepath.Join(c.StateDir, "calls"))
	if string(calls) != "bootstrap\nseed\n" {
		t.Fatalf("restart bootstrapped again: %q", calls)
	}
	after, _ := os.ReadFile(filepath.Join(c.StateDir, "config/server.yaml"))
	if string(before) != string(after) {
		t.Fatal("keysets changed")
	}
	if err := os.WriteFile(filepath.Join(c.StateDir, "certs/cluster.key"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap(context.Background(), c, env); err == nil {
		t.Fatal("corrupt TLS accepted")
	}
}

func TestHatchetSupervisorRejectsMissingDatabase(t *testing.T) {
	s := NewSupervisor(Config{Enabled: true}, nil)
	if err := s.Start(context.Background()); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("missing database: %v", err)
	}
	if s.Ready() {
		t.Fatal("invalid engine is ready")
	}
}
