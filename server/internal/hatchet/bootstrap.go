package hatchet

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

//go:embed private-listeners.patch
var listenerPatch []byte

func verifyBinaries(dir string) error {
	pin, e := os.ReadFile(filepath.Join(dir, "PIN"))
	if e != nil || strings.TrimSpace(string(pin)) != UpstreamCommit {
		return errors.New("hatchet: bundled pin receipt invalid")
	}
	patch, e := os.ReadFile(filepath.Join(dir, "PATCH_SHA256"))
	hash := sha256.Sum256(listenerPatch)
	if e != nil || strings.TrimSpace(string(patch)) != hex.EncodeToString(hash[:]) {
		return errors.New("hatchet: private listener patch receipt invalid")
	}
	receipt, e := os.ReadFile(filepath.Join(dir, "BINARIES_SHA256"))
	if e != nil {
		return errors.New("hatchet: binary checksum receipt missing")
	}
	sums := map[string]string{}
	for _, line := range strings.Split(string(receipt), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			sums[fields[1]] = fields[0]
		}
	}
	for _, name := range BinaryNames {
		raw, e := os.ReadFile(filepath.Join(dir, name))
		h := sha256.Sum256(raw)
		if e != nil || sums[name] != hex.EncodeToString(h[:]) {
			return errors.New("hatchet: binary checksum mismatch")
		}
	}
	return nil
}

func runCommand(ctx context.Context, c Config, env []string, name string, args ...string) error {
	path, e := BinaryPath(c.BinaryDir, name)
	if e != nil {
		return e
	}
	cmd := exec.CommandContext(ctx, path, args...)
	ownProcess(cmd)
	cmd.Env = env
	cmd.Dir = c.StateDir
	// Upstream bootstrap/token output can contain credentials. Do not forward it.
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if cmd.Run() != nil {
		return errors.New("hatchet: " + name + " failed")
	}
	return nil
}

type stateManifest struct {
	Files map[string]string `json:"files"`
}

var preservedFiles = []string{"config/server.yaml", "config/database.yaml", "certs/ca.cert", "certs/ca.key", "certs/cluster.pem", "certs/cluster.key", "certs/client-internal-admin.pem", "certs/client-internal-admin.key", "certs/client-worker.pem", "certs/client-worker.key"}

func stateHashes(c Config) (stateManifest, error) {
	m := stateManifest{Files: map[string]string{}}
	for _, name := range preservedFiles {
		path := filepath.Join(c.StateDir, name)
		fi, e := os.Lstat(path)
		if e != nil || !fi.Mode().IsRegular() {
			return m, errors.New("hatchet: incomplete persisted state")
		}
		raw, e := os.ReadFile(path)
		if e != nil || len(raw) == 0 {
			return m, errors.New("hatchet: empty persisted state")
		}
		h := sha256.Sum256(raw)
		m.Files[name] = hex.EncodeToString(h[:])
		if e = os.Chmod(path, 0600); e != nil {
			return m, errors.New("hatchet: secure state permissions failed")
		}
	}
	raw, _ := os.ReadFile(filepath.Join(c.StateDir, "config/server.yaml"))
	var cfg struct {
		Encryption struct {
			Master string `yaml:"masterKeyset"`
			JWT    struct {
				Public  string `yaml:"publicJWTKeyset"`
				Private string `yaml:"privateJWTKeyset"`
			} `yaml:"jwt"`
		} `yaml:"encryption"`
		Auth struct {
			Cookie struct {
				Secrets string `yaml:"secrets"`
			} `yaml:"cookie"`
		} `yaml:"auth"`
	}
	if yaml.Unmarshal(raw, &cfg) != nil || cfg.Encryption.Master == "" || cfg.Encryption.JWT.Public == "" || cfg.Encryption.JWT.Private == "" || cfg.Auth.Cookie.Secrets == "" {
		return m, errors.New("hatchet: persisted keysets invalid")
	}
	for _, pair := range [][2]string{{"cluster.pem", "cluster.key"}, {"client-internal-admin.pem", "client-internal-admin.key"}, {"client-worker.pem", "client-worker.key"}} {
		if _, e := tls.LoadX509KeyPair(filepath.Join(c.StateDir, "certs", pair[0]), filepath.Join(c.StateDir, "certs", pair[1])); e != nil {
			return m, errors.New("hatchet: persisted TLS invalid")
		}
	}
	return m, nil
}

func bootstrap(ctx context.Context, c Config, env []string) error {
	marker := filepath.Join(c.StateDir, "bootstrap.json")
	if raw, e := os.ReadFile(marker); e == nil {
		var old stateManifest
		if json.Unmarshal(raw, &old) != nil {
			return errors.New("hatchet: bootstrap receipt invalid")
		}
		current, e := stateHashes(c)
		if e != nil {
			return e
		}
		if len(old.Files) != len(current.Files) {
			return errors.New("hatchet: bootstrap receipt incomplete")
		}
		for name, sum := range current.Files {
			if old.Files[name] != sum {
				return errors.New("hatchet: persisted state changed; restore matching keysets")
			}
		}
		return nil
	}
	entries, e := os.ReadDir(c.StateDir)
	if e != nil || len(entries) != 0 {
		return errors.New("hatchet: incomplete bootstrap; restore or use empty state directory")
	}
	certs := filepath.Join(c.StateDir, "certs")
	config := filepath.Join(c.StateDir, "config")
	// LibreSSL on macOS truncates implicit serial paths at dots. This bootstrap
	// shim only supplies the explicit serial path; it never changes key material.
	openssl, e := exec.LookPath("openssl")
	if e != nil {
		return errors.New("hatchet: openssl required")
	}
	shim := filepath.Join(c.StateDir, "bootstrap-bin")
	if os.MkdirAll(shim, 0700) != nil {
		return errors.New("hatchet: bootstrap directory failed")
	}
	script := []byte("#!/bin/sh\nif [ \"$1\" = x509 ]; then\n exec \"$MULTICA_BOOTSTRAP_OPENSSL\" \"$@\" -CAserial \"$MULTICA_BOOTSTRAP_SERIAL\"\nfi\nexec \"$MULTICA_BOOTSTRAP_OPENSSL\" \"$@\"\n")
	if os.WriteFile(filepath.Join(shim, "openssl"), script, 0700) != nil {
		return errors.New("hatchet: bootstrap shim failed")
	}
	bootstrapEnv := append(append([]string{}, env...), "PATH="+shim+string(os.PathListSeparator)+os.Getenv("PATH"), "MULTICA_BOOTSTRAP_OPENSSL="+openssl, "MULTICA_BOOTSTRAP_SERIAL="+filepath.Join(certs, "ca.srl"))
	if e = runCommand(ctx, c, bootstrapEnv, "hatchet-admin", "quickstart", "--generated-config-dir", config, "--cert-dir", certs, "--skip", "seed", "--overwrite=false"); e != nil {
		return e
	}
	if e = runCommand(ctx, c, env, "hatchet-admin", "seed", "--config", config); e != nil {
		return e
	}
	m, e := stateHashes(c)
	if e != nil {
		return e
	}
	raw, _ := json.Marshal(m)
	if os.WriteFile(marker, raw, 0600) != nil {
		return errors.New("hatchet: bootstrap receipt write failed")
	}
	return nil
}
