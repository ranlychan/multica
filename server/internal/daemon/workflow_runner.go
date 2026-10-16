package daemon

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/multica-ai/multica/server/internal/daemon/processtree"
	wr "github.com/multica-ai/multica/server/internal/workflowrunner"
)

const WorkflowCodeWarning = "Workflows execute arbitrary code with this runtime's OS-user privileges, including access to local files and network credentials. Enable only for trusted workspace authors."

type WorkflowRunnerConfig struct {
	Enabled            bool
	AcceptedCodeRisk   bool
	Workspaces         []string
	Node, Entry, Cache string
	EngineAddress      string
	TLS                wr.TLS
	ValidationTimeout  time.Duration
}
type WorkflowRunner struct{ Config WorkflowRunnerConfig }
type WorkflowProcess struct {
	conn     net.Conn
	done     chan struct{}
	cancel   context.CancelFunc
	messages chan wr.Message
}

func (p *WorkflowProcess) Activate(ctx context.Context, generation string) error {
	_ = p.conn.SetWriteDeadline(time.Now().Add(time.Second))
	if json.NewEncoder(p.conn).Encode(map[string]string{"mode": "activate"}) != nil {
		return errors.New("workflow_worker_failed")
	}
	select {
	case message, ok := <-p.messages:
		if ok && message.Type == "active" && message.Generation == generation {
			return nil
		}
	case <-ctx.Done():
	case <-time.After(15 * time.Second):
	}
	return errors.New("workflow_worker_failed")
}
func (p *WorkflowProcess) Retire() error {
	_ = p.conn.SetWriteDeadline(time.Now().Add(time.Second))
	if json.NewEncoder(p.conn).Encode(map[string]string{"mode": "retire"}) != nil {
		return errors.New("workflow_worker_failed")
	}
	return nil
}

func (p *WorkflowProcess) Stop(timeout time.Duration) {
	_ = p.conn.SetWriteDeadline(time.Now().Add(time.Second))
	_ = json.NewEncoder(p.conn).Encode(map[string]string{"mode": "drain"})
	select {
	case <-p.done:
	case <-time.After(timeout):
		p.cancel()
		<-p.done
	}
	_ = p.conn.Close()
	p.cancel()
}
func (r *WorkflowRunner) start(ctx context.Context, message any) (*WorkflowProcess, error) {
	conn, child, err := workflowSocket()
	if err != nil {
		return nil, errors.New("workflow_ipc_unavailable")
	}
	childCtx, cancel := context.WithCancel(ctx)
	cmd := exec.Command(r.Config.Node, "--max-old-space-size=256", r.Config.Entry)
	cmd.Env = []string{"LANG=C", "NODE_CHANNEL_FD=3", "NODE_CHANNEL_SERIALIZATION_MODE=json"}
	cmd.ExtraFiles = []*os.File{child}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	p := &WorkflowProcess{conn: conn, done: make(chan struct{}), cancel: cancel, messages: make(chan wr.Message, 4)}
	go func() {
		defer close(p.done)
		defer child.Close()
		_ = processtree.Run(childCtx, cmd, time.Second)
		_ = conn.Close()
	}()
	go func() {
		defer close(p.messages)
		scan := bufio.NewScanner(conn)
		scan.Buffer(make([]byte, 4096), 2<<20)
		for scan.Scan() {
			var m wr.Message
			if json.Unmarshal(scan.Bytes(), &m) != nil {
				cancel()
				return
			}
			select {
			case p.messages <- m:
			case <-childCtx.Done():
				return
			}
		}
	}()
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if json.NewEncoder(conn).Encode(message) != nil {
		p.Stop(time.Millisecond)
		return nil, errors.New("workflow_worker_failed")
	}
	return p, nil
}
func (r *WorkflowRunner) Validate(ctx context.Context, source string) (wr.Validation, error) {
	timeout := r.Config.ValidationTimeout
	if timeout <= 0 || timeout > 30*time.Second {
		timeout = 6 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	p, err := r.start(ctx, map[string]any{"mode": "validate", "source": source})
	if err != nil {
		return wr.Validation{}, err
	}
	defer p.Stop(time.Millisecond)
	select {
	case m, ok := <-p.messages:
		if !ok || m.Type != "validated" || m.Result.Hash != wr.Hash(source) || m.Result.GraphDigest != wr.GraphDigest(m.Result.Graph) {
			return wr.Validation{}, errors.New("workflow_validation_failed")
		}
		return m.Result, nil
	case <-ctx.Done():
		return wr.Validation{}, errors.New("workflow_validation_timeout")
	}
}
func (r *WorkflowRunner) Start(ctx context.Context, d wr.Descriptor) (*WorkflowProcess, wr.Message, error) {
	return r.startWorker(ctx, ctx, d)
}
func (r *WorkflowRunner) startWorker(ctx, readyCtx context.Context, d wr.Descriptor) (*WorkflowProcess, wr.Message, error) {
	p, err := r.start(ctx, map[string]any{"mode": "worker", "descriptor": d})
	if err != nil {
		return nil, wr.Message{}, err
	}
	select {
	case m, ok := <-p.messages:
		if ok && m.Type == "ready" && m.Protocol == 1 && m.WorkspaceID == d.WorkspaceID && m.Generation == d.Generation && m.TenantID == d.TenantID {
			return p, m, nil
		}
	case <-readyCtx.Done():
	case <-time.After(25 * time.Second):
	}
	p.Stop(time.Second)
	return nil, wr.Message{}, errors.New("workflow_worker_failed")
}
func (r *WorkflowRunner) CacheSource(source, hash string) (string, error) {
	if hash != wr.Hash(source) {
		return "", errors.New("workflow_hash_mismatch")
	}
	if err := os.MkdirAll(r.Config.Cache, 0700); err != nil {
		return "", errors.New("workflow_cache_unavailable")
	}
	stat, err := os.Lstat(r.Config.Cache)
	if err != nil || !stat.IsDir() || stat.Mode().Perm() != 0700 {
		return "", errors.New("workflow_cache_not_private")
	}
	file, err := os.CreateTemp(r.Config.Cache, ".revision-")
	if err != nil {
		return "", errors.New("workflow_cache_unavailable")
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err = file.WriteString(source); err != nil {
		return "", err
	}
	if err = file.Sync(); err != nil {
		return "", err
	}
	if err = file.Close(); err != nil {
		return "", err
	}
	target := filepath.Join(r.Config.Cache, hash+".ts")
	if err = os.Rename(file.Name(), target); err != nil {
		return "", err
	}
	return target, nil
}
func (r *WorkflowRunner) Probe(ctx context.Context) (string, error) {
	c := r.Config
	if !c.Enabled || !c.AcceptedCodeRisk || len(c.Workspaces) == 0 {
		return "", errors.New("workflow_disabled")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.Command(c.Node, "--version")
	cmd.Env = []string{"LANG=C"}
	raw, err := processtree.Output(ctx, cmd, time.Second)
	v := strings.TrimPrefix(strings.TrimSpace(string(raw)), "v")
	major, _ := strconv.Atoi(strings.Split(v, ".")[0])
	if err != nil || major < 22 {
		return "", errors.New("workflow_node_unavailable")
	}
	if stat, e := os.Stat(c.Entry); e != nil || !stat.Mode().IsRegular() {
		return "", errors.New("workflow_runner_unavailable")
	}
	host, port, err := net.SplitHostPort(c.EngineAddress)
	if err != nil || port == "" || host == "" || host == "0.0.0.0" || host == "::" || host == "*" || c.TLS.ServerName == "" {
		return "", errors.New("workflow_engine_unavailable")
	}
	ca, err := os.ReadFile(c.TLS.CAFile)
	pool := x509.NewCertPool()
	if err != nil || !pool.AppendCertsFromPEM(ca) {
		return "", errors.New("workflow_tls_unavailable")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.TLS.ServerName, RootCAs: pool, NextProtos: []string{"h2"}}
	if c.TLS.Strategy == "mtls" {
		cert, e := tls.LoadX509KeyPair(c.TLS.CertFile, c.TLS.KeyFile)
		if e != nil {
			return "", errors.New("workflow_tls_unavailable")
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	} else if c.TLS.Strategy != "tls" {
		return "", errors.New("workflow_tls_unavailable")
	}
	ip := net.ParseIP(host)
	if c.TLS.Strategy != "mtls" && host != "localhost" && (ip == nil || (!ip.IsLoopback() && !ip.IsPrivate())) {
		return "", errors.New("workflow_tls_unavailable")
	}
	dialer := tls.Dialer{Config: tlsConfig}
	conn, err := dialer.DialContext(ctx, "tcp", c.EngineAddress)
	if err != nil {
		return "", errors.New("workflow_engine_unavailable")
	}
	_ = conn.Close()
	// Probe the actual packaged entrypoint; file existence alone is insufficient.
	if _, err = r.Validate(ctx, `import {defineWorkflow} from '@multica/workflow-sdk'; export default defineWorkflow({steps:[{id:'probe',run:async()=>null}]});`); err != nil {
		return "", errors.New("workflow_runner_unavailable")
	}
	return v, nil
}
