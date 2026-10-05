package hatchet

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type childProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
}

// Supervisor owns engine processes only. Workspace code is run by daemons.
type Supervisor struct {
	config      Config
	cleanup     func(context.Context) error
	mu          sync.Mutex
	ready       bool
	children    []*childProcess
	cancel      context.CancelFunc
	cleanupDone chan struct{}
}

func NewSupervisor(c Config, cleanup func(context.Context) error) *Supervisor {
	return &Supervisor{config: c, cleanup: cleanup}
}
func (s *Supervisor) Ready() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		return false
	}
	for _, p := range s.children {
		select {
		case <-p.done:
			return false
		default:
		}
	}
	return true
}

func (s *Supervisor) environment() ([]string, string, error) {
	c := s.config
	u, e := url.Parse(c.APIURL)
	if e != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.Port() == "" {
		return nil, "", errors.New("hatchet: bundled private API requires http://127.0.0.1:PORT")
	}
	if _, e = strconv.ParseUint(u.Port(), 10, 16); e != nil {
		return nil, "", errors.New("hatchet: API port invalid")
	}
	host, port, e := net.SplitHostPort(c.GRPCAddress)
	if e != nil || (c.TLSStrategy != "tls" && c.TLSStrategy != "mtls") || (!privateHost(host) && c.TLSStrategy != "mtls") {
		return nil, "", errors.New("hatchet: gRPC TLS policy invalid")
	}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return nil, "", errors.New("hatchet: health port unavailable")
	}
	healthPort := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	listener.Close()
	// Do not inherit ambient upstream overrides that can replace preserved keys,
	// listener policy, queues, credentials or remote configuration.
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + c.StateDir, "DATABASE_URL=" + c.DatabaseURL,
		"DATABASE_MAX_CONNS=15", "DATABASE_MIN_CONNS=1", "SERVER_MSGQUEUE_KIND=postgres", "SERVER_MSGQUEUE_PUBSUB_KIND=postgres",
		"SERVER_PORT=" + u.Port(), "SERVER_URL=" + c.APIURL, "SERVER_GRPC_PORT=" + port, "SERVER_GRPC_BIND_ADDRESS=" + host,
		"SERVER_GRPC_BROADCAST_ADDRESS=" + c.GRPCAddress, "SERVER_INTERNAL_CLIENT_INTERNAL_GRPC_BROADCAST_ADDRESS=127.0.0.1:" + port,
		"SERVER_INTERNAL_CLIENT_TLS_SERVER_NAME=cluster", "SERVER_HEALTHCHECK_PORT=" + healthPort, "SERVER_TLS_STRATEGY=" + c.TLSStrategy, "SERVER_GRPC_INSECURE=false",
		"SERVER_INTERNAL_CLIENT_BASE_INHERIT_BASE=false", "SERVER_INTERNAL_CLIENT_BASE_STRATEGY=" + c.TLSStrategy,
		"SERVER_INTERNAL_CLIENT_TLS_BASE_ROOT_CA_FILE=" + filepath.Join(c.StateDir, "certs/ca.cert"),
		"SERVER_INTERNAL_CLIENT_TLS_BASE_CERT_FILE=" + filepath.Join(c.StateDir, "certs/client-internal-admin.pem"), "SERVER_INTERNAL_CLIENT_TLS_BASE_KEY_FILE=" + filepath.Join(c.StateDir, "certs/client-internal-admin.key"),
		"SERVER_SECURITY_CHECK_ENABLED=false", "SERVER_AUTH_COOKIE_INSECURE=true", "SERVER_AUTH_COOKIE_DOMAIN=127.0.0.1",
		"SERVER_ALLOW_CREATE_TENANT=true", "SERVER_AUTH_BASIC_AUTH_ENABLED=true", "SERVER_AUTH_SET_EMAIL_VERIFIED=true",
		"SERVER_ANALYTICS_POSTHOG_ENABLED=false", "SERVER_ANALYTICS_AGGREGATE_ENABLED=false", "SERVER_PROMETHEUS_ENABLED=false",
		"ADMIN_EMAIL=" + c.ProvisioningEmail, "ADMIN_PASSWORD=" + c.ProvisioningPassword, "SERVER_LOGGER_LEVEL=warn"}
	return env, "http://127.0.0.1:" + healthPort + "/ready", nil
}

func (s *Supervisor) Start(ctx context.Context) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.config
	if !c.Enabled {
		return nil
	}
	if runtime.GOOS == "windows" {
		return errors.New("hatchet: bundled engine requires Unix host")
	}
	if s.ready {
		return nil
	}
	if len(s.children) > 0 {
		return ErrUnavailable
	}
	if c.DatabaseURL == "" || c.StateDir == "" || !filepath.IsAbs(c.StateDir) || !filepath.IsAbs(c.BinaryDir) {
		return errors.New("hatchet: database and absolute state/binary directories required")
	}
	if e := verifyBinaries(c.BinaryDir); e != nil {
		return e
	}
	env, health, e := s.environment()
	if e != nil {
		return e
	}
	if c.StartupTimeout <= 0 {
		c.StartupTimeout = 45 * time.Second
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	s.config = c
	startCtx, cancel := context.WithTimeout(ctx, c.StartupTimeout)
	defer cancel()
	if os.MkdirAll(c.StateDir, 0700) != nil || os.Chmod(c.StateDir, 0700) != nil {
		return errors.New("hatchet: state directory unavailable")
	}
	if e = runCommand(startCtx, c, env, "hatchet-migrate"); e != nil {
		return e
	}
	if e = bootstrap(startCtx, c, env); e != nil {
		return e
	}
	defer func() {
		if err != nil {
			stopCtx, cancel := context.WithTimeout(context.Background(), c.ShutdownTimeout)
			defer cancel()
			s.stop(stopCtx)
		}
	}()
	for _, name := range []string{"hatchet-engine", "hatchet-api"} {
		path, _ := BinaryPath(c.BinaryDir, name)
		cmd := exec.Command(path, "--config", filepath.Join(c.StateDir, "config"))
		ownProcess(cmd)
		cmd.Env = env
		cmd.Dir = c.StateDir
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		if cmd.Start() != nil {
			return errors.New("hatchet: service startup failed")
		}
		p := &childProcess{cmd: cmd, done: make(chan struct{})}
		s.children = append(s.children, p)
		go func() { _ = cmd.Wait(); close(p.done) }()
	}
	hc := &http.Client{Timeout: time.Second}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		alive := true
		for _, p := range s.children {
			select {
			case <-p.done:
				alive = false
			default:
			}
		}
		if !alive {
			return errors.New("hatchet: service exited during startup")
		}
		if healthy(startCtx, hc, health) && healthy(startCtx, hc, strings.TrimRight(c.APIURL, "/")+"/api/v1/meta") {
			break
		}
		select {
		case <-startCtx.Done():
			return errors.New("hatchet: startup health timeout")
		case <-ticker.C:
		}
	}
	s.ready = true
	if s.cleanup != nil {
		cleanupCtx, cancel := context.WithCancel(context.Background())
		s.cancel = cancel
		s.cleanupDone = make(chan struct{})
		done := s.cleanupDone
		go func() {
			defer close(done)
			timer := time.NewTicker(time.Minute)
			defer timer.Stop()
			for {
				attempt, cancel := context.WithTimeout(cleanupCtx, 30*time.Second)
				_ = s.cleanup(attempt)
				cancel()
				select {
				case <-cleanupCtx.Done():
					return
				case <-timer.C:
				}
			}
		}()
	}
	return nil
}
func healthy(ctx context.Context, hc *http.Client, origin string) bool {
	req, e := http.NewRequestWithContext(ctx, "GET", origin, nil)
	if e != nil {
		return false
	}
	r, e := hc.Do(req)
	if e != nil {
		return false
	}
	r.Body.Close()
	return r.StatusCode == http.StatusOK
}
func (s *Supervisor) stop(ctx context.Context) error {
	s.ready = false
	if s.cancel != nil {
		s.cancel()
		select {
		case <-s.cleanupDone:
		case <-ctx.Done():
		}
	}
	// Stop API before draining engine so no new submissions arrive during drain.
	for i := len(s.children) - 1; i >= 0; i-- {
		p := s.children[i]
		_ = terminateProcess(p.cmd)
		select {
		case <-p.done:
		case <-ctx.Done():
			_ = killProcess(p.cmd)
			<-p.done
		}
	}
	s.children = nil
	s.cancel = nil
	s.cleanupDone = nil
	return ctx.Err()
}
func (s *Supervisor) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stop(ctx)
}
