package hatchet

import (
	"errors"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

var ErrUnavailable = errors.New("hatchet: unavailable")

type Config struct {
	Enabled              bool
	DatabaseURL          string
	APIURL               string
	GRPCAddress          string
	TLSStrategy          string
	StateDir             string
	BinaryDir            string
	ProvisioningEmail    string
	ProvisioningPassword string
	StartupTimeout       time.Duration
	ShutdownTimeout      time.Duration
}

func ParseConfig(get func(string) string, applicationDatabaseURL string) (Config, error) {
	c := Config{StartupTimeout: 45 * time.Second, ShutdownTimeout: 30 * time.Second}
	enabled := get("MULTICA_HATCHET_ENABLED")
	if enabled != "" && enabled != "true" && enabled != "false" {
		return c, errors.New("hatchet: invalid MULTICA_HATCHET_ENABLED")
	}
	c.Enabled = enabled == "true"
	if !c.Enabled {
		return c, nil
	}
	c.DatabaseURL = get("MULTICA_HATCHET_DATABASE_URL")
	c.APIURL = get("MULTICA_HATCHET_API_URL")
	c.GRPCAddress = get("MULTICA_HATCHET_GRPC_ADDRESS")
	c.TLSStrategy = get("MULTICA_HATCHET_TLS_STRATEGY")
	c.StateDir = get("MULTICA_HATCHET_STATE_DIR")
	c.BinaryDir = get("MULTICA_HATCHET_BINARY_DIR")
	c.ProvisioningEmail = get("MULTICA_HATCHET_PROVISIONING_EMAIL")
	c.ProvisioningPassword = get("MULTICA_HATCHET_PROVISIONING_PASSWORD")
	for key, value := range map[string]string{"DATABASE_URL": c.DatabaseURL, "API_URL": c.APIURL, "GRPC_ADDRESS": c.GRPCAddress, "STATE_DIR": c.StateDir, "BINARY_DIR": c.BinaryDir, "PROVISIONING_EMAIL": c.ProvisioningEmail, "PROVISIONING_PASSWORD": c.ProvisioningPassword} {
		if value == "" {
			return Config{}, errors.New("hatchet: missing MULTICA_HATCHET_" + key)
		}
	}
	engine, err := pgconn.ParseConfig(c.DatabaseURL)
	if err != nil {
		return Config{}, errors.New("hatchet: invalid MULTICA_HATCHET_DATABASE_URL")
	}
	app, err := pgconn.ParseConfig(applicationDatabaseURL)
	if err != nil {
		return Config{}, errors.New("hatchet: invalid application database identity")
	}
	// Requiring distinct database names fails closed even when DNS aliases,
	// multi-host fallback, or separate clusters obscure connection identity.
	if engine.Database == app.Database {
		return Config{}, errors.New("hatchet: database must be dedicated")
	}
	u, err := url.Parse(c.APIURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || !privateHost(u.Hostname()) {
		return Config{}, errors.New("hatchet: API_URL must be a private origin")
	}
	host, _, err := net.SplitHostPort(c.GRPCAddress)
	if err != nil || (c.TLSStrategy != "tls" && c.TLSStrategy != "mtls") || (!privateHost(host) && c.TLSStrategy != "mtls") {
		return Config{}, errors.New("hatchet: worker endpoint requires TLS; public endpoints require mTLS")
	}
	for key, target := range map[string]*time.Duration{"STARTUP_TIMEOUT": &c.StartupTimeout, "SHUTDOWN_TIMEOUT": &c.ShutdownTimeout} {
		if raw := get("MULTICA_HATCHET_" + key); raw != "" {
			d, e := time.ParseDuration(raw)
			if e != nil || d <= 0 {
				return Config{}, errors.New("hatchet: invalid MULTICA_HATCHET_" + key)
			}
			*target = d
		}
	}
	return c, nil
}

func privateHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsPrivate())
}
