// Package hatchet integrates the Hatchet workflow engine into Multica.
//
// This package records the intended engine pin and checked source contract.
// Building, shipping and supervising the binaries are separate integration work;
// see README.md for verified behavior and the remaining compatibility gate.
//
// It deliberately does NOT import github.com/hatchet-dev/hatchet/pkg/client/rest.
// That package pulls echo, grpc, protobuf and zerolog into Multica's build for
// the server. README.md lists the pinned API surface.
package hatchet

import (
	_ "embed"
	"fmt"
	"strings"
)

//go:embed VERSION
var versionFile string

// UpstreamVersion and UpstreamCommit pin the Hatchet release Multica builds and
// will build and talk to. VERSION is the single source of truth; future build
// and deployment tooling must consume the same file.
//
// Bumping the version is a two-part change: edit VERSION, then follow the
// upgrade procedure in ./README.md (an engine bump runs Hatchet's own database
// migrations against the operator's data).
var (
	UpstreamVersion = "unknown"
	UpstreamCommit  = "unknown"
)

func init() {
	lines := strings.Split(strings.TrimSpace(versionFile), "\n")
	if len(lines) > 0 && lines[0] != "" {
		UpstreamVersion = strings.TrimSpace(lines[0])
	}
	if len(lines) > 1 && lines[1] != "" {
		UpstreamCommit = strings.TrimSpace(lines[1])
	}
}

// BinaryNames are the Hatchet binaries selected for the integration, listed in
// the intended bootstrap order. hatchet-lite is deliberately absent: it requires
// LITE_STATIC_ASSET_DIR pointing at Hatchet's own built frontend (see
// cmd/hatchet-lite/main.go upstream), and Multica builds its own UI. The
// api+engine split also lets the engine scale independently of the API.
var BinaryNames = []string{
	"hatchet-admin",
	"hatchet-migrate",
	"hatchet-api",
	"hatchet-engine",
}

// UpstreamLicense is the license of the bundled binaries. Recorded here so the
// NOTICE file and any future attribution work read it from one place.
const UpstreamLicense = "MIT"

// UpstreamRepo is where the pinned source is cloned from at build time.
const UpstreamRepo = "https://github.com/hatchet-dev/hatchet"

// BinaryPath returns the absolute path of a bundled Hatchet binary inside dir.
// It reports an error for a name outside BinaryNames so a caller cannot be
// tricked into executing an arbitrary file in the bin directory.
func BinaryPath(dir, name string) (string, error) {
	for _, known := range BinaryNames {
		if known == name {
			return dir + "/" + name, nil
		}
	}
	return "", fmt.Errorf("unknown hatchet binary %q", name)
}
