#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
pin=$(sed -n '2p' "$root/server/internal/hatchet/VERSION")
patchfile="$root/server/internal/hatchet/private-listeners.patch"
out=${HATCHET_OUTPUT_DIR:-$root/server/bin/hatchet}
mkdir -p "$out"
out=$(cd "$out" && pwd)
builddir=$(mktemp -d)
trap 'rm -rf "$builddir"' EXIT
if [[ -n "${HATCHET_UPSTREAM_DIR:-}" ]]; then
  resolved=$(git -C "$HATCHET_UPSTREAM_DIR" rev-parse "$pin^{commit}")
  [[ "$resolved" == "$pin" ]] || { echo 'Hatchet full pin mismatch' >&2; exit 1; }
  git -C "$HATCHET_UPSTREAM_DIR" archive "$pin" | tar -x -C "$builddir"
else
  git clone --no-checkout https://github.com/hatchet-dev/hatchet "$builddir/source"
  resolved=$(git -C "$builddir/source" rev-parse "$pin^{commit}")
  [[ "$resolved" == "$pin" ]] || { echo 'Hatchet full pin mismatch' >&2; exit 1; }
  git -C "$builddir/source" archive "$pin" | tar -x -C "$builddir"
fi
cd "$builddir"
git apply --check --unidiff-zero "$patchfile"
git apply --unidiff-zero "$patchfile"
go build -o "$out/" ./cmd/hatchet-admin ./cmd/hatchet-migrate ./cmd/hatchet-api ./cmd/hatchet-engine
cp LICENSE "$out/LICENSE.hatchet"
printf '%s\n' "$pin" > "$out/PIN"
if command -v sha256sum >/dev/null; then
  sha256sum "$patchfile" | awk '{print $1}' > "$out/PATCH_SHA256"
  (cd "$out" && sha256sum hatchet-admin hatchet-migrate hatchet-api hatchet-engine) > "$out/BINARIES_SHA256"
else
  shasum -a 256 "$patchfile" | awk '{print $1}' > "$out/PATCH_SHA256"
  (cd "$out" && shasum -a 256 hatchet-admin hatchet-migrate hatchet-api hatchet-engine) > "$out/BINARIES_SHA256"
fi
