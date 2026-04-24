#!/usr/bin/env bash
#
# Install TuringDB on macOS and Linux.
#
# Strategy:
#   1. If the user's pip site-packages is writable, run `pip install turingdb`.
#   2. Otherwise, download the appropriate wheel and either:
#        - install it into a writable site-packages directory, or
#        - extract its payload into $HOME/.turing/install as a last resort.
#   3. If $HOME/.claude exists, install the TuringDB Claude skill.
#
# All temporary files are removed on exit.

set -euo pipefail

log() { printf '[install-turingdb] %s\n' "$*"; }
err() { printf '[install-turingdb] ERROR: %s\n' "$*" >&2; }

case "$(uname -s)" in
    Darwin|Linux) ;;
    *) err "Unsupported OS: $(uname -s)"; exit 1 ;;
esac

PYTHON="${PYTHON:-python3}"
if ! command -v "$PYTHON" >/dev/null 2>&1; then
    err "python3 not found in PATH"
    exit 1
fi

TMPDIR_INSTALL="$(mktemp -d)"
cleanup() { rm -rf "$TMPDIR_INSTALL"; }
trap cleanup EXIT

is_writable() {
    local dir="$1"
    if [ -d "$dir" ]; then
        [ -w "$dir" ]
        return $?
    fi
    local parent
    parent="$(dirname "$dir")"
    while [ ! -d "$parent" ] && [ "$parent" != "/" ] && [ "$parent" != "." ]; do
        parent="$(dirname "$parent")"
    done
    [ -w "$parent" ]
}

USER_SITE="$("$PYTHON" -m site --user-site)"
log "User site-packages: $USER_SITE"

install_ok=0

if is_writable "$USER_SITE"; then
    log "User site is writable — installing via 'pip install --user turingdb'"
    if "$PYTHON" -m pip install --user --upgrade turingdb >/dev/null 2>&1; then
        install_ok=1
    else
        log "pip install failed — will try manual wheel install"
    fi
else
    log "User site is not writable — will try manual wheel install"
fi

if [ "$install_ok" -ne 1 ]; then
    log "Downloading turingdb wheel for this platform…"
    if ! "$PYTHON" -m pip download \
            --only-binary=:all: \
            --no-deps \
            --dest "$TMPDIR_INSTALL" \
            turingdb; then
        err "Failed to download turingdb wheel for this platform"
        exit 1
    fi

    WHEEL="$(find "$TMPDIR_INSTALL" -maxdepth 1 -name 'turingdb-*.whl' | head -n1)"
    if [ -z "$WHEEL" ]; then
        err "No wheel file found after pip download"
        exit 1
    fi
    log "Downloaded wheel: $(basename "$WHEEL")"

    # Locate a writable site-packages: check system site-packages first, then user site.
    TARGET_SITE=""
    while IFS= read -r sp; do
        [ -z "$sp" ] && continue
        if is_writable "$sp"; then
            TARGET_SITE="$sp"
            break
        fi
    done < <("$PYTHON" -c "
import site
paths = []
try:
    paths.extend(site.getsitepackages())
except Exception:
    pass
paths.append(site.getusersitepackages())
for p in paths:
    print(p)
")

    if [ -n "$TARGET_SITE" ]; then
        log "Installing wheel into site-packages: $TARGET_SITE"
        mkdir -p "$TARGET_SITE"
        "$PYTHON" -m zipfile -e "$WHEEL" "$TARGET_SITE"
    else
        INSTALL_DIR="$HOME/.turing/install"
        log "No writable site-packages — extracting wheel payload to $INSTALL_DIR"
        mkdir -p "$INSTALL_DIR"
        "$PYTHON" -m zipfile -e "$WHEEL" "$INSTALL_DIR"
        log "Add '$INSTALL_DIR/turingdb/bin' to your PATH to use the turingdb CLI."
    fi
    install_ok=1
fi

if [ "$install_ok" -eq 1 ]; then
    log "TuringDB install complete."
fi

if [ -d "$HOME/.claude" ]; then
    log "Detected $HOME/.claude — installing TuringDB Claude skill"
    if command -v npx >/dev/null 2>&1; then
        # --yes skips the npx package-install confirmation. Stdin is redirected
        # from /dev/null so the skills CLI detects a non-interactive shell and
        # accepts defaults instead of blocking on a TTY prompt we can't answer.
        if npx --yes skills add https://github.com/turing-db/turingdb-skills </dev/null; then
            log "TuringDB skill installed."
        else
            log "Failed to install TuringDB skill (continuing)."
        fi
    else
        log "npx not found in PATH — skipping skill install."
    fi
fi

log "Done."
