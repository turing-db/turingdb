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

ensure_pip() {
    if "$PYTHON" -m pip --version >/dev/null 2>&1; then
        return 0
    fi
    log "pip not found — bootstrapping"

    if "$PYTHON" -m ensurepip --user --upgrade; then
        if "$PYTHON" -m pip --version >/dev/null 2>&1; then
            log "pip bootstrapped via ensurepip"
            return 0
        fi
    fi

    local get_pip="$TMPDIR_INSTALL/get-pip.py"
    log "ensurepip unavailable — downloading get-pip.py"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL https://bootstrap.pypa.io/get-pip.py -o "$get_pip" || return 1
    elif command -v wget >/dev/null 2>&1; then
        wget -q https://bootstrap.pypa.io/get-pip.py -O "$get_pip" || return 1
    else
        err "Neither curl nor wget available to download get-pip.py"
        return 1
    fi

    log "Running get-pip.py --user"
    if "$PYTHON" "$get_pip" --user && "$PYTHON" -m pip --version >/dev/null 2>&1; then
        log "pip bootstrapped via get-pip.py"
        return 0
    fi

    # PEP 668 (EXTERNALLY-MANAGED) on Debian/Ubuntu blocks --user installs.
    # Retry with --break-system-packages, which is safe here because it still
    # lands in the user site-packages, not the system one.
    log "Retrying get-pip.py with --break-system-packages"
    if "$PYTHON" "$get_pip" --user --break-system-packages \
            && "$PYTHON" -m pip --version >/dev/null 2>&1; then
        log "pip bootstrapped via get-pip.py (--break-system-packages)"
        return 0
    fi
    return 1
}

if ! ensure_pip; then
    err "Could not find or install pip. On Ubuntu/Debian try:"
    err "  sudo apt-get update && sudo apt-get install -y python3-pip"
    exit 1
fi

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
BIN_DIR=""

if is_writable "$USER_SITE"; then
    log "User site is writable — installing via 'pip install --user turingdb'"
    if "$PYTHON" -m pip install --user --upgrade turingdb >/dev/null 2>&1 \
            || "$PYTHON" -m pip install --user --upgrade --break-system-packages turingdb >/dev/null 2>&1; then
        install_ok=1
        BIN_DIR="$("$PYTHON" -c "import sysconfig; print(sysconfig.get_path('scripts', scheme='posix_user'))" 2>/dev/null || echo "$HOME/.local/bin")"
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
        BIN_DIR="$TARGET_SITE/turingdb/bin"
    else
        INSTALL_DIR="$HOME/.turing/install"
        log "No writable site-packages — extracting wheel payload to $INSTALL_DIR"
        mkdir -p "$INSTALL_DIR"
        "$PYTHON" -m zipfile -e "$WHEEL" "$INSTALL_DIR"
        BIN_DIR="$INSTALL_DIR/turingdb/bin"
    fi
    # python -m zipfile -e does not preserve the +x bit — restore it.
    if [ -d "$BIN_DIR" ]; then
        chmod +x "$BIN_DIR"/* 2>/dev/null || true
    fi
    install_ok=1
fi

add_to_path() {
    local bin_dir="$1"
    local marker="# >>> turingdb installer >>>"
    local end_marker="# <<< turingdb installer <<<"
    local block="${marker}
export PATH=\"${bin_dir}:\$PATH\"
${end_marker}"

    local updated=0
    local rc
    for rc in "$HOME/.bashrc" "$HOME/.zshrc" "$HOME/.profile"; do
        [ -f "$rc" ] || continue
        if grep -qF "$marker" "$rc"; then
            # Replace existing block so the path stays up to date.
            "$PYTHON" - "$rc" "$marker" "$end_marker" "$block" <<'PY'
import sys, re
path, start, end, block = sys.argv[1:]
with open(path) as f:
    text = f.read()
pattern = re.compile(re.escape(start) + r".*?" + re.escape(end), re.DOTALL)
new = pattern.sub(block, text)
with open(path, "w") as f:
    f.write(new)
PY
            log "Updated PATH block in $rc"
        else
            printf '\n%s\n' "$block" >> "$rc"
            log "Appended PATH block to $rc"
        fi
        updated=1
    done

    # Export in the current process — effective if this script was sourced.
    export PATH="${bin_dir}:$PATH"
    hash -r 2>/dev/null || true

    if [ "$updated" -eq 0 ]; then
        log "No shell rc file found — add '${bin_dir}' to PATH manually."
    fi
}

if [ "$install_ok" -eq 1 ]; then
    # pip's wheel installer does not always preserve the +x bit on package
    # data, so find the installed turingdb package's bin dir and restore it.
    PKG_BIN="$("$PYTHON" -c "import os, turingdb; print(os.path.join(os.path.dirname(turingdb.__file__), 'bin'))" 2>/dev/null || true)"
    if [ -n "$PKG_BIN" ] && [ -d "$PKG_BIN" ]; then
        chmod +x "$PKG_BIN"/* 2>/dev/null || true
    fi

    if [ -n "$BIN_DIR" ] && [ -d "$BIN_DIR" ]; then
        add_to_path "$BIN_DIR"
    fi
    if [ -n "$BIN_DIR" ] && [ -x "$BIN_DIR/turingdb" ]; then
        log "turingdb binary: $BIN_DIR/turingdb"
    else
        err "Expected turingdb binary at '$BIN_DIR/turingdb' but it's missing or not executable."
        err "Contents of '$BIN_DIR':"
        ls -la "$BIN_DIR" >&2 2>/dev/null || err "  (directory does not exist)"
    fi
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

if [ -n "$BIN_DIR" ]; then
    case "${SHELL:-}" in
        */zsh)  source_cmd="source ~/.zshrc" ;;
        */bash) source_cmd="source ~/.bashrc" ;;
        *)      source_cmd="source your shell's rc file" ;;
    esac
    echo
    echo "╔══════════════════════════════════════════════════════════════════════╗"
    echo "║                                                                      ║"
    echo "║                    >>>   ACTION REQUIRED   <<<                       ║"
    echo "║                                                                      ║"
    echo "║   turingdb is installed, but your CURRENT shell does not know        ║"
    echo "║   about it yet. To start using it RIGHT NOW, run:                    ║"
    echo "║                                                                      ║"
    printf "║       %-63s║\n" "$source_cmd"
    echo "║                                                                      ║"
    echo "║   ...or just open a new terminal window.                             ║"
    echo "║                                                                      ║"
    echo "╚══════════════════════════════════════════════════════════════════════╝"
    echo
fi
