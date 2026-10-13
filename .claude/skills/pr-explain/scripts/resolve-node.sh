#!/usr/bin/env bash
# Print the bin directory of a node >= 20.11 (the PR Lens CLI minimum), or exit 1.
# Hardcodes no path: it asks the version managers, then sweeps the usual roots.
set -u

MIN_MAJOR=20
MIN_MINOR=11

acceptable() {
    local binary="$1" version major minor
    [ -x "$binary" ] || return 1
    version=$("$binary" --version 2>/dev/null) || return 1
    version=${version#v}
    major=${version%%.*}
    minor=${version#*.}
    minor=${minor%%.*}
    case "$major$minor" in
        ''|*[!0-9]*) return 1 ;;
    esac
    [ "$major" -gt "$MIN_MAJOR" ] && return 0
    [ "$major" -eq "$MIN_MAJOR" ] && [ "$minor" -ge "$MIN_MINOR" ] && return 0
    return 1
}

emit() {
    local directory
    directory=$(cd "$(dirname "$1")" && pwd) || return 1
    printf '%s\n' "$directory"
    exit 0
}

# 1. The node already on PATH.
on_path=$(command -v node 2>/dev/null) && acceptable "$on_path" && emit "$on_path"

# 2. A version manager, asked for its newest node.
if command -v fnm >/dev/null 2>&1; then
    candidate=$(fnm exec --using=22 -- node -e 'console.log(process.execPath)' 2>/dev/null) \
        && acceptable "$candidate" && emit "$candidate"
fi
if command -v volta >/dev/null 2>&1; then
    candidate=$(volta run --node 22 node -e 'console.log(process.execPath)' 2>/dev/null) \
        && acceptable "$candidate" && emit "$candidate"
fi
if command -v mise >/dev/null 2>&1; then
    candidate=$(mise which node 2>/dev/null) && acceptable "$candidate" && emit "$candidate"
fi
for nvm_script in "${NVM_DIR:-}/nvm.sh" "$HOME/.nvm/nvm.sh" "$HOME/.local/share/nvm/nvm.sh"; do
    [ -s "$nvm_script" ] || continue
    candidate=$(bash -c ". '$nvm_script' >/dev/null 2>&1 && nvm which node" 2>/dev/null) \
        && acceptable "$candidate" && emit "$candidate"
done

# 3. The usual roots, newest version first.
roots=$(ls -d \
    "${NVM_DIR:-/nonexistent}"/versions/node/*/bin/node \
    "${NVM_DIR:-/nonexistent}"/*/bin/node \
    "$HOME"/.nvm/versions/node/*/bin/node \
    "$HOME"/.local/share/nvm/*/bin/node \
    "$HOME"/.local/share/nvm/versions/node/*/bin/node \
    "$HOME"/.local/share/fnm/node-versions/*/installation/bin/node \
    "$HOME"/.local/share/mise/installs/node/*/bin/node \
    "$HOME"/.asdf/installs/nodejs/*/bin/node \
    "$HOME"/.volta/tools/image/node/*/bin/node \
    /usr/local/bin/node \
    /usr/local/*/bin/node \
    /opt/*/bin/node \
    /opt/*/*/bin/node \
    /snap/bin/node \
    2>/dev/null | sort -Vr)
for candidate in $roots; do
    acceptable "$candidate" && emit "$candidate"
done

# 4. A bounded sweep, newest version first.
sweep=$(find "$HOME" /opt /usr/local -maxdepth 6 -name node -type f -perm -u+x 2>/dev/null | sort -Vr)
for candidate in $sweep; do
    acceptable "$candidate" && emit "$candidate"
done

echo "no node >= ${MIN_MAJOR}.${MIN_MINOR} found" >&2
exit 1
