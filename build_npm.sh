#!/bin/bash

set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WASM_DIR="$ROOT_DIR/wasm"
BUILD_DIR="$WASM_DIR/build"
PACKAGE_DIR="$WASM_DIR/npm"
OUT_DIR="$ROOT_DIR/npm"

NUM_JOBS=${BUILD_JOBS:-$(getconf _NPROCESSORS_ONLN)}

EMSDK_ENV="$ROOT_DIR/external/dependencies/emsdk/emsdk_env.sh"
if [[ -f "$EMSDK_ENV" ]]; then
    source "$EMSDK_ENV"
fi

if ! command -v emcmake > /dev/null; then
    echo "emcmake not found: run ./dependencies.sh to install the pinned emsdk, or activate one on PATH" >&2
    exit 1
fi

# The wheel is cut from the same tags and must agree on the version, so both get it
# from scripts/project_version.py and differ only in the format they ask for.
npm_version() {
    if [[ -n "$VERSION_OVERRIDE" ]]; then
        echo "$VERSION_OVERRIDE"
        return
    fi

    "$ROOT_DIR/scripts/project_version.py" --format semver --dev-suffix "${DEV_VERSION_SUFFIX:-}"
}

set_package_version() {
    node -e '
const fs = require("fs");
const [manifestPath, version] = process.argv.slice(1);
const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
manifest.version = version;
fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
' "$PACKAGE_DIR/package.json" "$1"
}

# The version and the LICENSE are staged into the tracked package directory for the
# pack, the way build_backend.py stages binaries into python/turingdb/ for the wheel,
# and taken back out however this script exits.
VERIFY_DIR=""
cleanup() {
    set_package_version "0.0.0"
    rm -f "$PACKAGE_DIR/LICENSE"

    if [[ -n "$VERIFY_DIR" ]]; then
        rm -rf "$VERIFY_DIR"
    fi
}
trap cleanup EXIT

VERSION=$(npm_version)
echo "Building turingdb npm package $VERSION"

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"
emcmake cmake ..
make -j"$NUM_JOBS"

cd "$WASM_DIR"
node decoder_smoke.mjs

cd "$PACKAGE_DIR"
node client_smoke.mjs

cp "$ROOT_DIR/LICENSE" "$PACKAGE_DIR/"
set_package_version "$VERSION"

mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR"/turingdb-*.tgz

npm pack --pack-destination "$OUT_DIR"

TARBALL=$(ls "$OUT_DIR"/turingdb-*.tgz)

# Install what will actually be published and drive it through its public entry points,
# so a bad files list or exports map fails here rather than on a user's machine.
VERIFY_DIR=$(mktemp -d)
cp "$PACKAGE_DIR/verify_package.mjs" "$VERIFY_DIR/"

cd "$VERIFY_DIR"
npm install --silent --no-audit --no-fund "$TARBALL"
node verify_package.mjs

echo "Packed $TARBALL"
