#!/usr/bin/env bash
# Download the OptimusKG nodes.parquet and edges.parquet files from Harvard
# Dataverse via the upstream `optimuskg` Python client. Files are cached in
# ./data/ next to this script, and the canonical names are exposed as
# symlinks at data/nodes.parquet and data/edges.parquet for the import step.

set -euo pipefail
cd "$(dirname "$(readlink -f "$0")")"

if ! command -v uv >/dev/null 2>&1; then
    echo "uv is required (https://docs.astral.sh/uv/). Install it and re-run." >&2
    exit 1
fi

export OPTIMUSKG_CACHE_DIR="$PWD/data"
mkdir -p "$OPTIMUSKG_CACHE_DIR"

uv sync --quiet

for name in nodes.parquet edges.parquet; do
    if [ -L "data/$name" ] && [ -s "data/$name" ]; then
        echo "data/$name already present ($(du -hL "data/$name" | cut -f1)), skipping"
        continue
    fi
    echo "Downloading $name (this is ~150 MB per file) ..."
    src=$(uv run --quiet python -c "import optimuskg; print(optimuskg.get_file('$name'))")
    ln -sfn "$src" "data/$name"
    echo "  -> $(readlink -f "data/$name")"
done

echo
echo "Cached files (symlinks resolve to optimuskg cache layout):"
ls -lhL data/*.parquet 2>/dev/null || true
