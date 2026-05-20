#!/usr/bin/env bash
# Import the cached OptimusKG parquet files into a TuringDB graph using the
# turing-parquet tool. The resulting graph is written to ./turingdb.out/.

set -euo pipefail
cd "$(dirname "$(readlink -f "$0")")"

if [ ! -s data/nodes.parquet ] || [ ! -s data/edges.parquet ]; then
    echo "Missing data/nodes.parquet or data/edges.parquet. Run ./download.sh first." >&2
    exit 1
fi

if ! command -v turing-parquet >/dev/null 2>&1; then
    echo "turing-parquet is not on PATH. Build and install turingdb, then source setup.sh." >&2
    exit 1
fi

turing-parquet \
    -nodes data/nodes.parquet \
    -edges data/edges.parquet \
    -out turingdb.out \
    -graph optimuskg \
    "$@"
