#!/usr/bin/env bash
#
# Benchmark TuringDB with and without value hash join on the reactome dataset.
#
# This script handles everything end-to-end:
#   1. Clones turing-bench if not present
#   2. Installs Python dependencies via uv
#   3. Downloads the reactome dataset from S3 if not present
#   4. Runs benchmarks with and without TURING_VALUE_HASH_JOIN
#
# Prerequisites:
#   - turingdb built and on PATH (source setup.sh)
#   - uv installed (https://docs.astral.sh/uv/)
#   - aws CLI configured with 'turingdb_intern' profile (for dataset download)
#
# Usage:
#   ./bench_value_hash_join.sh [query-file]
#
# Examples:
#   ./bench_value_hash_join.sh
#   ./bench_value_hash_join.sh labelsets.cypher
#   TURING_BENCH_DIR=/path/to/turing-bench ./bench_value_hash_join.sh

set -e -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TURINGDB_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DATASET="reactome"
QUERY_FILE="${1:-queries_reactome.cypher}"
PORT=6667

# ---------------------------------------------------------------------------
# Step 1: Ensure turing-bench repo is available
# ---------------------------------------------------------------------------
TURING_BENCH_DEFAULT="$TURINGDB_ROOT/../turing-bench"

if [ -n "${TURING_BENCH_DIR:-}" ] && [ -d "$TURING_BENCH_DIR" ]; then
    echo "Using existing turing-bench at $TURING_BENCH_DIR"
elif [ -d "$TURING_BENCH_DEFAULT" ]; then
    TURING_BENCH_DIR="$(cd "$TURING_BENCH_DEFAULT" && pwd)"
    echo "Using existing turing-bench at $TURING_BENCH_DIR"
else
    TURING_BENCH_DIR="$(cd "$TURINGDB_ROOT/.." && pwd)/turing-bench"
    echo "Cloning turing-bench into $TURING_BENCH_DIR ..."
    git clone https://github.com/turing-db/turing-bench.git "$TURING_BENCH_DIR"
fi

# ---------------------------------------------------------------------------
# Step 2: Install Python dependencies
# ---------------------------------------------------------------------------
echo "Syncing Python dependencies in turing-bench ..."
(cd "$TURING_BENCH_DIR" && uv sync)

# ---------------------------------------------------------------------------
# Step 3: Ensure reactome dataset is downloaded
# ---------------------------------------------------------------------------
DUMPS="$TURING_BENCH_DIR/dumps"
mkdir -p "$DUMPS"

TURINGDB_DATA="$DUMPS/$DATASET.turingdb"

if [ ! -d "$TURINGDB_DATA" ]; then
    echo "Downloading reactome.turingdb dataset from S3 ..."
    if ! command -v aws &>/dev/null; then
        echo "Error: aws CLI not found. Install it or download the dataset manually."
        echo "  pip install awscli"
        echo "  aws s3 sync --profile turingdb_intern \\"
        echo "    s3://turingdb-external/bench-datasets/reactome.turingdb $TURINGDB_DATA"
        exit 1
    fi
    aws s3 sync --profile turingdb_intern \
        s3://turingdb-external/bench-datasets/reactome.turingdb "$TURINGDB_DATA"
fi

if [ ! -d "$TURINGDB_DATA" ]; then
    echo "Error: Reactome dataset not found at $TURINGDB_DATA"
    exit 1
fi

echo "Reactome dataset ready at $TURINGDB_DATA"

# ---------------------------------------------------------------------------
# Step 4: Validate query file
# ---------------------------------------------------------------------------
source "$TURING_BENCH_DIR/env.sh"

QUERY_FILE_PATH="$QUERIES_DIR/$DATASET/$QUERY_FILE"

if [ ! -f "$QUERY_FILE_PATH" ]; then
    echo "Error: Query file not found: $QUERY_FILE_PATH"
    echo "Available query files:"
    ls "$QUERIES_DIR/$DATASET/"
    exit 1
fi

# ---------------------------------------------------------------------------
# Step 5: Benchmark helpers
# ---------------------------------------------------------------------------
REPORT_DIR="$SCRIPT_DIR/reports"
mkdir -p "$REPORT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_FILE="$REPORT_DIR/vhj_bench_${TIMESTAMP}.txt"

stop_turingdb() {
    pkill -9 turingdb 2>/dev/null || true
    sleep 0.5
    for i in $(seq 1 50); do
        nc -z localhost $PORT 2>/dev/null || break
        sleep 0.1
    done
}

start_turingdb() {
    turingdb -demon -p $PORT -turing-dir "$TURINGDB_DATA" -load "$DATASET" &
    for i in $(seq 1 100); do
        nc -z localhost $PORT 2>/dev/null && break
        sleep 0.2
    done
    sleep 1
}

run_benchmark() {
    local label="$1"
    echo "================================================================"
    echo "  $label"
    echo "================================================================"
    (cd "$TURING_BENCH_DIR" && \
        uv run python -m turingbench turingdb \
            --query-file "$QUERY_FILE_PATH" \
            --database="$DATASET" \
            --url="http://localhost:$PORT")
    echo ""
}

# ---------------------------------------------------------------------------
# Step 6: Run benchmarks
# ---------------------------------------------------------------------------
echo ""
echo "=== Value Hash Join Benchmark ==="
echo "Dataset:    $DATASET"
echo "Queries:    $QUERY_FILE"
echo "Timestamp:  $TIMESTAMP"
echo ""

{

# --- Run WITHOUT value hash join ---
echo ""
echo ">>> BASELINE (without value hash join) <<<"
echo ""
stop_turingdb
unset TURING_VALUE_HASH_JOIN
start_turingdb
run_benchmark "WITHOUT value hash join"
stop_turingdb

# --- Run WITH value hash join ---
echo ""
echo ">>> VALUE HASH JOIN ENABLED <<<"
echo ""
export TURING_VALUE_HASH_JOIN=1
start_turingdb
run_benchmark "WITH value hash join"
stop_turingdb
unset TURING_VALUE_HASH_JOIN

echo "================================================================"
echo "  Benchmark complete. Results saved to: $REPORT_FILE"
echo "================================================================"

} 2>&1 | tee "$REPORT_FILE"
