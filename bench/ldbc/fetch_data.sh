#!/bin/bash
# Fetches the LDBC SNB test data set the official Neo4j reference implementation ships
# (cypher/test-data/vanilla) and converts it to the JSONL graph format TuringDB imports.
#
# Usage: ./fetch_data.sh [output-dir]   (default: ./data)

set -eu
set -o pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

REPO_URL=https://github.com/ldbc/ldbc_snb_interactive_v1_impls.git
OUTPUT_DIR="${1:-data}"
CHECKOUT_DIR="${OUTPUT_DIR}/ldbc_snb_interactive_v1_impls"

mkdir -p "${OUTPUT_DIR}"

if [ ! -d "${CHECKOUT_DIR}" ]; then
    echo "Cloning ${REPO_URL}"
    git clone --depth 1 --filter=blob:none --sparse "${REPO_URL}" "${CHECKOUT_DIR}"
    git -C "${CHECKOUT_DIR}" sparse-checkout set cypher/test-data
fi

CSV_DIR="${CHECKOUT_DIR}/cypher/test-data/vanilla"
if [ ! -d "${CSV_DIR}" ]; then
    echo "Expected CSVs at ${CSV_DIR}" >&2
    exit 1
fi

python3 ldbc_to_jsonl.py "${CSV_DIR}" "${OUTPUT_DIR}/ldbc.jsonl"
