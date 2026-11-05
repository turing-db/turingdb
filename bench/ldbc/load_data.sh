#!/bin/bash
# Loads data/ldbc.jsonl as the graph ldbc into a TuringDB directory, replacing any earlier
# load, with the turingdb built in this repository.
#
# Usage: ./load_data.sh [turing-dir]   (default: /tmp/ldbc)

set -eu
set -o pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

TURING_DIR="${1:-/tmp/ldbc}"
BINARY=../../build/tools/turingdb/turingdb
JSONL=data/ldbc.jsonl

if [ ! -f "${JSONL}" ] || [ ldbc_to_jsonl.py -nt "${JSONL}" ]; then
    echo "${JSONL} is missing or older than ldbc_to_jsonl.py, run ./fetch_data.sh" >&2
    exit 1
fi

rm -rf "${TURING_DIR}/graphs/ldbc"
mkdir -p "${TURING_DIR}/data"
cp "${JSONL}" "${TURING_DIR}/data/"

OUTPUT=$(echo 'LOAD JSONL "ldbc.jsonl" AS ldbc WITH DATETIMES ["birthday", "creationDate", "joinDate"]' \
    | "${BINARY}" -turing-dir "${TURING_DIR}" 2>&1)

if grep -q "\[error\]" <<< "${OUTPUT}"; then
    grep "\[error\]" <<< "${OUTPUT}" >&2
    exit 1
fi

grep "Query executed" <<< "${OUTPUT}"
