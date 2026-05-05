#!/bin/bash

# Skip on macOS GitHub Actions runners — the test pulls a 14 GB JSONL.
if [[ "$RUNNER_OS" == "macOS" ]]; then
    echo "Skipping reload_after_embedding_writeback on macOS runner"
    exit 0
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

# Kill any existing turingdb processes (SIGKILL for reliability)
pkill -9 turingdb 2>/dev/null || true
# Wait for port 6666 to be free
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null || break; sleep 0.1; done

rm -rf $SCRIPT_DIR/.turing

rm -f pyproject.toml
uv init
uv add $PYTURINGDB
uv add boto3

uv run main.py
testres=$?

turingdb stop -turing-dir $SCRIPT_DIR/.turing 2>/dev/null || true
pkill -9 turingdb 2>/dev/null || true

exit $testres
