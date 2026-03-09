#!/bin/bash

# Skip on macOS GitHub Actions runners
if [[ "$RUNNER_OS" == "macOS" ]]; then
    echo "Skipping load_jsonl_reactome on macOS runner"
    exit 0
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

# Kill any existing turingdb processes (SIGKILL for reliability)
pkill -9 turingdb 2>/dev/null || true
# Wait for port 6666 to be free (nc -z returns 0 if open, 1 if closed)
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null || break; sleep 0.1; done

rm -rf $SCRIPT_DIR/.turing
turingdb -demon -turing-dir $SCRIPT_DIR/.turing

rm -f pyproject.toml
uv init
uv add $PYTURINGDB
uv add boto3

uv run main.py
testres=$?

turingdb stop -turing-dir $SCRIPT_DIR/.turing

exit $testres
