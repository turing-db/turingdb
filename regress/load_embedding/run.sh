#!/bin/bash

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
# pyarrow/numpy only build the test Parquet fixture; install them straight into
# the venv so uv doesn't re-run its universal lock (the single-platform turingdb
# wheel can't satisfy the other Python/OS splits that lock would resolve).
uv pip install pyarrow numpy

# --no-sync: the venv is already complete (turingdb + pyarrow + numpy); don't let
# uv re-sync against the lock and prune the pip-installed fixture deps.
uv run --no-sync main.py
testres=$?

turingdb stop -turing-dir $SCRIPT_DIR/.turing

exit $testres
