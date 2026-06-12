#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

# Kill any existing turingdb processes (SIGKILL for reliability)
pkill -9 turingdb 2>/dev/null || true
# Wait for port 6666 to be free (nc -z returns 0 if open, 1 if closed)
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null || break; sleep 0.1; done

rm -rf $SCRIPT_DIR/.turing

# Create simpledb graph in the test's turing directory
$TURING_HOME/samples/simpledb/simpledb -turing-dir $SCRIPT_DIR/.turing

# Replace the empty graph with the fully populated dump
rm -rf $SCRIPT_DIR/.turing/graphs/simpledb
cp -r $SCRIPT_DIR/simpledb.out/simpledb $SCRIPT_DIR/.turing/graphs/simpledb

turingdb -demon -turing-dir $SCRIPT_DIR/.turing

rm -f pyproject.toml
# Pin uv to the Python version the wheel was built for (encoded in the wheel
# filename, e.g. cp310); otherwise uv creates the venv with the newest
# interpreter available (e.g. 3.14), which has no ABI for a cp310 wheel.
wheel_base=$(basename "$PYTURINGDB")
if [[ "$wheel_base" =~ -cp([0-9])([0-9]+)- ]]; then
    export UV_PYTHON="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
fi

uv init
uv add $PYTURINGDB

uv run main.py
testres=$?

turingdb stop -turing-dir $SCRIPT_DIR/.turing

exit $testres
