#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

SCRATCH_DIR=$SCRIPT_DIR/scratch
if [ -d $SCRATCH_DIR ]; then
    rm -rf $SCRATCH_DIR
fi

mkdir -p $SCRATCH_DIR

cd $SCRATCH_DIR

# Initialize uv repo
# Pin uv to the Python version the wheel was built for (encoded in the wheel
# filename, e.g. cp310); otherwise uv creates the venv with the newest
# interpreter available (e.g. 3.14), which has no ABI for a cp310 wheel.
wheel_base=$(basename "$PYTURINGDB")
if [[ "$wheel_base" =~ -cp([0-9])([0-9]+)- ]]; then
    export UV_PYTHON="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
fi

uv init --bare
uv add $PYTURINGDB

# Make sure turingdb is not running (SIGKILL for reliability)
pkill -9 turingdb 2>/dev/null || true
# Brief sleep to ensure process dies and port is released
sleep 0.5
# Wait for port 6666 to be free (nc -z returns 0 if open, 1 if closed)
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null || break; sleep 0.1; done
rm -rf $SCRIPT_DIR/.turing
uv run ../main.py
testres=$?

# Make sure turingdb was stopped at the end of the script
pkill -9 turingdb 2>/dev/null || true

exit $testres
