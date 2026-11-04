#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

# Kill any existing turingdb processes (SIGKILL for reliability)
pkill -9 turingdb 2>/dev/null || true
# Brief sleep to ensure process dies and port is released
sleep 0.5
# Wait for port 6666 to be free (nc -z returns 0 if open, 1 if closed)
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null || break; sleep 0.1; done

rm -rf $SCRIPT_DIR/.turing

# Run turingdb with commands piped to stdin:
# 1. Create graph and set up change
# 2. Checkout the change to enable writes
# 3. Use 'read' command to execute the multi-line cypher script
# 4. Submit the change
cat << 'EOF' | turingdb -turing-dir $SCRIPT_DIR/.turing 2>&1
CREATE GRAPH testgraph
cd testgraph
CHANGE NEW
checkout change-0
read test_script.cypher
CHANGE SUBMIT
quit
EOF

# Set PYTURINGDB if not already set (for running outside CMake)
if [ -z "$PYTURINGDB" ]; then
    export PYTURINGDB="turingdb"
fi

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
