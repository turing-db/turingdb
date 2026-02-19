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

# Create a minimal graph so the server has a context for queries
cat << 'EOF' | turingdb -turing-dir $SCRIPT_DIR/.turing 2>&1
CREATE GRAPH csvtest
cd csvtest
CHANGE NEW
checkout change-0
CREATE (n:Dummy {val: 1})
CHANGE SUBMIT
quit
EOF

# Now start daemon to run LOAD CSV tests via Python SDK
turingdb -demon -turing-dir $SCRIPT_DIR/.turing
# Wait for turingdb to be ready
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null && break; sleep 0.1; done

# Setup Python environment
rm -f pyproject.toml
uv init

# Set PYTURINGDB if not already set (for running outside CMake)
if [ -z "$PYTURINGDB" ]; then
    export PYTURINGDB="turingdb"
fi

uv add $PYTURINGDB

uv run main.py
testres=$?

pkill -9 turingdb 2>/dev/null || true

exit $testres
