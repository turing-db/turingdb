#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

pkill -f "[t]uringdb" 2>/dev/null || true
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

# Now start daemon to verify results via Python SDK
turingdb -demon -turing-dir $SCRIPT_DIR/.turing
# Wait for turingdb to be ready
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null && break; sleep 0.1; done

# Setup Python environment and verify results
rm -f pyproject.toml
uv init

# Set PYTURINGDB if not already set (for running outside CMake)
if [ -z "$PYTURINGDB" ]; then
    export PYTURINGDB="turingdb"
fi

uv add $PYTURINGDB

uv run main.py
testres=$?

pkill -f "[t]uringdb"

exit $testres
