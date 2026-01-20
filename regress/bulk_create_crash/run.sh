#!/bin/bash
# Regression test: bulk_create_crash
#
# This test demonstrates a crash (segfault) when executing a large CREATE statement
# with many nodes and edges (roads.cypher: ~175K nodes, ~179K edges).
#
# The crash occurs in WriteProcessor::createNodes() when accessing property columns
# via dynamic_cast in getConstPropertyValue().
#
# Expected behavior: This test should FAIL (segfault) until the bug is fixed.
# Once fixed, the test should pass and create the graph successfully.

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROADS_FILE="$SCRIPT_DIR/roads.cypher"

cd $SCRIPT_DIR

# Kill any existing turingdb processes
killall -9 turingdb 2>/dev/null || true
sleep 0.5
# Wait for port 6666 to be free
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null || break; sleep 0.1; done

rm -rf $SCRIPT_DIR/.turing

echo "=== Bulk CREATE Crash Test ==="
echo "Loading roads.cypher (~175K nodes, ~179K edges)..."
echo ""

# Run turingdb with the large CREATE statement
# Using timeout to prevent infinite hangs
timeout 300 bash -c "cat << 'EOF' | turingdb -turing-dir $SCRIPT_DIR/.turing -in-memory 2>&1
CREATE GRAPH roads
cd roads
CHANGE NEW
checkout change-0
read $ROADS_FILE
CHANGE SUBMIT
MATCH (n:Intersection) RETURN COUNT(n) as node_count
MATCH ()-[r:ROAD]->() RETURN COUNT(r) as edge_count
quit
EOF"

exit_code=$?

echo ""
echo "Exit code: $exit_code"

# Check for segfault (128 + 11 = 139)
if [ $exit_code -eq 139 ]; then
    echo "SEGFAULT detected (exit code 139)"
    echo "Test demonstrates the bug - turingdb crashes on large CREATE statements"
    exit 1
fi

# Check for abort (128 + 6 = 134)
if [ $exit_code -eq 134 ]; then
    echo "ABORT detected (exit code 134)"
    echo "Test demonstrates the bug - turingdb aborts on large CREATE statements"
    exit 1
fi

# Check for timeout
if [ $exit_code -eq 124 ]; then
    echo "TIMEOUT - turingdb took too long"
    exit 1
fi

# Check for other errors
if [ $exit_code -ne 0 ]; then
    echo "ERROR - turingdb exited with code $exit_code"
    exit 1
fi

echo "SUCCESS - Large CREATE statement executed without crash"
exit 0
