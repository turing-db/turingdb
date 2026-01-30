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

# Create simpledb graph in the test's turing directory
$TURING_HOME/samples/simpledb/simpledb -turing-dir $SCRIPT_DIR/.turing

# Replace the empty graph with the fully populated dump
rm -rf $SCRIPT_DIR/.turing/graphs/simpledb
cp -r $SCRIPT_DIR/simpledb.out/simpledb $SCRIPT_DIR/.turing/graphs/simpledb

turingdb -demon -turing-dir $SCRIPT_DIR/.turing
# Wait for turingdb to be ready
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null && break; sleep 0.1; done

rm -f pyproject.toml
uv init
uv add $PYTURINGDB

uv run main.py
testres=$?

pkill -9 turingdb 2>/dev/null || true

exit $testres
