#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

pkill -f "[t]uringdb" 2>/dev/null || true
# Wait for port 6666 to be free
for i in $(seq 1 100); do lsof -i :6666 -sTCP:LISTEN >/dev/null 2>&1 || break; sleep 0.1; done

rm -rf $SCRIPT_DIR/.turing

# Create simpledb graph in the test's turing directory
$TURING_HOME/samples/simpledb/simpledb -turing-dir $SCRIPT_DIR/.turing

# Replace the empty graph with the fully populated dump
rm -rf $SCRIPT_DIR/.turing/graphs/simpledb
cp -r $SCRIPT_DIR/simpledb.out/simpledb $SCRIPT_DIR/.turing/graphs/simpledb

turingdb -demon -turing-dir $SCRIPT_DIR/.turing

rm -f pyproject.toml
uv init
uv add $PYTURINGDB

uv run main.py
testres=$?

pkill -f "[t]uringdb"

exit $testres
