#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
SCRATCH_DIR=$SCRIPT_DIR/scratch
if [ -d $SCRATCH_DIR ]; then
    rm -rf $SCRATCH_DIR
fi

mkdir -p $SCRATCH_DIR

cd $SCRATCH_DIR

# Initialize uv repo
uv init --bare
uv add $PYTURINGDB

# Make sure turingdb is not running
pkill -f "[t]uringdb" 2>/dev/null || true
# Wait for port 6666 to be free
for i in $(seq 1 100); do lsof -i :6666 -sTCP:LISTEN >/dev/null 2>&1 || break; sleep 0.1; done
rm -rf $SCRIPT_DIR/.turing
uv run ../main.py
testres=$?

# Make sure turingdb was stopped at the end of the script
pkill -f "[t]uringdb"

exit $testres
