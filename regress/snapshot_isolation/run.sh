#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

pkill -f "[t]uringdb" 2>/dev/null || true
# Wait for port 6666 to be free
for i in $(seq 1 100); do lsof -i :6666 -sTCP:LISTEN >/dev/null 2>&1 || break; sleep 0.1; done

rm -rf $SCRIPT_DIR/.turing
turingdb -demon -turing-dir $SCRIPT_DIR/.turing

rm -f pyproject.toml
uv init
uv add $PYTURINGDB

uv run main.py
testres=$?

pkill -f "[t]uringdb"

exit $testres

