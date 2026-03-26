#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd "$SCRIPT_DIR"

pkill -9 turingdb 2>/dev/null || true
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null || break; sleep 0.1; done

rm -rf "$SCRIPT_DIR/.turing"
turingdb -demon -turing-dir "$SCRIPT_DIR/.turing"

rm -f pyproject.toml
uv init
uv add "$PYTURINGDB"

uv run main.py
testres=$?

turingdb stop -turing-dir "$SCRIPT_DIR/.turing"

exit $testres
