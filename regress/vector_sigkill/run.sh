#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

SCRATCH_DIR=$SCRIPT_DIR/scratch
if [ -d $SCRATCH_DIR ]; then
    rm -rf $SCRATCH_DIR
fi

mkdir -p $SCRATCH_DIR

cd $SCRATCH_DIR


wheel_base=$(basename "$PYTURINGDB")
if [[ "$wheel_base" =~ -cp([0-9])([0-9]+)- ]]; then
    export UV_PYTHON="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
fi

uv init --bare
uv add $PYTURINGDB

pkill -9 turingdb 2>/dev/null || true
sleep 0.5
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null || break; sleep 0.1; done
rm -rf $SCRIPT_DIR/.turing
uv run ../main.py
testres=$?

pkill -9 turingdb 2>/dev/null || true

exit $testres
