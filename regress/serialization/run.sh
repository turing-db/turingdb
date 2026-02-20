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

# Make sure turingdb is not running (SIGKILL for reliability)
echo '  ! Ensuring no turingdb process is running'
pkill -9 turingdb 2>/dev/null || true

echo '  ! Waiting for port 6666 to be free'
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null || break; sleep 0.1; done

echo "  ! Clearing turingdb directory '$SCRIPT_DIR/.turing'"
if [ -d "$SCRIPT_DIR/.turing" ]; then
        rm -rf $SCRIPT_DIR/.turing
fi

echo '  ! Running serialization regression test'
uv run ../main.py

testres=$?

echo '  ! Ensuring no turingdb process is running before exiting'
# Make sure turingdb was stopped at the end of the script
pkill -9 turingdb 2>/dev/null || true

exit $testres
