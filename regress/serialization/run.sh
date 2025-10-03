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
uv add turingdb

# Make sure turingdb is not running
pkill turingdb
uv run ../main.py
testres=$?

# Make sure turingdb was stopped at the end of the script
pkill turingdb

exit $testres
