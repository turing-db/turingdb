#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
SCRATCH_DIR=$SCRIPT_DIR/scratch
if [ -d $SCRATCH_DIR ]; then
    rm -rf $SCRATCH_DIR
fi

mkdir -p $SCRATCH_DIR

cd $SCRATCH_DIR
pwd

pkill turingdb
turingdb -turing-dir $SCRIPT_DIR/.turing

uv init --bare
uv add turingdb

uv run ../main.py
testres=$?

pkill turingdb

exit $testres
