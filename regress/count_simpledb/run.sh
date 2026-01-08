#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

pkill turingdb
rm -rf $SCRIPT_DIR/.turing

# Create simpledb graph in the test's turing directory
$TURING_HOME/samples/simpledb/simpledb -turing-dir $SCRIPT_DIR/.turing

turingdb -demon -turing-dir $SCRIPT_DIR/.turing

rm -f pyproject.toml
uv init
uv add $PYTURINGDB

uv run main.py
testres=$?

pkill turingdb

exit $testres
