#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

pkill turingdb
turingdb -turing-dir $SCRIPT_DIR/.turing

rm -f pyproject.toml
rm -f uv.lock
uv init
uv add turingdb

uv run create_turingdb.py
testres=$?

pkill turingdb
rm -rf .turing

exit $testres
