#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

pkill turingdb
turingdb

rm -f pyproject.toml
uv init
uv add turingdb

uv run main.py
testres=$?

pkill turingdb

exit $testres
