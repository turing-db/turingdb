#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

rm -f pyproject.toml
uv init
uv add turingdb

uv run create_turingdb.py
