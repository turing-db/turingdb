#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "$SCRIPT_DIR"

rm -f pyproject.toml
# Pin uv to the Python version the wheel was built for (encoded in the wheel
# filename, e.g. cp310); otherwise uv creates the venv with the newest
# interpreter available (e.g. 3.14), which has no ABI for a cp310 wheel.
wheel_base=$(basename "$PYTURINGDB")
if [[ "$wheel_base" =~ -cp([0-9])([0-9]+)- ]]; then
    export UV_PYTHON="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
fi

uv init
uv add $PYTURINGDB

uv run main.py
