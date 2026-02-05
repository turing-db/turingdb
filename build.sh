#!/bin/bash

source setup.sh
uv build --wheel

if [[ "$(uname)" == "Darwin" ]]; then
    uv add delocate
    uvx --from delocate delocate-wheel -v -w wheel dist/turingdb-*.whl
else
    uv add auditwheel
    uvx auditwheel repair dist/*.whl -w wheel
fi
