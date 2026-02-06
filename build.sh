#!/bin/bash

set -e

DEFAULT_PYTHON_VERSION=3.14
PYTHON_VERSION=${PYTHON_VERSION:-"${DEFAULT_PYTHON_VERSION}"}

# Python flag configuration
UV_PYTHON_FLAG="--python $PYTHON_VERSION"

# Source general setup script for $PATH
source setup.sh

# Setup toolchain variables for macos
if [[ "$(uname)" == "Darwin" ]]; then
    source external/dependencies/macos_setenv.sh
fi

# Build
uv build --wheel $UV_PYTHON_FLAG
cd build && make install && cd ..

if [[ "$(uname)" == "Darwin" ]]; then
    uv add delocate
    uvx --from delocate delocate-wheel -v -w wheel dist/turingdb-*.whl
else
    uv add auditwheel
    uvx auditwheel repair dist/*.whl -w wheel
fi
