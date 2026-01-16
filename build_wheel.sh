#!/bin/bash

set -e

python_version=${1-3.10}

uv python install $python_version

# Build wheel (this also configures cmake in build/)
uv build -o build/dist --wheel --python $python_version

# Complete build (build remaining targets like tests and samples) and install
make -C build -j8
# Use explicit prefix since scikit-build-core overrides CMAKE_INSTALL_PREFIX for wheel staging
cmake --install build --prefix build/turing_install

# Repair wheel (Linux only - auditwheel doesn't support macOS)
if [[ "$(uname)" == "Linux" ]]; then
    pip install auditwheel
    auditwheel repair -w build/wheelhouse build/dist/*
    wheel_dir="build/wheelhouse"
else
    wheel_dir="build/dist"
fi

# Test package installation
rm -rf build/wheeltest && mkdir build/wheeltest
cd build/wheeltest && uv venv test_venv --python $python_version && source test_venv/bin/activate && uv pip install ../${wheel_dir#build/}/*.whl && uv run turingdb --help
