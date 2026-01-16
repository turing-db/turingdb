#!/bin/bash

set -e

python_version=${1-3.10}

uv python install $python_version

# Build wheel (this also configures cmake in build/)
uv build -o build/dist --wheel --python $python_version

# Reconfigure cmake with correct install prefix (scikit-build-core uses a temp directory)
# This regenerates scripts like run_regress.sh with correct paths
# Pass CMAKE_ARGS if set (used on macOS for compiler flags)
cmake -S . -B build -DCMAKE_INSTALL_PREFIX="$(pwd)/build/turing_install" $CMAKE_ARGS

# Complete build (build remaining targets like tests and samples) and install
make -C build -j8
make -C build install

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
