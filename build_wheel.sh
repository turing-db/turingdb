#!/bin/bash

set -e

python_version=${1-3.10}

uv python install $python_version

# Build wheel
uv build -o build/dist --wheel --python $python_version

# Repair wheel
pip install auditwheel
auditwheel repair -w build/wheelhouse build/dist/*

# Test package installation
rm -rf build/wheeltest && mkdir build/wheeltest
cd build/wheeltest && uv venv test_venv --python $python_version && source test_venv/bin/activate && uv pip install ../wheelhouse/*.whl && uv run turingdb --help
