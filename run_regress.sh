#!/bin/bash

set -e

# Source general setup script for $PATH
source setup.sh

# Setup toolchain variables for macos
if [[ "$(uname)" == "Darwin" ]]; then
    source external/dependencies/macos_setenv.sh
fi

cd build
cmake ..
make install
make run_regress
