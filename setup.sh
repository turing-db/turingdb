#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

export TURING_HOME=$SCRIPT_DIR/build/turing_install
export TURING_SRC=$SCRIPT_DIR

export PATH=$TURING_HOME/bin:$PATH

# emcmake, for the wasm decoder. dependencies.sh installs the toolchain but cannot put it
# on PATH: it runs as a child process and its exports die with it.
EMSDK_ENV=$SCRIPT_DIR/external/dependencies/emsdk/emsdk_env.sh
if [[ -f "$EMSDK_ENV" ]]; then
    EMSDK_QUIET=1 source "$EMSDK_ENV"
    unset EMSDK_QUIET
fi
