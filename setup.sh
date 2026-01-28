#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

export TURING_HOME=$SCRIPT_DIR/build/turing_install
export TURING_SRC=$SCRIPT_DIR

export PATH=$TURING_HOME/bin:$PATH
