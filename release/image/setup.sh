#!/bin/bash

export PATH=/turing/tools/bin:/turing/pip/bin:$PATH
export LD_LIBRARY_PATH=$TURING/tools/lib:$LD_LIBRARY_PATH
export PYTHONUSERBASE=/turing/pip
export JUPYTERLAB_SETTINGS_DIR=/turing/jupyter/lab/user-settings

source /turing/turing.setup.sh
