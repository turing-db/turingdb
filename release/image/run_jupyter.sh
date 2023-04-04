#!/bin/bash

set -e

source /turing/setup.sh

mkdir -p /home/dev/.jupyter/checkpoints

JUPYTERLAB_SETTINGS_DIR=/turing/jupyter/lab/user-settings /turing/pip/bin/jupyter-lab --no-browser --FileContentsManager.checkpoints_kwargs="root_dir"="/home/dev/.jupyter/checkpoints" --ServerApp.token='' --ServerApp.allow_remote_access=true --port=8088 --ip=127.0.0.1 > /tmp/jupyter.log 2>&1 &
