#!/bin/bash

set -e

out_dir=$1

npm install
BUILD_PATH=$out_dir/build npm run build
