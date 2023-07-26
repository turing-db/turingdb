#!/bin/bash

set -e

out_dir=$1

npm install --legacy-peer-deps
BUILD_PATH=$out_dir npm run build
