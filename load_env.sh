#!/bin/bash

# Simplest possible load_env script
set -a
source "${1:-.env}"
set +a

echo "Environment variables loaded from ${1:-.env}"
