#!/bin/bash

git pull
git lfs pull
git submodule update --init
git submodule foreach '
    if [ "$name" != "external/AFLplusplus" ]; then
        git submodule update --init --recursive
    fi
'
