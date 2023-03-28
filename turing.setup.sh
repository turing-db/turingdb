#!/bin/bash

if [[ -z "$TURING_HOME" ]] ; then
    echo "ERROR: please set TURING_HOME."
    exit 1
fi

export PATH=$TURING_HOME/bin:$PATH
export PYTHONPATH=$TURING_HOME/lib/python:$PYTHONPATH
