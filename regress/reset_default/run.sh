#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "$SCRIPT_DIR"

pkill turingdb

# Set up an unloadable graph
rm -rf ~/.turing/graphs/default/
mkdir -p ~/.turing/graphs/default
echo "UNLOADABLE" >> ~/.turing/graphs/default/graph

turingdb -nodemon
startres=$?
if [ "$startres" -eq 0 ]; then # turingdb should fail to start
  echo "FAILURE: turingdb started with invalid 'default' graph"
  pkill turingdb
  exit 1
fi

turingdb -reset-default -nodemon
startres=$?
if [ "$startres" -ne 0 ]; then # turingdb should succeed in starting
  echo "FAILURE: turingdb failed to start whilst resetting invalid 'default' graph"
  exit 1
fi
pkill turingdb

# Have no default graph, without trying to reset it
rm -rf ~/.turing/graphs/default/
turingdb -nodemon
startres=$?
if [ "$startres" -ne 0 ]; then # turingdb should succeed in starting
  echo "FAILURE: turingdb failed to start with no 'default' graph dumped"
  exit 1
fi
pkill turingdb

# Have no default graph, and try to reset it
rm -rf ~/.turing/graphs/default/
turingdb -reset-default -nodemon
startres=$?
if [ "$startres" -ne 0 ]; then # turingdb should succeed in starting
  echo "FAILURE: turingdb failed to start with no 'default' graph dumped and attempting to reset it"
  exit 1
fi

pkill turingdb

echo "SUCCESS"
exit 0

