#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "$SCRIPT_DIR"
SCRIPT_GRAPHS_DIR="$SCRIPT_DIR/.turing/graphs"

rm -f pyproject.toml
uv init
uv add turingdb

pkill turingdb

# Set up an unloadable graph
rm -rf "$SCRIPT_GRAPHS_DIR/default/"
mkdir -p "$SCRIPT_GRAPHS_DIR/default"
echo "UNLOADABLE" >> "$SCRIPT_GRAPHS_DIR/default/graph"

turingdb -turing-dir $SCRIPT_DIR/.turing &> /dev/null
uv run check_db_status.py &> /dev/null
startres=$?
if [ "$startres" -eq 0 ]; then # turingdb should fail to start
  echo "FAILURE: turingdb started with invalid 'default' graph"
  pkill turingdb
  exit 1
else
  echo "SUCCESS: turingdb failed to start with invalid 'default' graph"
fi

turingdb -turing-dir $SCRIPT_DIR/.turing -reset-default &> /dev/null
uv run check_db_status.py &> /dev/null
startres=$?
if [ "$startres" -ne 0 ]; then # turingdb should succeed in starting
  echo "FAILURE: turingdb failed to start whilst resetting invalid 'default' graph"
  exit 1
else
  echo "SUCCESS: turingdb started whilst resetting invalid 'default' graph"
  pkill turingdb
fi

# Have no default graph, without trying to reset it
rm -rf ~/.turing/graphs/default/
turingdb -turing-dir $SCRIPT_DIR/.turing &> /dev/null
uv run check_db_status.py &> /dev/null
startres=$?
if [ "$startres" -ne 0 ]; then # turingdb should succeed in starting
  echo "FAILURE: turingdb failed to start with no 'default' graph dumped"
  exit 1
else
    echo "SUCCESS: turingdb started with no 'default' graph dumped"
    pkill turingdb
fi

# Have no default graph, and try to reset it
rm -rf ~/.turing/graphs/default/
turingdb -turing-dir $SCRIPT_DIR/.turing -reset-default &> /dev/null
uv run check_db_status.py &> /dev/null
startres=$?
if [ "$startres" -ne 0 ]; then # turingdb should succeed in starting
  echo "FAILURE: turingdb failed to start with no 'default' graph dumped and attempting to reset it"
  exit 1
else
  echo "SUCCESS: turingdb started with no 'default' graph dumped and attempting to reset it"
  pkill turingdb
fi

echo "ALL TESTS PASSED"
exit 0

