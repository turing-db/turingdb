#!/bin/bash
# Regression test: get_nodes_invalid_id
#
# This test demonstrates a crash (segfault) when requesting a non-existent node ID
# via the /get_nodes endpoint.
#
#
# Expected behavior: This test should FAIL (server crash) until the bug is fixed.
# Once fixed, the test should pass with a proper error response or empty result.

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

# Kill any existing turingdb processes (SIGKILL for reliability)
killall -9 turingdb 2>/dev/null || true
# Brief sleep to ensure process dies and port is released
sleep 0.5
# Wait for port 6666 to be free (nc -z returns 0 if open, 1 if closed)
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null || break; sleep 0.1; done

rm -rf $SCRIPT_DIR/.turing
turingdb -demon -turing-dir $SCRIPT_DIR/.turing
# Wait for turingdb to be ready
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null && break; sleep 0.1; done

rm -f pyproject.toml
uv init
# PYTURINGDB is set by the regression test runner, fallback to "turingdb" if not set
uv add ${PYTURINGDB:-turingdb}
uv add requests

uv run main.py
testres=$?

killall -9 turingdb 2>/dev/null || true

exit $testres