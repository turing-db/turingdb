#!/bin/bash
# Regression test: json_escape_properties (GitHub issue #592)
#
# Verifies that /get_nodes returns valid JSON when node properties contain
# special characters: single quotes, double quotes, backslashes.
#
# Expected: This test FAILS until the JSON serializer properly escapes strings.

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

# Kill any existing turingdb processes (SIGKILL for reliability)
pkill -9 turingdb 2>/dev/null || true
sleep 0.5
# Wait for port 6666 to be free (nc -z returns 0 if open, 1 if closed)
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null || break; sleep 0.1; done

rm -rf $SCRIPT_DIR/.turing
turingdb -demon -turing-dir $SCRIPT_DIR/.turing

# Wait for daemon to be ready
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null && break; sleep 0.1; done

rm -f pyproject.toml
uv init
uv add $PYTURINGDB
uv add httpx

uv run main.py
testres=$?

turingdb stop -turing-dir $SCRIPT_DIR/.turing

exit $testres
