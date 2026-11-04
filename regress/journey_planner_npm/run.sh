#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

if ! command -v npm > /dev/null; then
    echo "journey_planner_npm needs node and npm on PATH" >&2
    exit 1
fi

# The npm package built from this tree, as the other tests take the wheel: TURINGDB_NPM, or
# the one ./build_npm.sh leaves in $SOURCE_DIR/npm
if [[ -z "$TURINGDB_NPM" && -n "$SOURCE_DIR" ]]; then
    TURINGDB_NPM=$(ls "$SOURCE_DIR"/npm/turingdb-*.tgz 2>/dev/null | head -1)
fi
if [[ -z "$TURINGDB_NPM" || ! -f "$TURINGDB_NPM" ]]; then
    echo "journey_planner_npm needs the npm package: build it with ./build_npm.sh or set TURINGDB_NPM" >&2
    exit 1
fi
echo "- Using turingdb npm package: $TURINGDB_NPM"

# Node's fetch refuses the default port, 6666
PORT=6686
# A second server checks the token every request carries
AUTH_PORT=6687
AUTH_TOKEN=journey-planner-token

# Kill any existing turingdb processes (SIGKILL for reliability)
pkill -9 turingdb 2>/dev/null || true
# Wait for the ports to be free (nc -z returns 0 if open, 1 if closed)
for port in $PORT $AUTH_PORT; do
    for i in $(seq 1 100); do nc -z localhost $port 2>/dev/null || break; sleep 0.1; done
done

rm -rf $SCRIPT_DIR/.turing $SCRIPT_DIR/.turing-auth $SCRIPT_DIR/node_modules $SCRIPT_DIR/dist

turingdb -demon -turing-dir $SCRIPT_DIR/.turing -p $PORT
TURINGDB_AUTH_TOKEN=$AUTH_TOKEN turingdb -demon -turing-dir $SCRIPT_DIR/.turing-auth -p $AUTH_PORT -auth-on

npm install --no-audit --no-fund --no-package-lock --no-save "$TURINGDB_NPM"

npx tsc -p . && TURINGDB_URL=http://localhost:$PORT/query \
    TURINGDB_AUTH_URL=http://localhost:$AUTH_PORT/query \
    TURINGDB_AUTH_TOKEN=$AUTH_TOKEN \
    node dist/main.js
testres=$?

turingdb stop -turing-dir $SCRIPT_DIR/.turing
turingdb stop -turing-dir $SCRIPT_DIR/.turing-auth

exit $testres
