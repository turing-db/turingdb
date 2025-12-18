#!/usr/bin/env bash

set -e

if [ $# -lt 1 ]; then
    echo "Usage: $0 <test_name> [python-sdk-branch]"
    echo "    Examples:"
    echo "        $0 call_metadata"
    echo "        $0 call_metadata feature/cool-feature"
    exit 1
fi

TEST_NAME=$1
PYTHON_SDK_BRANCH=${2:-main}
[ $1 ] && PYTURINGDB="turingdb @ git+https://github.com/turing-db/turingdb-sdk-python@$PYTHON_SDK_BRANCH" || PYTURINGDB=turingdb

echo "- Running test $TEST_NAME with python-sdk branch $PYTHON_SDK_BRANCH"
DIR_NAME=$(realpath $(dirname $0))

cd $DIR_NAME

if [ ! -d $TEST_NAME ]; then
    echo "! Test $TEST_NAME does not exist"
    exit 1
fi

cd $TEST_NAME

echo "- PyTester path: $DIR_NAME/pytester"
uv run \
    --with "$PYTURINGDB" \
    --project $DIR_NAME/pytester \
    ./main.py
echo "Done"

