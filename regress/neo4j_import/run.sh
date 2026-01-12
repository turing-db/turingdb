#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

pkill turingdb
rm -rf $SCRIPT_DIR/.turing

# Import neo4j cyber-security-db using turing-import
turing-import -neo4j-json $TURING_HOME/neo4j/cyber-security-db -db-path $SCRIPT_DIR/.turing/graphs

# Create turing directory structure
mkdir -p $SCRIPT_DIR/.turing/graphs

# Rename the imported graph to cyber_security
mv $SCRIPT_DIR/.turing/graphs/bindump/cyber-security-db $SCRIPT_DIR/.turing/graphs/cyber_security

turingdb -demon -turing-dir $SCRIPT_DIR/.turing

rm -f pyproject.toml
uv init
uv add $PYTURINGDB

uv run main.py
testres=$?

pkill turingdb

exit $testres
