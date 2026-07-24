#!/usr/bin/env bash

QUERY=$1
SAMPLE_NAME=$2

GRAPH_NAME="/home/cyrus/.turing/graphs/simpledb"
MAIN_REGEX='(?m)func\.func @main\(\) \{(\n|.)*\}'
SAMPLE_DIR="/home/cyrus/2turing/samples/mlir/"

DB_FILE="$SAMPLE_DIR/$SAMPLE_NAME.mlir"
NL_FILE="$SAMPLE_DIR/$SAMPLE_NAME.nl.mlir"

printf "//%s\n\n" " $QUERY" > "$DB_FILE"
printf "//%s\n\n" " $QUERY" > "$NL_FILE"

# add db dialect
./mlir -d -q "$QUERY" -g "$GRAPH_NAME" | awk '/func\.func @main\(\)/{found=1} found{print} /^\}/{found=0}' >> $DB_FILE

# add nl dialect
./mlir -l -q "$QUERY" -g "$GRAPH_NAME" | awk '/func\.func @main\(\)/{found=1} found{print} /^\}/{found=0}' >> $NL_FILE
