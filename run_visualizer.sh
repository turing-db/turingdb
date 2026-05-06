#!/bin/sh
set -e

serve -s "$TURINGDB_VIS_DIR" -l "tcp://127.0.0.1:$TURINGDB_VIS_PORT" >/tmp/turingdb-visualizer.log 2>&1 &

exec "$@"
