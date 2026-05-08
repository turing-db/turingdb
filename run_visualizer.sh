#!/bin/sh
set -e

turingdb start -demon -p 6666 -start-timeout 30000

serve -s "$TURINGDB_VIS_DIR" -l "tcp://127.0.0.1:$TURINGDB_VIS_PORT" >/tmp/turingdb-visualizer.log 2>&1 &

exec "$@"
