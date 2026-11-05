#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

pkill -9 turingdb 2>/dev/null || true
for i in $(seq 1 100); do nc -z localhost 6666 2>/dev/null || break; sleep 0.1; done

rm -rf $SCRIPT_DIR/.turing

OUTPUT=$(cat << 'EOF' | turingdb -turing-dir $SCRIPT_DIR/.turing -in-memory 2>&1
:param when => datetime('2026-11-05T14:05:00Z')
:param wait => duration({hours: 1})
:param later => $when + $wait
:param tags => ['a', 'b']
:param nothing => null
:param broken => 1 +
RETURN $later AS later, $nothing IS NULL AS nothing
SHOW PARAMETERS
EOF
)

echo "$OUTPUT"

fail=0

expect() {
    if echo "$OUTPUT" | grep -qE -- "$1"; then
        echo "ok: $1"
    else
        echo "MISSING: $1"
        fail=1
    fi
}

# The RETURN row, then the SHOW PARAMETERS rows: name, type, value
expect '^\| 2026-11-05T15:05:00Z +\| true +\|'
expect '^\| later +\| DateTime +\| 2026-11-05T15:05:00Z +\|'
expect '^\| nothing +\| Null +\| null +\|'
expect '^\| tags +\| List +\| \[a, b\] +\|'
expect '^\| wait +\| Duration +\| PT1H +\|'
expect '^\| when +\| DateTime +\| 2026-11-05T14:05:00Z +\|'
expect 'PARSE_ERROR'

if echo "$OUTPUT" | grep -qE '^\| broken +\|'; then
    echo "UNEXPECTED: the parameter of the broken expression was set"
    fail=1
fi

exit $fail
