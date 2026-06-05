#!/bin/bash

# for CI runner, just exit success
if ! command -v nix-shell &>/dev/null; then
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$SCRIPT_DIR/vardeps" "$@" \
    | awk '/^flowchart/,0' \
    | nix-shell "$SCRIPT_DIR/shell.nix" --run "python3 $SCRIPT_DIR/asciimermaid.py"
echo ""
