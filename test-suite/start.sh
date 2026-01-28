#!/usr/bin/env bash
set -euo pipefail

show_help() {
  cat <<'HELP'
Usage: ./start.sh [options]

Starts the test-suite Bun API server and the UI dev server.

Options:
  -h, --help    Show this help and exit.
HELP
}

case "${1-}" in
  -h|--help)
    show_help
    exit 0
    ;;
  "")
    ;;
  *)
    echo "Unknown option: $1" >&2
    show_help
    exit 1
    ;;
esac

bun run server &
server_pid=$!
sleep 0.5  # Give it a moment to fail if it's going to
if ! kill -0 "$server_pid" 2>/dev/null; then
  echo "Error: server failed to start" >&2
  exit 1
fi

cleanup() {
  kill "$server_pid" 2>/dev/null || true
}
trap cleanup EXIT

bun run dev
