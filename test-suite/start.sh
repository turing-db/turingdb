#!/usr/bin/env bash
set -euo pipefail

show_help() {
  cat <<'HELP'
Usage: ./start.sh [options]

Starts the test-suite Bun API server and the UI dev server.

Options:
  -h, --help               Show this help and exit.
  -b, --cli-binary <path>  Path to the test-suite CLI binary.
                           If not provided, defaults to
                           $TURING_SRC/build/build_package/dev-bin/query_test_suite_cli
HELP
}

if [ -z "${TURING_SRC-}" ]; then
  echo "Error: TURING_SRC environment variable is not set. Please source setup.sh" >&2
  exit 1
fi

cli_binary="${TURING_SRC}/build/build_package/dev-bin/query_test_suite_cli"
current_dir="$(pwd)"
script_dir="$(cd "$(dirname "$0")" && pwd)"

case "${1-}" in
  -h|--help)
    show_help
    exit 0
    ;;
  -b|--cli-binary)
    cli_binary="$current_dir/$2"
    shift 2
    ;;
  "")
    ;;
  *)
    echo "Unknown option: $1" >&2
    show_help
    exit 1
    ;;
esac

# Install dependencies
bun i --cwd="$script_dir/backend"
bun i --cwd="$script_dir/frontend"

(
  cd "$(dirname "$0")/backend"
  bun run server --cli-binary "$cli_binary"
) &
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

(
  cd "$(dirname "$0")/frontend"
  bun run dev
)
