# Test Suite UI + API

This folder contains the Bun-based dev server and the web UI used to run and manage the query test suite.

## What it runs
- API server (`bun run server`) exposing `/api/*` endpoints for listing and running tests.
- UI dev server (`bun run dev`) for the browser frontend.

## Quick start
From this directory:

```bash
./start.sh
```

The script starts both the API server and the UI dev server. Stop with Ctrl+C.

## Script usage
```bash
./start.sh --help
```

- If the `start.sh` script cannot find the query test CLI, specify its path with `--cli-binary`.

```bash
./start.sh --cli-binary ../build/dev-bin/query_test_suite_cli
```

## Notes
- The API server expects the C++ test runner binary to be built and available as configured in `server.ts`.
- The UI (port 5555) will talk to the API server on the port defined in `server.ts` (default 5566).
- If the server is running on a remote machine, forward the UI dev server port:

```bash
ssh -NL 5555:localhost:5555 remote-machine
```

- If used through coder, access the UI using the 5555 forwarded port.
