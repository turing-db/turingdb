# Stop Command — Specifications

This document describes the **design** of the `turingdb stop` command and the related
infrastructure required to support graceful shutdown.

---

## Overview

The design is built around two files placed inside the `turing-dir` at startup:

| File | Purpose |
|---|---|
| `{turing-dir}/turingdb.lock` | Exclusive lock — prevents two instances from sharing the same directory |
| `{turing-dir}/turingdb.sock` | Unix-domain socket — used by `turingdb stop` to send a `STOP` command |

A third path is used for logs:

| Path | Content |
|---|---|
| `{turing-dir}/logs/turingdb.log` | Combined console + file log output |

All three paths should be derived from the turing directory and exposed by `TuringConfig`.

---

## Key Classes

### `LockFile` (`system/LockFile.h`)

Should acquire an exclusive advisory lock on `turingdb.lock` using `flock(LOCK_EX | LOCK_NB)`.

- **`tryLock()`** — opens/creates the file, acquires the lock, writes the current PID, and
  returns a `LockFileResult<void>`. If the lock is already held, it should read the existing
  PID from the file and return an `ALREADY_LOCKED` error that includes the PID.
- **`unlock()`** — closes the file descriptor (releasing the `flock` lock) and deletes the
  file. Should be called automatically by the destructor.
- The lock file stores only one piece of data: the PID on a single line. No port, timestamp,
  or hostname is needed.

Error types should be defined in `LockFileErrorType`: `UNKNOWN`, `PERMISSION_DENIED`,
`ALREADY_LOCKED`, `NO_PID`.

`TuringDB::init()` should call `_lockFile.tryLock()` and panic if the lock cannot be acquired.

### `SystemEventHandler` (`system/SystemEventHandler.h`)

A singleton that runs a dedicated thread (`tdb.com`) responsible for:

1. **Signal handling** — `SIGINT` and `SIGTERM` should be caught via `sigaction`. The signal
   handler should write to a self-pipe; the event thread reads from that pipe. This avoids the
   restrictions of async-signal-safe code in the handler itself.

2. **Socket command handling** — listens on the Unix-domain socket at `turingdb.sock`.
   Should support two commands sent as plain strings:
   - `PING` → replies `PONG` (liveness probe)
   - `STOP` → replies `OK`, then calls the registered `_onStop` callback and exits the thread

When either a signal or a `STOP` command is received, the thread should call `_onStop()` and
exit.

**API:**
```cpp
SystemEventHandler::initialize(socketPath);  // create instance + start thread
SystemEventHandler::setOnStop(callback);     // set the shutdown callback
SystemEventHandler::terminate();             // stop thread + close fds (idempotent)
SystemEventHandler::requestStop(socketPath); // client side: connect + send "STOP"
SystemEventHandler::requestPing(socketPath); // client side: connect + send "PING"
```

**Note on signal handler constraints:** the signal pipe fds and socket fd must be stored in
file-scope globals because signal handlers cannot access instance state. This limits the design
to one `SystemEventHandler` per process, which should be enforced by a singleton guard in
`initialize()`.

### `TuringConfig` (`system/TuringConfig.h`)

Should expose the following paths, all derived from `setTuringDirectory()`:

```
logsDir      = {turing-dir}/logs
lockFilePath = {turing-dir}/turingdb.lock
socketPath   = {turing-dir}/turingdb.sock
```

Required options:
- **`useSystemEvents(bool)`** — when `false`, `TuringDB::init()` should skip initialising
  `SystemEventHandler`. Should be set to `false` in `TuringTestEnv` so unit tests do not
  register signal handlers or create socket files.
- **`setOnStopRequest(callback)`** / **`getOnStopRequest()`** — the callback invoked by
  `SystemEventHandler` when a stop event is received. Should be set by `StartCmd` to stop the
  active shell or server.

---

## Start Command (`tools/turingdb/StartCmd.cpp`)

Startup sequence:

1. Build `TuringConfig` (optionally override turing directory with `-turing-dir`).
2. Set up logging to `{turing-dir}/logs/turingdb.log` with **append mode** (not truncated),
   so log history is preserved across restarts.
3. Optionally daemonise.
4. Construct `TuringDB` and call `turingDB.init()`, which should:
   - Create all required directories (including `logs/`).
   - Acquire `turingdb.lock` (panic if another instance holds it).
   - Initialise `SystemEventHandler` on `turingdb.sock`.
5. Register `onStopRequest` callback: stop the active `TuringShell` (interactive mode) or
   call `TuringServer::stop()` (daemon mode).
6. Start `TuringServer`; enter the shell loop (interactive) or `server->wait()` (daemon).
7. On exit, `TuringDB::stop()` should call `SystemEventHandler::terminate()` — joining the
   event thread and closing all fds. The `LockFile` destructor should then release the lock
   and delete the file.

**Flags:**

| Flag | Description |
|---|---|
| `-turing-dir <path>` | Override root turing directory (default: `~/.turing`) |
| `-p <port>` | HTTP listen port (default: 6666) |
| `-i <addr>` | HTTP listen address (default: 127.0.0.1) |
| `-demon` | Daemonise the process |
| `-reset-default` | Delete the default graph before starting |
| `-load <graph>` | Load a named graph at startup (repeatable) |
| `-in-memory` | Disable on-disk persistence |

---

## Stop Command (`tools/turingdb/StopCmd.cpp`)

```
turingdb stop [-turing-dir <path>]
```

1. Build `TuringConfig` (optionally with `-turing-dir`).
2. Check whether `turingdb.sock` exists in the turing directory. If not, report that no
   instance appears to be running and exit with failure.
3. Call `SystemEventHandler::requestStop(socketPath)`:
   - Connect to the Unix-domain socket.
   - Send the string `"STOP"`.
   - Read `"OK"` back.
   - Return `true` on success.
4. Log success or failure to the console (console-only logging, no file).

**Flags:**

| Flag | Description |
|---|---|
| `-turing-dir <path>` | Which turing directory to stop (default: `~/.turing`) |

---

## Signal Handling

Signal handling should live entirely in `SystemEventHandler`, not in `TuringServer`.
`TuringServer` should not register any signal handlers.

Both `SIGINT` and `SIGTERM` should cause the event thread to invoke `_onStop`, which
propagates through the `onStopRequest` callback registered in `TuringConfig`.

---

## Logging

Logs should be written to `{turing-dir}/logs/turingdb.log` (file + console), opened in
**append mode** so log history is preserved across restarts.
`LogSetup::setupLogFileBacked()` should accept a `truncate` parameter (defaulting to `false`).

Logs should not be written to the current working directory.

---

## Testing

Unit tests should disable `SystemEventHandler` via `config.useSystemEvents(false)` inside
`TuringTestEnv` so they do not register signal handlers or create socket files.

A regression test in `regress/start-stop/` should cover:
1. Starting TuringDB in daemon mode (`-demon`) and waiting for the HTTP server to become
   reachable.
2. Stopping via signal (`pkill -2` / SIGINT) and verifying the port is released.
3. Repeating steps 1–2, then stopping via `turingdb stop` (socket command).

---

## Future Work

- **Stop timeout / force kill.** `turingdb stop` should optionally wait for the process to
  fully exit and issue `SIGKILL` if a configurable timeout is exceeded.

- **`-all` flag.** Stopping all running instances would require tracking per-instance metadata
  (e.g. in `/tmp`). A background job would be needed to maintain that directory in case the OS
  cleans it up.

- **`turingdb status` command.** The lock file could store additional fields (port, version)
  to allow a `status` command to report on a running instance without connecting to it.
