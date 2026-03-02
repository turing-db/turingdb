# Visualizer Integration Specification

## Overview

Embed a lightweight proxy into TuringDB so that `turingdb -ui` serves the
[turingdb-visualizer](https://github.com/turing-db/turingdb-visualizer) web UI.

The visualizer frontend is hosted at a well-known URL on the internet (e.g.
`https://vis.turingdb.ai`).  When `-ui` is passed, TuringDB spawns a
small Express proxy that:

- Forwards all UI traffic (HTML, JS, CSS, assets) to the remote hosted frontend.
- Intercepts API requests (`/api/*`) and forwards them to the local TuringDB
  HTTP server.

No changes to the C++ HTTP server.  No Node.js bundling.  The proxy is a single
JavaScript file.

## Architecture

```
Browser ──► Express proxy (port 8080) ──┬──► Remote UI host (GET /*)
                                        │
                                        └──► localhost:6666 (POST /api/*)
                                             TuringDB HTTP server
```

The TuringDB HTTP server continues to handle POST-only API traffic on its port
as before.  The proxy is the single entry point for the browser.  It runs in a
child process managed by the `turingdb` binary.

### Why Express?

- The visualizer already uses `express` + `express-http-proxy` in its production
  `server.js`.  This is a proven pattern, not a new invention.
- A single ~30-line JS file.  No build step, no bundling.
- Avoids all C++ HTTP server changes (no GET support, no static serving, no
  content-type mapping).
- Node.js is the only runtime dependency; it is already ubiquitous on developer
  machines.

### Why not build proxy logic in C++?

- The C++ HTTP server is a custom low-level implementation (epoll, raw sockets,
  chunked transfer encoding).  Adding reverse-proxy support (upstream HTTP
  client, TLS, response streaming, header rewriting) would be a significant
  effort for no performance benefit — this is a developer tool, not a production
  gateway.

## CLI Changes

Two new flags in `TuringDBTool.cpp`:

```
-ui             Launch the web visualizer UI
-ui-host <url>  Override the remote UI host URL
                (default: https://vis.turingdb.ai)
```

When `-ui` is set:

1. Resolve the proxy script path: `$TURING_HOME/share/visualizer/proxy.mjs`.
2. Verify `node` is available on `$PATH`.  If not, log error and exit.
3. Start TuringDB server as usual (port from `-p`, default 6666).
4. Spawn `node proxy.mjs` as a child process with environment variables:
   - `TURING_API_PORT` — the TuringDB server port (from `-p`).
   - `TURING_API_HOST` — the TuringDB server address (from `-i`).
   - `TURING_UI_HOST`  — the remote frontend URL (from `-ui-host` or default).
   - `TURING_UI_PORT`  — the proxy listen port (API port + 1, i.e. 6667).
5. Log: `Visualizer UI available at http://localhost:<ui-port>/`
6. On shutdown (SIGINT/SIGTERM or shell exit), kill the proxy child process.

The `-ui` flag is orthogonal to all other flags (`-demon`, `-p`, `-i`, `-load`,
`-in-memory`, etc.).

## Proxy Script

`visualizer/proxy.mjs`:

```js
import express from "express";
import proxy from "express-http-proxy";

const apiHost = process.env.TURING_API_HOST || "127.0.0.1";
const apiPort = process.env.TURING_API_PORT || "6666";
const uiHost  = process.env.TURING_UI_HOST  || "https://vis.turingdb.ai";
const uiPort  = process.env.TURING_UI_PORT  || "6667";

const app = express();

// API requests → local TuringDB server
app.use("/api", proxy(`${apiHost}:${apiPort}`, {
    proxyReqPathResolver: (req) => req.url,
}));

// Everything else → remote UI host
app.use("/", proxy(uiHost));

app.listen(uiPort, "0.0.0.0", () => {
    console.log(`Visualizer proxy listening on port ${uiPort}`);
});
```

The proxy depends on two npm packages: `express` and `express-http-proxy`.
These are installed via `npm install` at build/package time and shipped in
`node_modules/` alongside `proxy.mjs`.

## Source Layout

```
visualizer/
├── proxy.mjs              # The proxy script
├── package.json           # express + express-http-proxy dependencies
├── package-lock.json      # Locked dependency versions
└── README.md              # How to update dependencies
```

`node_modules/` is not checked into the repo.  It is created at build time by
`npm install --production` (triggered from CMake) and installed alongside the
proxy script.

## Installation

### CMake

Add to root `CMakeLists.txt`:

```cmake
add_subdirectory(visualizer)
```

`visualizer/CMakeLists.txt`:

```cmake
# Install proxy script and package files
install(FILES proxy.mjs package.json package-lock.json
        DESTINATION share/visualizer)

# Run npm install at install time to populate node_modules
install(CODE "
    execute_process(
        COMMAND npm install --production --prefix \${CMAKE_INSTALL_PREFIX}/share/visualizer
        RESULT_VARIABLE npm_result)
    if(NOT npm_result EQUAL 0)
        message(WARNING \"npm install failed for visualizer proxy\")
    endif()
")
```

### Install Tree

After `make install`:

```
turing_install/
├── bin/turingdb
├── share/
│   └── visualizer/
│       ├── proxy.mjs
│       ├── package.json
│       ├── package-lock.json
│       └── node_modules/        # Created by npm install
│           ├── express/
│           └── express-http-proxy/
└── ...
```

## C++ Changes

### TuringDBTool.cpp

Add argument parsing:

```cpp
bool ui = false;
std::string uiHost("https://vis.turingdb.ai");

argParser.add_argument("-ui")
         .help("Launch the web visualizer UI")
         .store_into(ui);
argParser.add_argument("-ui-host")
         .metavar("url")
         .help("Remote UI host URL (default: https://vis.turingdb.ai)")
         .store_into(uiHost);
```

After `server.start()`, if `ui` is true:

```cpp
if (ui) {
    UIProxy uiProxy;
    uiProxy.setAPIPort(port);
    uiProxy.setAPIHost(address);
    uiProxy.setUIHost(uiHost);
    uiProxy.setUIPort(port + 1);
    uiProxy.start();  // Spawns node child process

    spdlog::info("Visualizer UI available at http://localhost:{}/", port + 1);
}
```

On shutdown, `UIProxy::stop()` kills the child process (SIGTERM).

### UIProxy.h / UIProxy.cpp (new files in `server/`)

A thin wrapper around `fork()`+`exec()`:

```cpp
class UIProxy {
public:
    void setAPIPort(uint32_t port);
    void setAPIHost(const std::string& host);
    void setUIHost(const std::string& host);
    void setUIPort(uint32_t port);

    void start();   // fork + exec node proxy.mjs
    void stop();    // kill child process

private:
    pid_t _pid {0};
    uint32_t _apiPort {6666};
    std::string _apiHost {"127.0.0.1"};
    std::string _uiHost {"https://vis.turingdb.ai"};
    uint32_t _uiPort {6667};
};
```

`start()` implementation:
1. Resolve proxy script path relative to binary (`../share/visualizer/proxy.mjs`).
2. Verify the file exists.
3. Set environment variables (`TURING_API_PORT`, etc.).
4. `fork()` + `execlp("node", "node", scriptPath, nullptr)`.
5. Store child `pid`.

`stop()` implementation:
1. If `_pid > 0`, send `SIGTERM` and `waitpid()`.

### server/CMakeLists.txt

Add `UIProxy.cpp` to sources.

## File-by-File Change Summary

| File | Change |
|------|--------|
| `visualizer/proxy.mjs` | New. Express proxy script (~30 lines). |
| `visualizer/package.json` | New. Declares express + express-http-proxy. |
| `visualizer/README.md` | New. Brief instructions. |
| `visualizer/CMakeLists.txt` | New. Install proxy + npm install at install time. |
| `CMakeLists.txt` | Add `add_subdirectory(visualizer)`. |
| `tools/turingdb/TuringDBTool.cpp` | Add `-ui` and `-ui-host` flags, spawn UIProxy. |
| `server/UIProxy.h` | New. Child process manager for the proxy. |
| `server/UIProxy.cpp` | New. fork/exec/kill implementation. |
| `server/CMakeLists.txt` | Add UIProxy.cpp to sources. |

## What This Does NOT Change

- The C++ HTTP server: no GET support, no static files, no new endpoints.
- The server's threading model, event loop, or request processing.
- Behavior when `-ui` is not passed — identical to today.
- The visualizer source code — it remains in its own repository.

## Runtime Requirements

- `node` (v18+) must be on `$PATH` when using `-ui`.
- Network access to the remote UI host.
- `npm` must be available at `make install` time (for `npm install`).

If `node` is not found, `turingdb -ui` prints a clear error and exits:
`"Error: 'node' is required for -ui but was not found on PATH"`.

## Open Questions

1. **Default UI host URL**: The spec uses `https://vis.turingdb.ai` as a
   placeholder.  The actual hosting location needs to be decided and the
   frontend deployed there.

2. **UI port selection**: Currently `api_port + 1`.  Could also use a fixed
   default (8080) or add a `-ui-port` flag.  Keeping it derived from `-p`
   avoids extra flags.

3. **CORS**: The proxy eliminates CORS issues entirely since the browser talks
   to a single origin.  No CORS headers needed on the TuringDB server.
