# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TuringDB is a high-performance in-memory column-oriented graph database engine written in C++23. It uses OpenCypher as its query language and is designed for analytical and read-intensive workloads with millisecond query latency on large graphs.

## Build Commands

```bash
# Initial setup (run only once after cloning)
git clone --recursive https://github.com/turing-db/turingdb.git
./pull.sh
./dependencies.sh

# Build
mkdir -p build && cd build
cmake ..
make -j8
make install

# Setup environment (adds turingdb to PATH)
source setup.sh
```

**Build variants:**
- Debug build: `DEBUG_BUILD=1 cmake ..`
- Release with debug info: `RELDEBUG=1 cmake ..`
- Callgrind profiling: `CALLGRIND_PROFILE=1 cmake ..`

Always use make, do not use ninja.

## Testing

**Unit tests (GoogleTest):**
```bash
cd build
ctest                                    # Run all tests
ctest -R test_storage                    # Run tests matching pattern
./test/storage/test_storage_labelmap     # Run single test executable
./test/storage/test_storage_labelmap --gtest_filter=*TestName*  # Run specific test case
```

**Regression tests:**
```bash
cd build
make run_regress    # Run all regression tests
```

Regression tests are Python-based and located in `regress/`. Each test has a `run.sh` script.

## Architecture

### Query Processing Pipeline
1. **Parser** (`query/parser/`) - Cypher parser based on ANTLR g4 grammar
2. **AST** (`query/AST/`) - Abstract syntax tree representation
3. **Analyzer** (`query/analyzer/`) - Semantic analysis and validation
4. **Plan** (`query/plan/`) - Query plan generation
5. **Optimizer** (`query/optimizer/`) - Plan optimization
6. **Pipeline** (`query/pipeline/`) - Streaming query execution with processors

### Storage Layer (`storage/`)
- Column-oriented storage with DataParts (immutable data partitions)
- Supports git-like versioning with snapshot isolation
- Key components: Graph, NodeContainer, EdgeContainer, DataPart
- Subdirectories: columns, iterators, indexers, versioning, mergers

### Server (`server/`, `net/`)
- Single TCP listener with two response strategies selected at startup by `USE_TURING_PROTO`:
  - **REST (default):** in-tree HTTP/1.1 stack — `net/http_parser/HTTPParser`, `net/http_common/HTTPWriter`, dispatched by `server/DBServerProcessor`. JSON over HTTP, endpoints routed in `server/DBURIParser.h` / `server/Endpoints.h`. No third-party web framework.
  - **Native binary protocol (`USE_TURING_PROTO=1`):** TuringDB's binary proto framed as HTTP/1.1 chunked transfer — same `HTTPParser` reads the request, `net/turing_proto_server/TuringProtoWriter` emits one proto packet per HTTP chunk, `server/TuringProtoServerProcessor` handles dispatch. Client side: `net/turing_proto_client/TuringClient` (C++) / `python/turingdb/binary_client.py` (`BinaryClient`).
- Default port: 6666

### Key Modules
- `db/` - TuringDB main class
- `common/` - Shared utilities and external libraries (spdlog, nlohmann_json)
- `io/` - File I/O and S3 integration
- `import/` - Data import (JSONL, GML formats)
- `jobs/` - Job/task system
- `memory/` - Memory management
- `vector/` - Vector search with Faiss

## C++ Coding Style

Please read `CODING_STYLE.md` for guidelines before any new work.

Key points:
**Formatting:**
- 4 spaces indentation, no tabs
- Opening brace on same line (except constructors)
- All control structures (`if`, `for`, `while`) must have braces, even for single-statement bodies. Never use the brace-less form, including not on the same line as the condition (no `if (cond) return x;` or `if (cond) doThing();`).
- A sequence of conditionals must be written as an `if` / `else if` / `else if` chain, even when each branch returns. The presence of `return` does not dispense you from `else if`. Do not write multiple consecutive `if (...) { return ...; }` statements with no `else`.
- `public`/`private` aligned with `class`
- Do not overwrap: prefer keeping statements on one line when they fit. A slightly long line is better than an ugly split.
- Return type must be on the same line as the function name, never on a separate line — even if the line is long
- Function calls: first argument must start on the same line as the function name, never on the next line. If a call must wrap across lines, put **each argument on its own line**, all aligned under the first argument — never pack two-or-three-per-line.
- Use blank lines between logical groups of statements in function bodies; don't write overly compact code
- Switch cases: `return` directly inside each case body, and write `break;` after it aligned with `case` (one level out from the case body) for visual uniformity, even though unreachable. Don't capture per-case results into a local to return after the switch.

**Naming:**
- Private members prefixed with underscore: `_member`
- Methods use lowerCamelCase: `myFunction`
- No abbreviations in identifiers — spell them out (`rowGroup`, not `rg`; `column`, not `col`; `index`, not `idx`). Applies to locals, parameters, members, and comments.
- No Google-style `k`-prefix on constants. Use descriptive names: `previewRowsCount`, not `kPreviewRows`; `batchSize`, not `kBatch`.
- Helpers that unconditionally throw use a `throw`-prefix: `throwError`, `throwIfError` — not `raise`, `bail`, or `fail`. C++ uses `throw` as the language keyword, so the name should match.
- Area-local classes take a domain prefix matching their directory. Types under `io/parquet/`, `tools/turing-parquet/`, `dump/`, `import/`, etc. are written `ParquetReader`, `ParquetSchema`, `GraphLoader`, not unqualified `Reader`/`Schema`/`Loader`. Everything lives in `namespace db`, so unqualified names shadow generic ones and read ambiguously. Match file names to class names (`ParquetSchema.h` for `class ParquetSchema`).

**Includes order:**
1. Current class header (followed by blank line)
2. Standard library (`<stdlib.h>` style, not `<cstdlib>`)
3. External libraries
4. Project headers (outer to inner)
5. Utility headers (in own paragraph)

**Pointers and references:**
- Star/ampersand close to type: `MyType*`, `MyType&`
- Prefer raw pointers for arguments, references for STL containers
- Only use `std::unique_ptr` for ownership; no `std::shared_ptr`
- Don't return STL containers or strings; use output references

**Member variables:**
- POD/scalar members must always have a default value: `size_t _count {0};`
- Getter methods use `getX()` style: `getDimension()`, `getCapacity()`
- In the `private` section, member variables come before private member functions

**Headers vs implementation:**
- Keep headers declaration-only; put non-trivial implementations in `.cpp` files
- Only trivial one-liner getters should be inline in headers
- Destructors: declare `~ClassName();` in header, define in `.cpp` (not `= default` in header)
- In `.cpp` files, open with `using namespace db;` after the includes — don't wrap the body in `namespace db { ... }`.
- Helper placement in `.cpp` files — pick by relationship to the class:
  - Single-use, trivial → inline at the call site.
  - Operates on the class's own enums/types (e.g., `toString(MyEnum)` where the enum lives with the class) → `static` member of that class. Callers write `Class::toString(value)`, which keeps the surface cohesive and avoids namespace pollution.
  - Class-independent utility, only used in this `.cpp` (operates on third-party types or types from other classes) → anonymous namespace in the `.cpp`. Don't push it onto an unrelated class just to avoid the anonymous namespace — that pollutes the class's surface and falsely suggests a relationship to its state.
  - Reused across translation units → free function in a shared header.

**Error handling and return values:**
- Don't return large/non-trivial objects by value from factory methods; use fill/output reference patterns instead
- Throw `TuringException` on failure rather than returning error codes or optionals
- Use the layer-appropriate exception type: `FatalException` in `storage/` code (available via `common`), `PipelineException` in `query/pipeline/`. Storage cannot include pipeline headers (would create a circular dependency).
- In `main()`, return `EXIT_SUCCESS` / `EXIT_FAILURE` (from `<stdlib.h>`), not literal `0` / `1`.

**Other rules:**
- Never `using namespace` in headers
- Never `using namespace std`
- Never use `const_cast`. If you need a mutable reference, expose it through the type's API (e.g., add a non-const overload of the getter) rather than casting away const on an existing one. Reaching for `const_cast` is a sign that the type's API is wrong — fix the API instead.
- Use `const` extensively — all local variables should be `const` unless they need mutation
- **Never call the same getter repeatedly when the result can be reused.** Capture it once in a named local and use that everywhere. Writing `loop.getBody()` on several consecutive lines (`setInsertionPointToStart(loop.getBody())` then `loop.getBody()->getArgument(0)` then `loop.getBody()->getArgument(3)`) is bad style: it is noisy, hides that all the calls operate on the same object, and re-evaluates the call for nothing. Write `mlir::Block* loopBody = loop.getBody();` once and use `loopBody`. This applies to any getter or accessor chain, not just MLIR.
- When a conditional expression is long or compound, extract each part into a descriptively named `const bool` before the `if` statement
- Use `bioassert` for assertions (from BioAssert.h)
- Exceptions must derive from `TuringException`
- No move semantics/RVO; pass by pointer or reference
- Prefer `enum class` with trailing comma
- Prefer `size_t` for indices and counts in new code (row groups, columns, rows, batch sizes). Narrow with `static_cast<int>(...)` at third-party API boundaries inside the `.cpp`.
- Don't wrap unused parameter names in `/*comments*/` — just name them normally. `-Wunused-parameter` isn't enabled in this codebase, so there's no warning to silence.
- Don't add `(void)param;` lines in function bodies to mark a parameter as used. Same reason: no warning to silence; it's pure noise.

## Design Conventions

- Embedding operations (cosine_similarity, euclidean_distance) are implemented as functions through the EvalFunction path, not as binary operators. New computation operations on embeddings should follow the function pattern (Functions.h, EvalFunction, ColumnFunctions).
- **Extract-then-render for inspection tools.** When building tools that consume a streaming/SAX source (e.g. `ParquetSaxVisitor` driving `ParquetReader`), don't print or serialize directly inside the visitor callbacks. Define a high-level, source-independent data structure (e.g. `ParquetSchema` with `ParquetSchemaField`), have the visitor populate it, and render in a separate step. The high-level object stays decoupled from the source format and can be queried, transformed, or rendered multiple ways.
- **Visitor shared state lives in the caller.** For SAX-style visitors that produce or mutate a shared output object — including pipelines where one visitor extracts and a later visitor enriches (e.g., `ParquetSchemaExtractor` then `ParquetJsonDetector`) — construct the output in the caller and pass it to each visitor's constructor by reference. The visitor stores `T&` and mutates the shared object in callbacks; it does NOT own the object and does NOT expose a `getX()` accessor. The caller's flow then reads as one object flowing through a series of visitors.

## Build/iteration workflow

- During multi-turn iteration on a change (style feedback, API shape, refactors), **don't build after each edit**. Just write the edits and stop. Builds are slow and noisy; running them every micro-revision burns time. Only build when the user explicitly asks ("build" / "compile" / "run it") or signals the design is settled.
- Don't preface `make` with `cmake ..`. `make` already re-runs cmake when any tracked `CMakeLists.txt` has changed. Only run `cmake ..` after editing `CMakeLists.txt` / `dependencies.sh`, or when resetting the build directory.

## Project context

- **Partitioning** is on the near-term roadmap (as of 2026-05-04). Some customers have specifically asked for **METIS-style structural partitioning** (edge-cut minimization), not hash partitioning. Treat partitioning as a near-term constraint when discussing scale-out, not a future-optional.
- The **replication / distribution** initiative is motivated by *both* (a) scaling beyond single-node memory (which forces partitioning) and (b) write availability across network partitions / regions (which motivates CRDT-style merge). The realistic landing point is a layered hybrid; evaluate any proposal against both goals.

## CI / self-hosted runners

- macOS CI runs on a few **self-hosted minis that share one filesystem** (`/Users/m1`) across several runner instances (`actions-runner`, `actions-runner-2`, ...), with **no isolation** beyond each instance's own `_work` checkout. Linux self-hosted runners are containerized.
- The `external/dependencies` build is cached on local disk and **shared across those instances** (`scripts/dep_cache.sh`, base `/Users/m1/actions-local-cache`). The dependency build bakes **absolute paths into the installed CMake/pkg-config files** (e.g. faiss's `BLAS_LIBRARIES=…/external/dependencies/lib/libopenblas.a`, LLVM/MLIR configs) pointing into the *building* instance's checkout. Each instance `git clean -ffdx`s its own checkout between jobs, so a consumer on a *different* instance imports a path that a concurrent checkout deletes mid-build — surfacing as `make: No rule to make target '…/libopenblas.a'`, and downstream as a misleading `delocate … FileNotFoundError: dist/turingdb-*.whl`. `scripts/dep_cache.sh publish` rewrites those baked paths to the canonical, never-git-cleaned cache location. The bug is non-deterministic (depends on whether a concurrent job on the publishing instance cleans during your build window), so reproductions need overlapping runs. Bump the macOS dependency cache key when changing how deps are built/cached, or stale poisoned entries persist.
- Per-host serialization (port 6666, host-wide `pkill turingdb`, the shared dep cache) uses a kernel `flock` via `scripts/with_host_lock.py`, which the OS releases on process exit.

## Commit style

Do not include Claude as a co-author in the commits.

## PR style

PR descriptions are one short sentence. Do not use the default `## Summary` / `## Test plan` template — no headers, no bullets, no checklists, no "Generated with Claude Code" footer. The title carries the detail; the body just states the intent in a sentence.
