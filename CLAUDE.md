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
- HTTP-based REST API using Crow framework
- Default port: 6666

### Key Modules
- `db/` - TuringDB main class
- `common/` - Shared utilities and external libraries (spdlog, nlohmann_json, crow)
- `io/` - File I/O and S3 integration
- `import/` - Data import (Neo4j, GML formats)
- `jobs/` - Job/task system
- `memory/` - Memory management
- `vector/` - Vector search with Faiss

## C++ Coding Style

Please read `CODING_STYLE.md` for guidelines before any new work.

Key points:
**Formatting:**
- 90 char max line length (ideal ~80)
- 4 spaces indentation, no tabs
- Opening brace on same line (except constructors)
- `public`/`private` aligned with `class`

**Naming:**
- Private members prefixed with underscore: `_member`
- Methods use lowerCamelCase: `myFunction`

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

**Other rules:**
- Never `using namespace` in headers
- Never `using namespace std`
- Use `const` extensively
- Use `bioassert` for assertions (from BioAssert.h)
- Exceptions must derive from `TuringException`
- No move semantics/RVO; pass by pointer or reference
- Prefer `enum class` with trailing comma

## Commit style

Do not include Claude as a co-author in the commits.
