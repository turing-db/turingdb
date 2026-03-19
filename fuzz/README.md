# TuringDB Fuzzing

AFL++ fuzzing harnesses for finding crashes and bugs in TuringDB.

## Quick Start

```bash
# Install AFL++
sudo apt-get install -y afl++

# Build and run all harnesses (5 min each)
./fuzz/run_afl.sh

# Fuzz only the query engine, forever
./fuzz/run_afl.sh --nostop --cypher

# Fuzz only the HTTP parser, 10 minutes
./fuzz/run_afl.sh --http --time 600
```

## Harnesses

| Flag | Harness | What it tests |
|------|---------|---------------|
| `--cypher` | `fuzz_query_engine` | Full query pipeline: parse, analyze, plan, optimize, execute against SimpleGraph data |
| `--http` | `fuzz_http_parser` | Custom HTTP parser: method, URI, headers, Content-Length, payload handling |
| `--csv` | `fuzz_csv_parser` | CSV parser: `parseCSVLine()` and `peekFileStructure()` |
| `--gml` | `fuzz_gml_importer` | GML importer: `importContent()` with arbitrary GML data |

If no flag is specified, all harnesses are run.

## Options

```
--time SECS    Time per harness in timed mode (default: 300)
--nostop       Run forever until Ctrl+C (no restarts between rounds)
--build-only   Build harnesses without running AFL
--skip-build   Use existing build in build_afl/
```

## How the Build Works

The build is a 3-step process:

1. **gcc** — builds the entire project normally
2. **afl-g++** — recompiles only `query/`, `net/`, `server/`, `fuzz/` for AFL instrumentation
3. **afl-g++ link** — re-links harnesses with the AFL runtime

This gives accurate coverage data focused on the attack surface, without instrumenting storage/vector/common code.

## Crash Results

Crash inputs are saved to:

```
fuzz_results/afl_findings/<harness>/default/crashes/
```

Reproduce a crash:

```bash
./build_afl/fuzz/fuzz_query_engine < fuzz_results/afl_findings/fuzz_query_engine/default/crashes/crash_000000
```

Run under valgrind for detailed analysis:

```bash
# Build debug harnesses first (one-time)
cd build_debug && cmake .. && make -j8 fuzz_query_engine fuzz_http_parser

# Analyze a crash
valgrind --leak-check=full --track-origins=yes \
  build_debug/fuzz/fuzz_query_engine < fuzz_results/afl_findings/fuzz_query_engine/default/crashes/crash_000000
```

## Exception Policy

The harnesses only catch expected user-input errors (`CompilerException`, `PipelineException`, `VersionControlException`). The following are **not caught** and will crash the process for AFL to report:

- `FatalException` — internal logic errors
- `bioassert` failures — assertion violations (throw `FatalException`)
- Any other unexpected exception

## Dictionaries

Token dictionaries improve AFL's mutation quality for structured inputs:

- `fuzz/cypher.dict` — 178 Cypher keywords, operators, functions, procedures
- `fuzz/http.dict` — HTTP methods, headers, TuringDB endpoints, URI parameters

These are automatically loaded by `run_afl.sh` for the corresponding harness.

## Seed Corpus

- `fuzz/corpus/cypher/` — 312 Cypher queries extracted from the test suite
- `fuzz/corpus/http/` — 20 HTTP requests covering all TuringDB endpoints
- `fuzz/corpus/csv/` — 3 CSV files (basic, quoted, no headers)
- `fuzz/corpus/gml/` — 1 GML graph file
