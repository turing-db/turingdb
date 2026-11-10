# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TuringDB is a high-performance in-memory column-oriented graph database engine written in C++23. It uses OpenCypher as its query language and is designed for analytical and read-intensive workloads with millisecond query latency on large graphs.

## Shared team memory

The following import pulls in the team's accumulated memory — coding-style feedback (with rationale and worked examples), architecture invariants, and project context. It is committed to the repo and loaded from CLAUDE.md so it reaches every teammate deterministically, independent of anyone's personal memory configuration. Treat these entries as binding as the rules in this file.

@.claude/memory/MEMORY.md

## Build Commands

```bash
# Initial setup (run only once after cloning)
git clone --recursive https://github.com/turing-db/turingdb.git
./pull.sh
./dependencies.sh

# Build
mkdir -p build && cd build
cmake -G Ninja ..
ninja -j8
ninja install

# Setup environment (adds turingdb to PATH)
source setup.sh
```

**Build variants:**
- Debug build: `DEBUG_BUILD=1 cmake -G Ninja ..`
- Release with debug info: `RELDEBUG=1 cmake -G Ninja ..`
- Callgrind profiling: `CALLGRIND_PROFILE=1 cmake -G Ninja ..`

Ninja is the preferred way to build: configure with `cmake -G Ninja ..` and build with `ninja`, not make. CMake compiles through ccache and links with mold when they are installed; `-DTURING_USE_CCACHE=OFF` and `-DTURING_USE_MOLD=OFF` turn them off.

If `ccache` is not on `PATH`, install it in the home directory, which needs no sudo. On Linux, download the latest `linux-<arch>-musl-static` archive from the ccache GitHub releases page (https://github.com/ccache/ccache/releases), where `<arch>` is the output of `uname -m`, and copy the `ccache` binary it contains into `~/.local/bin`. `~/.local/bin` must be on `PATH`. On macOS, `brew install ccache`. A build directory configured without ccache picks it up on the next `cmake ..`.

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
ninja run_regress    # Run all regression tests
```

Regression tests are Python-based and located in `regress/`. Each test has a `run.sh` script.

## Architecture

### Query Processing Pipeline
1. **Parser** (`query/parser/`) - Hand-written lexer (`CypherLexer`) and recursive-descent parser (`CypherGrammar`)
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

### Data model invariants

These are structural properties of the graph model, not validation rules layered on top — the storage layer *cannot represent* a violation. Treat them as ground truth when reasoning about correctness (a "bug" that assumes the opposite is not a bug). Code that ingests external data (`import/`, `LOAD PARQUET`/GML/JSONL) must treat a violation as malformed **user input** and reject it with a clear `TuringException`, not as an internal error.

- **Every node has at least one label.** An empty label set is not representable; there is no "unlabeled node". An import file with a node whose label list is empty/null is malformed input, not a node to be created label-less.
- **Every edge has exactly one edge type.** A null/empty edge type is not representable; an edge cannot exist without a type. A null `__type` in an import file is malformed input.
- **A property value is a scalar, a list or a map.** The scalar types are `ValueType`'s Int64, UInt64, Double, Bool, String, Embedding, DateTime, Duration and Point; `ValueType::List` is the list one, held by `TypedPropertyContainer<types::List>` over a `ListContainer` that owns its elements (see `docs/lists.md`). A stored list may be heterogeneous and may hold nested lists, so a LIST-typed column in an import file is a property to ingest, not unsupported input. `ValueType::Map` is the map one, held by `TypedPropertyContainer<types::Map>` over a `MapContainer` (see `docs/maps.md`): string keys, stored sorted, over values that may be scalars, lists or nested maps. A stored list may hold maps too, owned by its `ListContainer`'s own `MapContainer`.
- **`ValueType` and `CellTag` are append-only.** Both are written to disk as their raw ordinals — the property tag in `PropertyTypeMapDumper`, and the cell tag of an element inside an `EncodedList` and of a value inside an `EncodedMap` — with no format version beside them. A new member goes immediately before the sentinel; inserting one anywhere else silently reinterprets every datapart already written. The same holds for `net::proto::ColumnInternalKind` on the wire, and for `PointCRS`, whose ordinal every stored point carries.
- **`DateTime` is microseconds since the Unix epoch, UTC** (`storage/metadata/DateTime.h`). `datetime("...")` parses ISO-8601 and spends any offset on the instant, so `...T16:05:00+02:00` and `...T14:05:00Z` are one value and both render as `Z`. One int64 carries no zone; a zoned datetime would be a second type, not a widening of this one.
- **`Duration` is a signed count of microseconds** (`storage/metadata/Duration.h`). It has no month component, because a month has no fixed length; a calendar duration would be a second type. `duration(<integer>)` reads the count as microseconds, and a duration renders as ISO-8601 hours, minutes and seconds (`PT25H1M1S`).
- **A `Point` is x, y and z in one of four CRSs** (`storage/metadata/Point.h`): cartesian, cartesian-3d, wgs-84 and wgs-84-3d, Neo4j's. It is 32 bytes, so two equal points are equal in every byte: a 2D point holds a z of 0, no coordinate is -0.0, and the padding is zero. No coordinate is non-finite, a geographic latitude lies in [-90, 90] and its longitude in [-180, 180]. A point renders as the text `point({x: 1.0, y: 2.0, crs: 'cartesian'})`.

### Server (`server/`, `net/`)
- Single TCP listener speaking TuringDB's binary protocol framed as HTTP/1.1 chunked transfer. The in-tree `net/http_parser/HTTPParser` reads the request (endpoints routed in `server/DBURIParser.h` / `server/Endpoints.h`), `server/TuringProtoServerProcessor` handles dispatch, and `net/turing_proto_server/TuringProtoWriter` emits one proto packet per HTTP chunk. There is no JSON protocol and no third-party web framework. Client side: `net/turing_proto_client/TuringClient` (C++), `python/turingdb/binary_client.py` (`BinaryClient`), `wasm/npm` (JS).
- Default port: 6666

### Key Modules
- `db/` - TuringDB main class
- `common/` - Shared utilities and external libraries (spdlog, nlohmann_json)
- `io/` - File I/O and S3 integration
- `import/` - Data import (JSONL, GML formats)
- `jobs/` - Job/task system
- `memory/` - Memory management
- `vector/` - Vector search with Faiss

## Fix the root cause, in its most general form

**Every bug fix and every improvement goes to the root cause and takes the most general
form that fits. Never patch the symptom.** A tactical fix makes the reported query pass
and leaves the defect in place for the next query of the same shape. It also adds one
more special case that every later reader has to learn. A fix is judged by the class of
inputs it repairs, not by the one reported.

Before writing any fix:
- Name the invariant that broke and the layer that owns it, then fix it in that layer. A
  lowering crash on an op that should not have verified is a verifier or codegen bug. Wrong
  rows produced by a bad rewrite are a pass bug, not an executor bug.
- Find the class, not the instance. List the other queries, types, operators and code paths
  that reach the same defect, and probe them. The fix covers the whole class, and the tests
  show it does.
- Look for siblings. If one helper recurses once per op, check every helper in the file. If
  one switch is missing an enum member, check every switch over that enum.
- Change the abstraction, not the call site. An `if (kind == X)` next to the failing line is
  the tactical shape. A missing concept - a type, an op, an interface, a trait, one helper
  shared by N callers - is the general shape.
- Refactor when the fix needs it. Do not bend a fix around a bad abstraction to keep the
  diff small. Splitting a class, adding an op or an interface, moving logic to the layer
  that owns it, renaming what the new design makes wrong: all are part of the fix.

A fix is tactical if it: special-cases a name or a literal; rejects input it should handle;
adds a flag parameter to route around one case; catches an exception to hide it; raises a
limit (stack size, buffer size, timeout) instead of removing the dependency on it; copies
logic that already exists elsewhere; or is exercised only by the reported query.

Worked examples:
- TUR-183. An UNWIND of ~5,500 ids folded into a 5,500-deep OR chain and
  `collectNodeIDDisjunction` overflowed the 512 KB macOS thread stack. Tactical: raise the
  stack size, or convert that one helper. Root cause: IR walks that recurse once per op.
  Fix: all six recursive helpers in `DBPasses.cpp` became worklists, and the rule went into
  "Optimisation passes" below.
- Cross product OOM. Two 64Ki-row sides laid out 4.29e9 rows (64 GiB) in one step.
  Tactical: cap the side sizes or reject large products. Root cause: an op whose output
  outgrows its inputs was a straight-line op. Fix: `nl.cross_product` became an iterator
  driven like a scan. Peak memory is one chunk per product, and LIMIT works through the
  ordinary loop exit with no extra operand.
- Analyzer rejections. A valid query that fails downstream is fixed by making the engine
  run it, not by an analyzer rule that turns it away (see "Cypher language support").

If the general fix is much larger than the request, say so: give the tactical and the
general option and recommend the general one. Never ship the tactical one silently.

### Engineer the query engine like a compiler

The query engine is a compiler: parser, AST, analyzer, IR dialects, passes, lowering,
interpreter. Before choosing a design, look at how LLVM, MLIR and GCC, and the query
compilers (HyPer/Umbra, DuckDB, Calcite), solve the same problem. Follow their practice
unless there is a stated reason not to.
- One job per phase. The frontend checks and translates, passes optimise, lowering
  translates, the runtime executes. Do not move work across those lines to save effort
  (see "No optimisation in codegen").
- Invariants live in the IR and are checked once, by the op's verifier. Do not re-check
  them in every consumer. Where the type system allows, make the invalid state
  unrepresentable instead of checking for it.
- Canonicalise early, then match. Passes match one canonical shape, not N variants of it.
- Passes are small, composable, and driven by a worklist to a fixed point (see
  "Optimisation passes").
- Analyses run on IR values - use-def chains, dominance, dataflow - not on Cypher names or
  on the AST.
- Lowering is total: every op that verifies lowers. A verified op that fails to lower is a
  lowering bug.
- Reduce before fixing. Shrink a failing query to the smallest IR or Cypher that still
  fails, and make that the test.

## Design choices: build what the field builds, and ask first

**Hold every design to the standard of a professional engine, and put the choice to the
user before building it.** Reusing an existing structure for a job it was not made for
looks minimal: no new type, no new op, a small diff. That minimality is not an argument.
The borrowed structure carries the wrong meaning, every later feature has to work around
it, and every reader has to learn why it is there.

This applies to any choice that others will build on: a representation, a type, an IR op,
a data structure, an interface, a wire or file format, a module boundary. Before proposing
one, answer these:
- Is it proper? Each concept gets a representation that names it. If the case for a
  design is that an existing type or container "already handles it", it is borrowed.
- How do mature systems do it? Postgres, DuckDB, SQLite, Neo4j and HyPer/Umbra for
  databases; LLVM, MLIR and GCC for compilers; the spec or RFC for a format or protocol.
  Name the system and its mechanism. If no mature system does it your way, find out why.
- What does the folklore say? Most problems in this field have a standard answer and
  known pitfalls, in textbooks, papers and the source of those systems. Use the standard
  answer unless there is a stated reason not to.
- Then ask the user. Give the canonical option, any alternative, what each costs, and a
  recommendation. Do not implement a design choice the user has not seen.

Implementation details inside a design already agreed, or that follow a pattern the
codebase already has, need no question.

Worked example: query parameters (`$name`). Suggested: use a list element as the untyped
value that carries a parameter, since a list already holds values of any type. A parameter
is an expression whose value arrives with the query. A list element is part of a stored
property value. The suggestion used one for the other because it fit. The field's answer:
a parameter is its own node in the AST and the IR, its type is resolved at analysis or
bind time, and at execution it is bound to a value of the engine's own tagged value type.
Postgres has `Param` and `ParamListInfo` (a Datum and a type OID), DuckDB
`BoundParameterExpression` and `Value`, Neo4j a parameter map of `AnyValue`. The lesson is
not about parameters. It is about any design whose main virtue is that it needed nothing
new.

## Cypher language support

**If a query is valid openCypher, implement it. Do not reject it in the analyzer.** An
analyzer error is for queries that are genuinely ill-formed; it is never a shortcut around
a gap in codegen. When a valid query fails somewhere downstream — a lowering assert, an
invalid-IR error, wrong rows — the fix is to make the engine execute it. Adding a rule that
turns it away is a regression dressed as validation: it closes the door on the feature and
hides the gap behind a message that blames the user's query.

Before writing any new analyzer rejection, establish which side of the line the query is on:
- **Valid Cypher** → implement it. Judge by the language's semantics, not by how far the
  current implementation happens to reach. Reference points: openCypher, Neo4j's behaviour,
  and SQL where the construct is shared (`GROUP BY` / `ORDER BY` / `DISTINCT` scoping).
- **Invalid Cypher** → reject it, with a message naming what is wrong with the query.

Worked example — `ORDER BY` over an aggregating projection:
- `RETURN n.age, count(n.name) ORDER BY n.age + 1` is **valid**: `n.age + 1` is functionally
  determined by the grouping key `n.age`, exactly as SQL allows `GROUP BY age ... ORDER BY
  age + 1`. So grouping keys are substituted into the key expression at any depth
  (`DBProgramGenerator::bindOrderByKeyColumns`), and the key is computed over the grouped
  column.
- `RETURN n, count(b) ORDER BY b.name` is **invalid**: `b` is neither a grouping key nor
  determined by one, so `b.name` has no single value per group to order on. That one is
  rejected (`CypherAnalyzer::analyzeAggregateOrderBy`).

Corollary: a rejection rule must reject *exactly* the invalid set. Probe the engine with
the shapes on both sides of the line before writing the check, so a rule aimed at the
invalid ones does not also turn away queries that already work.

## Optimisation passes: ALWAYS use a worklist

**Every optimisation pass over the db or nl dialect drives its rewrites from a worklist.
Never restart a walk of the whole function after each rewrite.** This is the standard
design from decades of compiler work (LLVM's InstCombine, MLIR's greedy pattern driver,
the classic dataflow solvers), and it applies to every new pass and every pass you touch.

Forbidden shape:

```cpp
while (true) {
    Match match;
    const WalkResult walked = getOperation()->walk([&match](FilterOp filter) {
        return matchX(filter, match) ? WalkResult::interrupt() : WalkResult::advance();
    });
    if (!walked.wasInterrupted()) {
        return;
    }
    rewriteX(match);
}
```

Each rewrite re-walks and re-matches every op, so K rewrites over F candidates cost K × F
match attempts: quadratic. Required shape:
- Seed the worklist once with every candidate op.
- Pop an op, skip it if a rewrite erased it, match it, rewrite it.
- After a rewrite, push only the ops it can have changed the answer for: the users of the
  values it replaced and the ops it created. Nothing else is re-matched.
- Track erased ops (an erased set, or an `mlir::RewriterBase` listener) so the worklist
  never touches a freed op. Every erase in the rewrite goes through `rewriter.eraseOp`,
  which also reports the ops nested in the erased one; a bare `op->erase()` is invisible
  to the listener.
- A match that reads IR far above its op (a use-def chain, a factor's shape) can start to
  hold after a rewrite elsewhere. Run the worklist in rounds until a round rewrites
  nothing, as `applyPatternsGreedily` does. The cost is rounds × candidates, not
  rewrites × candidates.

When a rewrite fits `mlir::RewritePattern`, use `mlir::applyPatternsGreedily`, which is
this worklist already. For a pass over filters, use `runFilterWorklist` in
`query/ir/dialect/db/DBPasses.cpp`. Collecting all matches first and then rewriting them
is fine only when no rewrite can touch another's match.

## Ops with a body declare how columns enter and leave it

**An op with a region says, through `ColumnCarrierOpInterface`, which columns come into
the region and which come back out. A pass reads that layout from the interface. It never
works it out from the op's name.** The layout is a fact about the op, so the op states it,
once. A pass that switches over op kinds to compute argument positions holds a second copy
of that fact, and that copy covers only the ops its author thought of.

The interface (`query/ir/dialect/db/DBInterfaces.td`) has two methods for regions, beside
`getCarrySet` and `getRowSource`, which describe an op's results:
- `getRegionCarrySet(region, RegionCarrySet&)`: `_count` operands from `_operandOffset`
  come into the region as the arguments from `_argumentOffset`. `_handsBack` says the op
  also returns them as its first results. `_yieldsBack` says those results come from the
  region's yield, which then leads with them, as the region left them.
- `getYieldedResultOffset(region, offset)`: the columns the region yields come out as the
  results from `offset` on, one row for each row the region yields. It returns false where
  the op pads or merges what a region yields.

```text
op                                      into the region                   back out
CASE, coalesce, list ops                trailing operands, trailing args  -
EXISTS, COUNT, COLLECT, [(n)-->(m)|..]  inputs, leading args, row tag      -
CALL run once per row                   inputs, all args                  inputs, then yields
CALL carrying its scope                 inputs, leading args              its yield, inputs first
OPTIONAL MATCH, optional CALL           inputs, leading args              inputs only: own columns pad
cross product, hash join                -                                 each factor's yield in turn
```

A body run once per row has no row tag; one carrying its scope may have one after its
carried arguments, which is why the arguments are counted from `_argumentOffset` rather
than from the end.

Rules:
- A new op with a region implements `getRegionCarrySet`, and `getYieldedResultOffset` when
  its results are a region's yield row for row. An op that implements neither is opaque:
  no pass carries a column into its body or out of it. That is correct, but every
  optimisation stops at the op.
- `getYieldedResultOffset` returning false is a correctness statement. A row an optional
  op's body missed comes back with its own columns null, so a column carried out through
  the yield would turn that row's real value into null.
- In a pass, go through the helpers in `DBPasses.cpp`: `matchRegionCarrySet`,
  `carriedColumnOf`, `carriedArgument`, `carryIntoRegions`. Never write
  `isa<CallSubquery, OptionalMatch>` next to argument arithmetic. A pass that handles only
  some ops, as `trim_unread_columns` handles CASE and the list ops, keeps that choice as a
  predicate over op kinds and still reads the layout from the interface.
- This is MLIR's own answer: `RegionBranchOpInterface::getEntrySuccessorOperands` says
  which operands flow into which region arguments, and `RegionBranchTerminatorOpInterface`
  which yielded values flow to which results.

Worked example: TUR-390 (#1348). `reuse_property_reads` reached into CASE, coalesce and
the list ops through `matchRegionCarrySet`, a switch that knew their trailing layout.
Subquery bodies take their inputs first, ahead of a row tag, so
`MATCH (n:Person) RETURN n.age, COUNT { MATCH (n)-->(m) WHERE m.age = n.age }` read
`n.age` a second time inside the body. A sixth case in the switch would have fixed COUNT
and left EXISTS, COLLECT, CALL, OPTIONAL MATCH, pattern comprehensions and cross products.
Instead the layout moved onto the ops: 14 ops implement one or both methods, and the pass
walks into and out of all of them with one generic step each way.

## Maps keyed by IR objects forget what is erased

**A map keyed by `mlir::Value` or `mlir::Operation*` drops an op's entries when the op is
erased, and every erase goes through the one function that does it.** MLIR puts a new op
in the memory an erased one freed. A key left behind then answers for whatever value is
created next at that address. Which value that is depends on the heap, so the program
changes between runs of one binary, and so does the bug.
- In lowering, erase through `DBLowering::eraseOp`, never a bare `op->erase()`. It calls
  `forgetErasedValues` (`query/ir/common/IRConstantColumn.h`), which drops the results and
  block arguments of the op and of every op nested in it.
- A new cache keyed by IR objects does the same at each erase, or is cleared after every
  rewrite, as `ConeCache` in `DBPasses.cpp` is.
- MLIR's own answer is a `RewriterBase::Listener` (`notifyOperationErased`). It works only
  in `add_mlir_library` targets: MLIR is built without RTTI and only those targets get
  `-fno-rtti`, so subclassing `RewriterBase` in lowering or common fails to link with
  `undefined symbol: _ZTIN4mlir12RewriterBaseE`.
- Iterating such a map is non-deterministic too. Use `InsertionOrderedMap` when the order
  reaches the IR (#1316).

Finding a bug of this class:
- An intermittent crash in a test that runs fixed queries is non-determinism until shown
  otherwise. Run `EXPLAIN (nl)` of the query in many processes and compare the dumps before
  reading the crash. Two different dumps put the bug in codegen or lowering, however far
  downstream it crashes.
- ASAN did not reproduce it: its allocator lays the heap out differently.
- Pin it with a test that moves the heap: lower the query many times, each behind padding
  of a different size, and require one program (`LoweringDeterminismTest`). 64 lowerings
  passed on the broken code; 256, each behind 24 more bytes, failed 10 runs of 10.

Worked example: #1374. `main` failed `test_query_ir_doom` with a segfault about 1 run in
30. When a `reduce` accumulator widens (`line = ''` becomes Any), `lowerFold` erases the
`nl.reduce` it built and lowers it again. The erased values stayed in `_constantColumns`,
lowering's cache of which values hold one value for every row. When the retry's
`nl.constant "#"` landed at the address of an erased value cached as not constant,
`rowAlignedChunk` did not broadcast it, and `NLTranslator` read the 1-row constant as a
full column. The crash was in the translator, but making `to_any` accept a constant would
have left lowering non-deterministic. The fix routed all 13 erases in lowering through
`eraseOp`.

## DenseMap moves its entries on erase and on growth

**No iterator, reference or pointer into an `llvm::DenseMap` or `llvm::DenseSet` survives an
erase from it or an insert into it.** Both move entries. A stale reference reads whatever the
heap left there, so the bug changes between runs.
- An erase leaves no tombstone. This LLVM's `eraseFromFilledBucket` shifts the entries after
  the hole back into it (Knuth's Algorithm R). Erasing while iterating skips the entry that
  moves into the hole. Use `remove_if`, which `DenseMap.h` names as the replacement.
- An insert that grows the table moves every entry to a new bucket array and frees the old
  one. `M[k] = it->second` and `M[a] = M.at(b)` then read freed memory: C++17 evaluates the
  right side first, but only to a reference, and the copy reads it after `M[k]` has grown
  the table. Copy the value into a local before the insert.
- A reference from `M[k]` or `M.find(k)->second` does not live across a call that can insert
  into or erase from `M`, including a call handed `*this`.
- `InsertionOrderedMap` keeps references across an insert, since its entries live in a
  `std::deque`, but not iterators. An erase invalidates both.
- valgrind reports these on every run that moves the entry: the read lands in a freed bucket
  array. A native run mostly reads the old value and passes.

Worked example: `rowSource` in `IRRowAlignment.cpp` wrote `sources[value] = operandIt->second`.
The 48th entry grows the table from 64 buckets to 128. A `coalesce` of a 46-term sum carried
across a hop put the `nl.fill_nulls` at that entry. Once glibc reused the freed array, the
source read back was garbage, and `sameRowSource` segfaulted on it in 9 runs of 10
(`CarriedColumnRowSourceTest`). The first version of the scrub in `DBLowering::eraseOp` erased
from `_valueMap` while iterating it and kept one of two entries holding a freed value.

## A null range: ask why the caller holds one

**When UBSan reports a `memcpy` given a null pointer, find out why the caller holds a null range
before changing the copy.** There are two cases, and only one is a bug in the caller.
- A null handled as a value: the caller copies the payload of something that is null. Fix the
  caller. Write the Null tag, or skip the payload after `isNull()`, as the other nullable
  readers do. A copy that accepts the null range leaves the caller wrong.
- A valid empty value whose pointer is null: MLIR spells `''` as a null `StringRef`, and a
  `std::vector` that never allocated has a null `data()`. `std::span`, `std::string_view` and
  `std::copy` all allow `(nullptr, 0)`; only C's `memcpy` does not. The byte sinks accept it
  through `std::copy_n`: `SpanBuffer`, `StringBuffer`, `TuringProtoInBuf`, `TuringProtoOutBuf`
  and `NLSerializedKey`.
- To list the callers, put the sink back on `memcpy` and run the tests with
  `UBSAN_OPTIONS=print_stacktrace=1`. Each test stops at its first report, so rerun the
  failures after each fix.

Worked example: the CI Sanitizers nightly of 2026-11-10. A `count(DISTINCT p)` over a missed
OPTIONAL MATCH keyed the null path, an empty `GraphPath`, as a path with zero IDs, and appended
its IDs: `memcpy(dst, nullptr, 0)`. The first fix made `NLSerializedKey::append` accept that.
The cause was the key serializing a null as a value: #1434 keys it as `CellTag::Null`, and #1435
stops the wire decoders reading the null path's IDs. With the sinks on `memcpy`, 27 of 1243
tests failed: 2 from null paths, and 25 from MLIR's empty string, which is a valid value. So the
sinks stay tolerant.

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
- `constexpr` constants are written in uppercase with underscores: `DB_PASS_COUNT`, not `dbPassCount`; `PREVIEW_ROWS_COUNT`, not `kPreviewRows`. Never use the Google-style `k`-prefix. Plain `const` locals and parameters stay lowerCamelCase.
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

**Comments — write as few as possible:**
- **The default is no comment at all.** Code that needs a comment to be understood usually needs a better name or a smaller function — fix that instead of narrating it.
- Never restate what the code already says (`// increment the counter`, `// loop over the rows`, `// return the result`), never add section banners (`// ---- helpers ----`), and never add a doc block on top of every function/class/member just because it's public.
- When a comment is genuinely warranted, it explains a **why** that the code cannot: a non-obvious invariant, or a workaround for an external bug whose cause is off-screen. **Hard cap: 2-4 lines.** If it doesn't fit, the explanation belongs in the commit message, the PR, or a design doc — not in the source.
- That bar is high, and "it explains a why" is not self-justifying — it is the excuse every unwanted comment comes with. Before writing one, name the specific wrong conclusion a teammate would reach without it. If you cannot, there is no comment to write. **When in doubt, leave it out** — a reviewer asking for one comment is cheap, deleting the ones nobody asked for is not.
- **Never explain where a statement sits.** A comment justifying that a call runs after (or before) some other code — "runs once X has populated Y", "must come before Z" — is narration: the order is in the source, and a reader who moves the call finds out from the tests. Ordering rationale goes in the commit message. This is the clause that gets abused most, so it is not a why that earns a comment.
- **A comment is not part of implementing a change.** Finishing an edit is not an occasion to annotate it. If you find yourself writing prose about code you just wrote or just moved, that is the reflex this section exists to stop — delete it and move on.
- **Never annotate the conventions in this file.** Several rules here mandate constructs that look odd out of context — the unreachable `break;` after a `return` in a `case`, unused parameters left plainly named, no `= default` in headers, `bioassert` in place of a check. They are house style and every reader knows them; `// unreachable`, `// intentionally unused`, `// see CLAUDE.md` is precisely the noise this section forbids.
- Don't leave behind change-log narration (`// was previously X`, `// NEW:`, `// fixed bug where...`) — git history holds that.
- Do not re-add comments a reviewer deleted, and don't add comments to untouched code you happen to be editing near.
- `CODING_STYLE.md`'s snippets carry `//` markers (`// Good:`, `// Bad:`, `// Do something` bodies) to label the *examples*. They are not a model for how much to comment real code.
- The one standing exception is the free-function layer at the top of `tools/*` drivers with a `main()`: one single-line description per helper. It does not extend to class methods, to library code under `storage/`, `query/`, `net/`, or to any file outside `tools/`.

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
- For count/budget values that are never negative (LIMIT counts, row budgets), keep them unsigned end to end: `UI64Attr` in MLIR ops → `uint64_t` accessor → `size_t` in the interpreter. Never use `I64Attr`/`int64_t` and cast — a negative literal should fail to parse, not reach a verifier.
- Don't wrap unused parameter names in `/*comments*/` — just name them normally. `-Wunused-parameter` isn't enabled in this codebase, so there's no warning to silence.
- Don't add `(void)param;` lines in function bodies to mark a parameter as used. Same reason: no warning to silence; it's pure noise.
- **Fill columns with `std::fill`/`std::fill_n`/`std::copy`, not `reserve` + `push_back` loops.** A `push_back` loop does not vectorise; `std::fill_n` (repeated value) and `std::copy` (range, commonly lowered to `memcpy`) do. When laying out a column whose size is known up front, `resize` the output's `getRaw()` to the final size, then write ranges through an iterator with these algorithms — see `CartesianProductProcessor` and the `blockRepeatColumn`/`tileColumn` helpers in `NLExecutor.cpp`.

## Design Conventions

- Embedding operations (cosine_similarity, euclidean_distance) are implemented as functions through the EvalFunction path, not as binary operators. New computation operations on embeddings should follow the function pattern (Functions.h, EvalFunction, ColumnFunctions).
- **Extract-then-render for inspection tools.** When building tools that consume a streaming/SAX source (e.g. `ParquetSaxVisitor` driving `ParquetReader`), don't print or serialize directly inside the visitor callbacks. Define a high-level, source-independent data structure (e.g. `ParquetSchema` with `ParquetSchemaField`), have the visitor populate it, and render in a separate step. The high-level object stays decoupled from the source format and can be queried, transformed, or rendered multiple ways.
- **Visitor shared state lives in the caller.** For SAX-style visitors that produce or mutate a shared output object — including pipelines where one visitor extracts and a later visitor enriches (e.g., `ParquetSchemaExtractor` then `ParquetJsonDetector`) — construct the output in the caller and pass it to each visitor's constructor by reference. The visitor stores `T&` and mutates the shared object in callbacks; it does NOT own the object and does NOT expose a `getX()` accessor. The caller's flow then reads as one object flowing through a series of visitors.
- **No optimisation in codegen.** `DBProgramGenerator` emits the straightforward, obviously-correct db-dialect form and nothing cleverer. Liveness / dead-column elimination, "is this variable read later", CSE, reordering, pushdown — all of it belongs in a dedicated MLIR pass over the emitted dialect, where the analysis runs on MLIR values, is reusable, and has one source of truth. Concretely: codegen may only ask questions about what it has already built ("which columns are bound and row-aligned at this insertion point" — `collectInFlightColumns`), never forward-looking ones ("will anyone read this again"), which would mean walking the AST ahead of the insertion point and maintaining a parallel dead-set beside the IR. Emit every column in flight and let a pass prune it. A straightforward emission that is wasteful is the correct output — note the pass that should improve it instead of improving it here. Avoid `live`/`dead` naming in codegen too; it implies an analysis that should not be there.

## Build/iteration workflow

- During multi-turn iteration on a change (style feedback, API shape, refactors), **don't build after each edit**. Just write the edits and stop. Builds are slow and noisy; running them every micro-revision burns time. Only build when the user explicitly asks ("build" / "compile" / "run it") or signals the design is settled.
- **Build only the affected target, never the whole tree for a test.** When the change is one unit test — writing it, fixing it, adding a case to it — build and run that test's target alone: `ninja -j8 test_query_ir_<name>`, then `./test/query/ir/<feature>/test_query_ir_<name>`. Do not run a bare `ninja -j8`. The same holds for any single binary you are fixing: name its target. A bare `ninja -j8` rebuilds the whole engine and every test binary for nothing.
- Don't preface `ninja` with `cmake ..`. `ninja` already re-runs cmake when any tracked `CMakeLists.txt` has changed. Only run `cmake ..` after editing `CMakeLists.txt` / `dependencies.sh`, or when resetting the build directory.
- **Make minimal, targeted edits — fix exactly what's asked.** When the request is narrow (a comment, a wording, one line), change only that and leave surrounding working code alone. Don't refactor an adjacent loop or rename things just because you're nearby (e.g. asked to fix a confusing comment over a loop, rewrite the comment — don't also convert the loop to `all_of`). It keeps diffs small and reviewable and respects the existing structure. If a larger refactor seems worthwhile, propose it separately and let the user decide rather than bundling it in. This rule is about *unrelated* changes. A refactor that the root-cause fix needs is part of the fix, not scope creep (see "Fix the root cause, in its most general form").

## Benchmarking

**Compare binaries one server at a time.** Start a server, load the graph, time the queries,
stop it, then start the next. Never keep two servers resident, loading or answering at the
same time, even when their queries are interleaved and never execute at once: they share the
L3, the memory bandwidth and the CPU's turbo budget. Run the servers in ABAB order (baseline,
new, baseline, new) with the same seed and query order, so drift over the session hits both
binaries equally.

**A broad slowdown is the machine until an A/B says otherwise.** When turing-bench reports
regressions spread over many unrelated queries, check the host before the code:
- Idle states: `cat /sys/devices/system/cpu/cpu*/cpuidle/state*/disable`.
- The clock one busy core reaches: `perf stat -e cycles,task-clock -- timeout 3 sh -c 'while :; do :; done'`.
- Other load during the run: `sar -u -f /var/log/sysstat/saDD`.
- Changes made with sudo: `sudo grep COMMAND /var/log/auth.log`.

Then time the two commits' binaries as above. turing-bench keeps them on hetz-bench in
`~/.cache/turing-bench/bin/<sha>/turingdb`. hetz-bench is shared: work in a private directory
on a private port, and kill only your own pids. An interrupted ssh command does not stop a
job it started with `setsid nohup`; check `ps` and stop it before the next measurement.

Worked example: turing-bench run #4 (2026-10-27) reported 55 regressions of +20-40% for
`2ed7e5956` against `1ea18067`. Between the two runs C6 had been disabled on hetz-bench, which
capped one busy core at 2.8 GHz instead of 3.9. The two binaries timed the same, and
`LOAD JSONL`, whose code had not changed, took 215 s against 167 s. With C6 back on, a rerun
cleared the 55 regressions.

## Triaging review findings

**Every finding a review turns up gets one outcome: fixed now, filed, by design, or not
tracked. When no rule below clearly applies, file it.** "By design" and "not tracked" take a
finding out of view, so they need a rule that covers it. A ticket costs little; a wrong
dismissal hides a bug.

Classify each finding with the first rule that fits:
- **Non-determinism: fix now, before anything else.** A query whose IR, plan or answer differs
  between two runs of one binary. Example: codegen walked pointer-keyed maps, so 6 fixtures
  carried their columns in heap-address order; #1316 made those maps `InsertionOrderedMap`s.
- **An answer that depends on chunk boundaries or row order: never by design**, even when it
  follows from a design that was chosen. Example: TUR-419. A cell write types a new property
  from its first chunk, so 65,536 integers then 2.5 store as Double, while 65,537 raise.
- **A major bug, related to the change under review, with a simple fix: fix now.** A bug that
  predates the change, or whose fix needs a design choice, is filed instead (TUR-416:
  a failed write query keeps its writes).
- **Arithmetic overflow: not tracked, except for durations.** Integer overflow is out of scope
  for now (TUR-214 was cancelled). `sum()` of durations wrapping where `duration + duration`
  raises is tracked (TUR-410).
- **Error messages: not tracked.** A wording, a type name inside a message, a missing caret.
- **TuringDB policy: by design.** `id()` and `elementId()` are always numbers.
- **Where TuringDB chose better than Neo4j: by design.** A property holding mixed kinds raises,
  a map can be a property, and values are more typed: a typed `sum()` of no rows is the zero
  of its type (0.0, PT0S), `percentileCont()` is always a Double, and a duration has no month,
  so two durations compare. A difference from Neo4j is a bug only if it is wrong in
  TuringDB's own model.
- **Dead code: delete it now**, with its build entries. Never file it.
- **Everything else: file a ticket**, as a bug, a gap or a limitation. A missing feature is
  filed, not built during triage.

Fixing one:
- Each fix is its own PR, stacked on the previous one. Write the failing test first.

Filing one:
- Reproduce it on the current tip first. Put the query and the answer you got in the ticket.
- Search Linear for a duplicate. A sibling of an open ticket goes on it as a comment: the
  `count()` cell beside a double that cannot type a new property went on TUR-394.
- Team TuringDB, project Query Engine, no priority. Write the description for people; code
  locations and reproduction details go in a collapsible `+++ Agent notes` section.
- When the fix is a design choice, list the options with their costs and leave the choice open.

After each fix or ticket, update the review document. Each finding states its status
(`fixed by #N`, `ticketed as TUR-N`, `by design`, `not tracked`, `open`), and every summary
line points to the same place.

## Project context

- **Partitioning** is on the near-term roadmap (as of 2026-05-04). Some customers have specifically asked for **METIS-style structural partitioning** (edge-cut minimization), not hash partitioning. Treat partitioning as a near-term constraint when discussing scale-out, not a future-optional.
- The **replication / distribution** initiative is motivated by *both* (a) scaling beyond single-node memory (which forces partitioning) and (b) write availability across network partitions / regions (which motivates CRDT-style merge). The realistic landing point is a layered hybrid; evaluate any proposal against both goals.

## CI runners

- Linux x86 CI runs on **self-hosted runner containers** (`ubuntu-22.04-turing`, `ubuntu-24.04-turing`), several per host. The containers of a host share one cache directory, mounted at `/opt/actions-local-cache`, where `scripts/ci_cache.py` restores and saves `external/dependencies` and the reactome dataset. Bump the key's salt when changing how deps are built, or stale entries persist.
- macOS CI runs on **GitHub-hosted larger runners** (`macos15`, `macos26`), one job at a time each. They cache `external/dependencies` with `actions/cache`, whose repo limit is 10 GB. Linux ARM runs on GitHub-hosted `ubuntu-*-arm` and caches `external/dependencies` with `actions/cache` the same way.
- The dependency build bakes **absolute paths into its installed CMake/pkg-config files** (e.g. faiss's `BLAS_LIBRARIES=…/external/dependencies/lib/libopenblas.a`, the LLVM/MLIR configs). A cached tree is only valid at the checkout path it was built at.
- Per-host serialization (port 6666, host-wide `pkill turingdb`) uses a kernel `flock` via `scripts/with_host_lock.py`, which the OS releases on process exit.

## Commit style

Do not include Claude as a co-author in the commits.

Commit messages are one short area-prefixed line, no body — e.g. `MLIR: implement limit`. The area prefix names the subsystem touched.

## PR style

PR descriptions are one short sentence. Do not use the default `## Summary` / `## Test plan` template — no headers, no bullets, no checklists, no "Generated with Claude Code" footer. The title carries the detail; the body just states the intent in a sentence.

### Explain the PRs you open

**When you open a pull request, run `/pr-explain` on it before you hand it back.** The body
stays the one sentence above. The explanation goes in a comment, so the two rules do not
fight: the author's sentence says what the PR is for, and the comment gives a reviewer the
shape of the change before they read a line of the diff.

`/pr-explain` writes the comment in Matt Pocock's `pr` shape — Summary, Evidence, Merge
Danger — over a PR Lens diagram of the change, and opens it with an `AI-generated summary`
note. All three skills are vendored under `.claude/skills/`, so this needs nothing
installed. It does need node 20.11 or later for the diagram; the skill finds one or says so
and carries on without it.

The skill posts the comment without asking, with its diagrams attached as SVGs. It never
pushes a canvas to `prlens.dev`, which is a third party, and it never touches the PR body
or title.

For a stack of PRs, explain the ones a reviewer has to understand on their own. A stack
of near-identical mechanical changes does not need one comment each.
