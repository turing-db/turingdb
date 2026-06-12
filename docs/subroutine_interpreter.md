# A subroutine-threaded interpreter for the nl dialect

This document is the design and implementation plan for the runtime that executes
`nl` (nested-loop) dialect programs. The `nl` dialect (`query/ir/dialect/nl`) is
the imperative, chunk-at-a-time counterpart of the declarative `db` dialect: a
query is a nest of `nl.for` loops driving database iterators, with `nl.output`
as the sink. The interpreter described here runs such a program directly,
without lowering to LLVM and without walking MLIR data structures at runtime.

```mlir
func.func @two_hop() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!nl.node_id>> {
    %edges = nl.get_out_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %b in %edges
        : !nl.iter<!nl.chunk<!nl.node_id>, !nl.chunk<!nl.edge_id>, !nl.chunk<!nl.edge_type_id>, !nl.chunk<!nl.node_id>> {
      %hop = nl.get_out_edges(%b, {%srcs}) : !nl.chunk<!nl.node_id>
      nl.for %srcs2, %eids2, %etypes2, %c, %aFiltered in %hop
          : !nl.iter<!nl.chunk<!nl.node_id>, !nl.chunk<!nl.edge_id>, !nl.chunk<!nl.edge_type_id>, !nl.chunk<!nl.node_id>, !nl.chunk<!nl.node_id>> {
        nl.output(%aFiltered, %c) : !nl.chunk<!nl.node_id>, !nl.chunk<!nl.node_id>
      }
    }
  }
  func.return
}
```

# Design space

Four execution strategies were considered. The decisive context for all of them
is that `nl` dispatch is **per chunk, not per row**: one `nl.for` step fills a
chunk of up to `ChunkConfig::CHUNK_SIZE` IDs through a storage iterator, so the
cost of one handler transition is amortized over thousands of rows of
memory-bound graph traversal work. Interpretation overhead is a fraction of a
percent of runtime before any dispatch optimization is applied.

## Tree-walking the MLIR module — rejected

Walking `mlir::Operation`s at runtime means dispatching on the dynamic op type
at every step, chasing pointers through MLIR's allocation-heavy structures, and
re-deriving types and operand bindings on every visit. It also keeps the MLIR
module alive for the lifetime of the query. The deeper objection is the lost
**translate-once** step: a separate translation pass is the natural place to
assign storage to SSA values, preallocate chunk buffers and select kernels, so
that none of it is redone per iteration.

## Tail-call threading (musttail + preserve_none) — rejected

First POC: https://godbolt.org/z/8MYY7o8ao. Handlers chained with
`__attribute__((musttail))` calls under `[[clang::preserve_none]]`, an explicit
frame of slot pointers in registers, loop control decomposed into
loopInit/loopHead/loopBody handlers (the CPython 3.14 / wasm3 family).

Its two virtues are translate-once (shared with the chosen design) and
suspendability: with an explicit frame and pc and a flat C stack, execution can
pause mid-loop. Its costs: `preserve_none` requires Clang 19+ (and gating with
`__has_attribute`), throwing through musttail/preserve_none frames does not mix
with the codebase's `TuringException`-based error model, and the flat stack
makes perf/gdb attribution opaque. Per-instruction dispatch savings are what
this technique buys, and at chunk granularity there is nothing to save.

## Copy-and-patch JIT — rejected

Build-time-compiled stencils memcpy'd into executable memory with operands and
continuations patched in (the CPython 3.13 JIT technique). A stencil JIT is the
subroutine-threaded interpreter with the indirect calls inlined and the
descriptor data constant-folded into immediates — so its ceiling is exactly the
glue fraction it removes, under 1% here. In exchange: a pinned-Clang stencil
extraction toolchain (the Clang version gate returns through the back door,
plus relocation parsing, per-architecture stencil sets), W^X executable memory
in a long-running server, no exception unwinding through JIT frames, and
anonymous code in profilers. I-cache behavior also inverts in the interpreter's
favor: interpreter handlers are shared across all concurrent queries and stay
hot, while per-query JIT code starts cold.

If fused compilation is ever warranted (tight per-row expression work), the
natural route is MLIR lowering (`nl` → `scf` → `llvm` + ORC) behind adaptive
tiering — start interpreting, swap in compiled code when it pays — not a
hand-rolled stencil system. The interpreter below is the tier 0 such a design
requires, so the two are stages, not rivals.

## Subroutine threading — chosen

Second POC: https://godbolt.org/z/xaT9n4f4f. Handlers are ordinary functions
composed by indirect calls through a descriptor table built at translation
time. The key property: **loop control stays a native C++ loop inside the
handler**, and the nesting structure of the nl program maps directly onto the C
call stack. With `-O3` each loop compiles to real machine-code loop structure;
the only per-iteration interpretation cost is one predicted indirect call.

What this buys over the alternatives:

- No toolchain gate: plain C++, any Clang or GCC.
- Exceptions work: a `TuringException` thrown from the innermost handler
  unwinds the loop nest correctly for free. Early termination (LIMIT) gets the
  same channel.
- The C stack mirrors the loop nest, so perf and gdb show `forLoop → forLoop →
  output` with time attributable per nesting depth.
- The handlers drive the **existing storage chunk writers**
  (`ScanNodesChunkWriter`, `GetOutEdgesChunkWriter`, `GetInEdgesChunkWriter`)
  unmodified — the interpreter reuses the native iteration machinery rather
  than reimplementing it.

The conscious trade-off: execution is **run-to-completion**. Iteration state
lives on the C stack, so the interpreter cannot pause mid-loop and resume.
Streaming results does not need suspension — `nl.output` pushes into a sink,
and the sink can stream chunks out while the loops keep running. Suspension
would only matter for cooperative time-slicing of queries on a shared thread;
if that ever becomes a requirement, the escape hatches are a stackful fiber or
moving loop state into the descriptor data.

Recursion depth equals the query's pattern length (a handful of frames), so
stack depth is a non-issue.

# Generalizing the POC

The POC indexes a flat descriptor array by loop depth (`_descrs[depth + 1]`),
which only handles perfectly nested single-statement bodies. The real dialect
needs three generalizations:

1. **Bodies are sequences.** An `nl.for` body can contain an `nl.output` *and*
   a nested loop, or several sibling loops. Each loop's descriptor data owns
   its body as a vector of descriptors and the handler runs the whole sequence
   per step. Once the data carries its own body, the `depth` parameter and the
   global table disappear.

2. **Iterator-producing ops are not descriptors.** `nl.scan_nodes`,
   `nl.get_out_edges` and `nl.get_in_edges` do nothing per step — they
   configure the iterator that the adjacent `nl.for` drives. Translation folds
   them into the loop's descriptor data (iterator kind plus resolved slot
   pointers). The descriptor program collapses to just loops and outputs.

3. **Translation is where types die.** Every SSA chunk value gets a slot — a
   preallocated `ColumnVector` of the right element type — assigned during
   translation. Handlers receive typed pointers resolved once; the runtime
   never consults a type again. The loop structure is static, so the full set
   of live chunks is known up front and execution performs no allocation.

# Architecture

New library `query/ir/interpreter/`, static target `turing_db_ir_interpreter_s`,
linking the storage library, the nl dialect library and `turing_db_ir_common_s`.

The hard boundary: **only `NLTranslator` includes MLIR headers.** `NLProgram`
and `NLExecutor` are pure runtime. After translation the MLIR module can be
destroyed; the descriptor program is self-contained, which also makes it the
natural unit for a future plan cache.

```
        MLIR module (nl ops)
              |
              |  NLTranslator::translate(func::FuncOp)     [MLIR-facing, once]
              v
          NLProgram          descriptors + preallocated chunk slots
              |
              |  NLExecutor::run(view, program, sink)   [no MLIR, per execution]
              v
         NLOutputSink        receives output chunks, push-based
```

## NLProgram — the descriptor program

```cpp
enum class NLChunkKind {
    NodeID,
    EdgeID,
    EdgeTypeID,
};

struct NLFunctionData;

using NLHandlerFunction = void (*)(NLExecutionContext&, NLFunctionData*);

struct NLFunctionDescriptor {
    NLHandlerFunction _function {nullptr};
    NLFunctionData* _data {nullptr};
};
```

`NLFunctionData` is the base of the per-descriptor payloads. Loop payloads come
in two shapes, monomorphized at translation time so there is no runtime switch
on iterator kind:

- `NLScanLoopData`: the `ColumnNodeIDs*` slot of its single loop variable, and
  the body descriptors.
- `NLEdgeLoopData` (shared by the out- and in-edge loops; direction is encoded
  by which handler the descriptor points at): the `const ColumnNodeIDs*` input
  slot (a loop variable of the enclosing loop), typed slot pointers for the
  four fixed chunks (sources, edge IDs, edge type IDs, targets), a scratch
  `ColumnVector<size_t>` for the writer's indices column, a carry list of
  `{const Column* in, Column* out, gather function}` entries with the gather
  selected by `NLChunkKind` at translation time, and the body descriptors.

`NLOutputData` holds the operand slots as `const Column*` plus their kinds.

`NLProgram` owns all of it: the slot buffers (one `ColumnVector` per `nl.for`
block argument, reserved to the chunk size up front), the `NLFunctionData`
nodes, the top-level descriptor vector, and a configurable
`size_t _chunkSize {ChunkConfig::CHUNK_SIZE}` so tests can force multi-step
loops with tiny chunks.

Known simplification for this milestone: the program owns its slot state, so
one `NLProgram` instance supports one execution at a time. Splitting the
immutable program from per-run state belongs with the plan cache work.

## NLExecutor — handlers and entry point

```cpp
class NLExecutor {
public:
    static void run(const GraphView& view, NLProgram& program, NLOutputSink& sink);
};
```

`NLExecutionContext` carries the `GraphView`, the sink pointer and the chunk
size. Handlers are static member functions of `NLExecutor`, one per
descriptor shape, exposed so the translator can bind them into descriptors.

**Scan loop.** Constructs a `ScanNodesChunkWriter(view)` on the handler's own
stack — loop state on the native C stack is the point of the design — binds the
loop-variable slot with `setNodeIDs`, then:

```cpp
while (writer.isValid()) {
    writer.fill(context._chunkSize);
    if (!nodeIDs->empty()) {
        runBody(context, loopData->_body);
    }
}
```

(`fill` clears its bound columns before writing the next chunk, so handlers do
not clear the writer-filled slots themselves.)

**Edge loops.** One shared implementation templated over the writer type,
instantiated as the out-edges and in-edges handlers. The writer is constructed
fresh on each entry to the handler — each step of the *enclosing* loop re-enters
it, so the iterator naturally re-initializes against the outer loop's current
input chunk. Per step the writer fills its chunks plus the indices column,
where `indices[i]` is the input-chunk row that produced output row `i`. That
indices column carries the whole join-back semantics:

- out-edges: the writer fills edge IDs, edge types and targets; the sources
  chunk is gathered: `sources[i] = input[indices[i]]`.
- in-edges: the writer fills sources directly (`setSrcIDs`); the targets chunk
  is gathered from the input the same way.
- carries (`columns_to_filter`): `carryOut[k][i] = carryIn[k][indices[i]]` via
  the translation-time-selected gather. This one gather kernel implements the
  dialect's carry-set filtering.

Carry-in chunk lengths are `bioassert`ed against the input chunk length at
loop entry.

**Output.** Asserts all operand chunks have equal length and calls
`sink.appendChunks(...)`.

Errors throw `IRException`; because handlers are ordinary calls, a throw from
any depth unwinds the entire nest correctly.

## NLOutputSink — push-based output

```cpp
class NLOutputSink {
public:
    virtual ~NLOutputSink();

    virtual void appendChunks(std::span<const Column* const> chunks) = 0;
};
```

This is the run-to-completion contract: output is pushed chunk-wise as the
loops run. Streaming to a client, if ever needed, plugs in here without
touching the interpreter. Tests use a collecting sink that copies chunks into
owned columns.

## NLTranslator — MLIR to NLProgram, the only MLIR-facing file

Constructor takes the `NLProgram&` to fill (caller-owns-output convention);
`void translate(mlir::func::FuncOp function)` performs a single walk of the
function body:

- `nl.scan_nodes` / `nl.get_out_edges` / `nl.get_in_edges` emit no descriptor.
  They record an iterator-config entry keyed by their result `Value`: the
  iterator kind, the input chunk value and the carried chunk values.
- `nl.for` looks up its operand's config, allocates one slot per body block
  argument (block-argument order equals iterator chunk order, guaranteed by
  `For::verify`), registers the arguments in the value-to-slot map, builds the
  loop payload with resolved slot pointers, and recurses into the body.
- `nl.output` resolves its operands through the value-to-slot map into an
  `NLOutputData`.
- `nl.yield` and `func.return` are structural; any other op is an
  `IRException`.

Chunk SSA values only exist as `nl.for` block arguments, so the value-to-slot
map is complete by construction. An iterator operand not defined by one of the
three source ops is a translation error. An iterator value consumed by two
`nl.for` ops works for free: each loop instantiates its own writer from the
shared config.

Two same-loop checks close a gap the op verifiers leave open (they constrain
only types): every `nl.output` operand must be a loop variable of the
innermost enclosing `nl.for`, and a carry set must consist of loop variables
of the same loop that binds `input_nodes`. Cross-loop chunks have no per-row
correspondence to the current step, so such programs are rejected at
translation with an `IRException` instead of misaligning rows or tripping the
runtime length asserts.

# Storage APIs the handlers drive

All in `storage/iterators/`; exhaustion is `Iterator::isValid()` in all cases.

| Writer | Construction | Per-step |
|---|---|---|
| `ScanNodesChunkWriter` | `(const GraphView&)` | `setNodeIDs(ColumnNodeIDs*)`, `fill(maxCount)` |
| `GetOutEdgesChunkWriter` | `(const GraphView&, const ColumnNodeIDs* input)` | `setIndices`, `setEdgeIDs`, `setEdgeTypes`, `setTgtIDs`, `fill(maxCount)` |
| `GetInEdgesChunkWriter` | `(const GraphView&, const ColumnNodeIDs* input)` | `setIndices`, `setEdgeIDs`, `setEdgeTypes`, `setSrcIDs`, `fill(maxCount)` |

Chunks are `ColumnVector<T>` (`ColumnNodeIDs`, `ColumnEdgeIDs`,
`ColumnEdgeTypes` from `storage/columns/ColumnIDs.h`). The writers handle
tombstone filtering internally, including filtering the indices column, so the
gather-by-indices step composes correctly with deletions.

To verify while coding (not design risks): the exact append/overwrite behavior
of `fill` (handlers clear slots per step regardless), and the in-edges setter
name.

# Testing

`test/query/ir/NLExecutorTest.cpp`, following the `ScanNodesIteratorTest`
fixture pattern (`TuringTest` + `JobSystem`; `Graph::create()` →
`newChange()` → datapart builder → `submit`). Programs are parsed from string
with `mlir::parseSourceString<ModuleOp>` (the file-based `IRAssembler` stays
out of unit tests), translated, and run with a collecting sink.

1. scan → output: all node IDs; plus the empty-graph case.
2. scan → out-edges → output `(%srcs, %b)`: 1-hop pairs, exercises the source
   gather.
3. Two-hop with carry, outputting `(%aFiltered, %c)`: exercises carry
   filtering end-to-end against hand-computed paths.
4. The in-edges variant of (2).
5. (2) re-run with `_chunkSize = 2` on a graph large enough to force several
   steps per loop: results must be identical regardless of chunking.

CMake mirrors the `add_storage_tests` function pattern, linking the
interpreter, dialect and storage libraries.

# Implementation order

1. `NLProgram.h/.cpp` — runtime structures, no MLIR.
2. `NLExecutor.h/.cpp` — handlers and entry point.
3. `NLTranslator.h/.cpp` — the MLIR walk.
4. CMake wiring for the library and tests.
5. Tests, then one build and test run to verify the milestone.

# Deferred work

- db → nl lowering and wiring the interpreter into the query path.
- Plan cache: split immutable `NLProgram` from per-run slot state, enabling
  concurrent executions of one translated program.
- LIMIT / early-termination channel (status return or exception).
- Chunk-fullness invariant: ragged chunks (highly selective steps producing
  near-empty chunks) erode the amortization argument; the fix belongs in the
  iterator contract (keep filling across input rows), not the interpreter.
- Fused superinstruction handlers for hot shapes (e.g. scan → expand → output):
  drop-in descriptor replacements, no architectural change.
- Per-element expression evaluation, when the dialect grows it: vectorized
  expression kernels first; compilation only behind adaptive tiering via MLIR
  lowering if profiles ever demand it.
