# TuringDB Development Notes

## Architecture Principles
- Module dependency: `memory` depends on `storage`, NOT vice versa. Storage layer must never include LocalMemory.h
- Columns must be allocated from LocalMemory (pool allocator), never via `new`
- Never use `const_cast` -- restructure code instead
- Never use `dynamic_cast` for column downcasts -- use `static_cast` (types are known at compile time)
- Never use `std::shared_ptr` -- use raw pointers for arguments, `std::unique_ptr` only for ownership

## Column Type System
- `types::String::Primitive` = `std::string_view` (non-owning)
- `ColumnOptVector<T>` = `ColumnVector<std::optional<T>>`
- To register a new column type in LocalMemory: add to `ContainerKind::Types` (ContainerKind.h), add `MakeMemoryPool` entry (LocalMemory.h), implement `staticKind()` and `_staticKind` members
- Non-template column types (like ColumnMask, ColumnStringTable) must be added to ContainerKind::Types directly
- `allocSame()` uses `ColumnAllocator` which calls the memory pool's `alloc()` with no args -- new instance is always empty/default
- String data in columns: use `ColumnVector<std::string>` for owned strings, NOT `ColumnVector<std::string_view>` (views dangle)
- Non-POD column types need a .cpp file even if just for the destructor (e.g. `~ColumnStringTable() = default;`)

## Pipeline Architecture
- `duplicateDataframeShape()` creates new empty columns via `allocSame()`
- Composite columns (like ColumnStringTable) need external sub-column setup after `allocSame()` -- handled by `populateStringTableShape()` helper in PipelineBuilder.cpp
- Don't put shape initialization logic inside the column class itself (code smell: storage depending on memory)
- CSVSourceProcessor::execute() must `return` after `finish()` to avoid writing stale data downstream
- When a source processor finishes (returns 0 rows), don't call `writeData()`

## LOAD CSV Implementation
- ColumnStringTable stores `std::vector<Column*>` of `ColumnVector<std::string>` field columns
- Sub-columns are allocated externally from LocalMemory and added via `addFieldColumn()`
- `resolveCSVExpressions()` in ExprProgramGenerator maps IndexExpr/PropertyExpr to field columns
- CSVParser uses mmap-based I/O; `parseFileStructure()` peeks at headers/field count without needing a ColumnStringTable
- CSVSourceProcessor::peekFile() uses CSVParser::parseFileStructure() (not hand-rolled parsing)
- Type alias convention: `using StringColumn = ColumnVector<std::string>` (defined in ColumnStringTable.h)
- **Dependency rule**: AST (LoadCSVStmt) and plan (LoadCSVNode) use `bool skipOnError` -- NOT `CSVErrorMode` enum. This keeps the parser/AST/plan free of `CSVErrorMode.h`. The bool-to-enum conversion happens at the boundary in `PipelineGenerator::translateLoadCSVNode`.

## User Preferences
- User is strict about code quality: minimal, no fluff, no memory waste
- Don't change unrelated code or add unnecessary abstractions
- Don't rename or restructure existing patterns from main branch without being asked
- Check `git diff main` before making changes to verify current state

## Build
- Always build from `build/` directory: `make -j8`
- Regression tests: `make run_regress`
- Unit tests: `ctest` or `ctest --output-on-failure`

## Code Style Feedback
- [Braces and memory access patterns](feedback_braces_and_style.md)
- [Coding style preferences](feedback_coding_style.md) — POD defaults, getX() getters, no return-by-value, impl in .cpp, destructors in .cpp, throw TuringException
- [Functions vs operators](feedback_functions_vs_operators.md) — Embedding ops should be functions, not binary operators
- [Private member ordering](feedback_private_member_ordering.md) — Variables before functions in private sections
- [Return type same line](feedback_return_type_same_line.md) — Return type and function name on one line
- [Code spacing](feedback_code_spacing.md) — Blank lines between logical groups, don't write compact code
- [Const and span preferences](feedback_const_and_span.md) — const on locals, std::span for non-owning references
- [No auto for casts](feedback_no_auto_for_casts.md) — Explicit types at static_cast sites, hoist getters into locals
- [Friend placement](feedback_friend_placement.md) — `friend` declarations at top of `public:`, no `class` keyword
- [ObjectMap reserve/publish](feedback_objectmap_reserve_publish.md) — reserve name before the load, publish after, work in between

## Distributed / Replication Design
- [Distribution design docs + unified identity model](reference_distribution_design_docs.md) — REPLICATE.md / GRAPHHUB.md / hub-spec live in sibling clones ($HOME); unified identity model in docs/IDENTITY_MODEL.md

## Workflow Preferences
- [Test-first: write failing test before fix](feedback_test_first.md)
- [Explicit CI runner labels](feedback_explicit_ci_runner_labels.md) — put real runner labels in each workflow's os_list + `runs-on: ${{ matrix.os }}`, not a computed ternary / runner-group object
