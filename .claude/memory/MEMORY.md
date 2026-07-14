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

- @feedback_braces_and_style.md — Braces on all bodies, memory access patterns
- @feedback_coding_style.md — POD defaults, getX() getters, no return-by-value, impl in .cpp, destructors in .cpp, throw TuringException
- @feedback_functions_vs_operators.md — Embedding ops should be functions, not binary operators
- @feedback_private_member_ordering.md — Variables before functions in private sections
- @feedback_return_type_same_line.md — Return type and function name on one line
- @feedback_code_spacing.md — Blank lines between logical groups, don't write compact code
- @feedback_const_and_span.md — const on locals, std::span for non-owning references
- @feedback_no_auto_for_casts.md — Explicit types at static_cast sites, hoist getters into locals
- @feedback_friend_placement.md — `friend` declarations at top of `public:`, no `class` keyword
- @feedback_objectmap_reserve_publish.md — reserve name before the load, publish after, work in between
- @feedback_no_shared_ptr.md — never introduce shared_ptr; raw pointer / reference / unique_ptr instead
- @feedback_no_abbreviations.md — spell identifiers out (rowGroup, not rg; column, not col)
- @feedback_size_t_for_indices.md — size_t for indices and counts in new code, narrow at API boundaries
- @feedback_switch_case_break.md — return inside the case body, break still written aligned to case
- @feedback_naming_throw.md — throw-prefixed names (throwError) for helpers that always throw
- @feedback_no_k_prefix_constants.md — no Google-style k-prefix; use descriptive constant names
- @feedback_exit_status_macros.md — return EXIT_SUCCESS / EXIT_FAILURE from main, not 0 / 1
- @feedback_cpp_using_namespace.md — `using namespace db;` at top of .cpp, no body-wrapping namespace
- @feedback_call_arg_alignment.md — wrapping a call: one argument per line, aligned under the first
- @feedback_no_unused_param_comments.md — don't wrap unused parameter names in /*comments*/
- @feedback_no_void_cast_unused.md — don't add `(void)param;` lines for unused parameters
- @feedback_pipeline_exception.md — FatalException in storage, PipelineException in pipeline
- @feedback_anon_namespace_at_top.md — anonymous namespace block goes at the top of the .cpp, after `using namespace db;`
- @feedback_no_namespace_comment.md — no `// namespace` comment on a closing brace
- @feedback_no_default_ctors_in_header.md — define ctors and dtors in .cpp, never `= default` in headers
- @feedback_no_friends.md — prefer targeted public getters/setters over `friend`
- @feedback_pass_through_extracted_data.md — reuse caller-extracted data as a parameter, don't re-extract in the callee
- @feedback_parquet_helpers.md — tools/turing-parquet: multi-step ops on Parquet types get a dedicated ParquetXxx class
- @feedback_dump_object_bytes.md — bulk-write trivially-copyable object bytes, layouts pinned by central static_asserts; no staging
- @feedback_doc_utility_functions.md — one-line WHAT comment above each free function in tool .cpp drivers
- @feedback_lean_error_messages.md — keep thrown messages lean; no env/config override hints, no filler adjectives
- @feedback_count_suffix.md — name count variables with a Count suffix (currentDataPartsCount, not currentDataParts)

## Workflow Preferences
- @feedback_test_first.md — write failing test before fix
- @feedback_test_on_simpledb.md — use real SimpleGraph, not a bespoke minimal graph, esp. for bugs reported on simpledb
- @feedback_separate_test_file.md — new test → its own .cpp + CMake target, don't append to an existing test file
- @feedback_explicit_ci_runner_labels.md — put real runner labels in each workflow's os_list + `runs-on: ${{ matrix.os }}`, not a computed ternary / runner-group object
- @feedback_no_build_during_iteration.md — don't build after every micro-edit; wait until the user asks
- @feedback_no_redundant_cmake.md — don't run `cmake ..` before `make`; make reconfigures itself

## Project context
- @project_partitioning.md — partitioning is near-term; some customers asked for METIS-style structural partitioning
- @project_replication_motivation.md — replication driven by BOTH scaling past single-node memory AND write availability

## Build & CI
- @feedback_no_macos_strip.md — wheel build's `_strip_binary` is Linux-only; skip strip on macOS (dylib/codesign issues)

## Codebase references
- @reference_bioassert_throws.md — bioassert throws a catchable FatalException (TuringException); the abort() is dead code
- @reference_gcc_maybe_uninit_sort.md — sort an index permutation to dodge GCC's -Wmaybe-uninitialized; a call-site pragma can't suppress it
- @reference_no_string_predicates.md — no CONTAINS / STARTS WITH / ENDS WITH; StringOperator existing in the AST does NOT imply support
- @reference_change_visibility.md — within a change MATCH sees the COMMITted tip (read-your-own-writes works after COMMIT); after SUBMIT the change is gone, checkout head to see committed data
