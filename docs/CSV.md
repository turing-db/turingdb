# LOAD CSV Specification

This document specifies the design and architecture for CSV loading functionality in TuringDB, modeled after Neo4j's `LOAD CSV` clause.

## Table of Contents

1. [Overview](#overview)
2. [Design Goals](#design-goals)
3. [Key Design Decisions](#key-design-decisions)
4. [Syntax](#syntax)
5. [Architecture](#architecture)
6. [Implementation Details](#implementation-details)
   - [AST Layer](#ast-layer)
   - [Analyzer Layer](#analyzer-layer)
   - [Plan Layer](#plan-layer)
   - [Pipeline Layer](#pipeline-layer)
   - [Storage Layer](#storage-layer)
   - [CSV Parser](#csv-parser)
   - [Expression Lowering](#expression-lowering)
   - [Memory Registration](#memory-registration)
   - [Dataframe Shape Propagation](#dataframe-shape-propagation)
   - [Error Handling](#error-handling)
   - [Restrictions](#restrictions)
7. [LOAD EMBEDDING CSV](#load-embedding-csv)
8. [Examples](#examples)
9. [Future Work](#future-work)
10. [Implementation State](#implementation-state)
11. [Appendix: RFC 4180 CSV Format](#appendix-rfc-4180-csv-format)

---

## Overview

TuringDB needs to support loading data from CSV files directly in Cypher queries. This enables:

- Bulk data import into the graph
- Embedding import for vector search use cases
- Data transformation and filtering during import

The design follows Neo4j's `LOAD CSV` semantics where possible, adapted for TuringDB's columnar architecture.

### Use Cases

1. **Bulk Import**: Loading nodes and relationships from CSV files (10GB-100GB scale)
2. **Embedding Import**: Loading vector embeddings with potentially hundreds of columns (e.g., 768-dimensional embeddings)
3. **Data Transformation**: Converting and validating data during import using Cypher expressions

---

## Design Goals

1. **Performance**: Support large CSV files (10GB-100GB) with chunked processing
2. **Columnar Alignment**: Produce columnar data structures compatible with TuringDB's storage
3. **Simplicity**: Avoid complex type systems (no variants, no runtime type dispatch)
4. **Composability**: `LOAD CSV` works as a reading statement, composable with `MATCH`, `CREATE`, etc.
5. **Error Handling**: Malformed lines are runtime errors by default

### Non-Goals (Deferred)

- `WITH` clause filtering (not yet implemented in TuringDB)
- S3/HTTP file sources (local files only for Phase 1)
- Custom delimiters (RFC 4180 standard CSV only)

---

## Key Design Decisions

### Decision 1: All CSV Values Are Strings

Unlike an earlier prototype that used lists of variants, this design treats all CSV values as strings. Type conversion is explicit via Cypher functions.

**Rationale**:
- Avoids variant type overhead and complexity
- Aligns with Neo4j's behavior
- Enables vectorized string-to-type conversion
- No changes needed to the expression evaluation system for heterogeneous types

**Implications**:
- `row[0]` returns a `String`, not a variant
- Users must explicitly convert: `toInteger(row[1])`, `toFloat(row[2])`
- Type errors surface at conversion time, not at CSV parsing time

### Decision 2: `row` is a StringTable Type

The `row` variable introduced by `LOAD CSV ... AS row` has a new `EvaluatedType::StringTable` type. This type:

- Supports integer indexing: `row[i]` returns `String`
- Supports header-based access: `row.columnName` returns `String` (when `WITH HEADERS`)
- Is not a general-purpose list type

**Rationale**:
- Special-case handling avoids implementing general list indexing
- Compile-time index resolution for literals enables efficient code generation
- Runtime indexing with bounds checking for non-literal expressions
- Clear semantics specific to CSV use case

### Decision 3: Index Expression

`row[e]` supports any integer-typed expression `e`. When `e` is a literal integer, the index is resolved at compile time for efficiency.

**Valid**: `row[0]`, `row[42]`, `row[i]`, `row[1+1]`

**Compile-time optimization**: When the index is a literal integer, the column access is resolved at compile time, avoiding runtime bounds checking overhead.

**Runtime indexing**: When the index is a non-literal expression, bounds checking occurs at runtime. If the index is out of bounds, a runtime error is raised.

### Decision 4: Single Column Storage (ColumnStringTable)

Instead of exposing N separate columns to the pipeline (one per CSV field), the CSV data is stored in a single `ColumnStringTable` column. This column contains multiple `ColumnString` instances inside (one per CSV field), with fields already split during parsing.

**Structure**:
- `ColumnStringTable`: Container holding N `ColumnString` columns (one per CSV field, already parsed/split)

**Rationale**:
- No need to read CSV twice (once for schema, once for data)
- Schema discovery happens at runtime as part of execution
- More flexible - can handle CSVs with varying column counts
- Simpler pipeline generation since column count is not needed upfront
- Single column abstraction simplifies dataframe management

**Type mappings**:
- `row` variable has type `StringTable` (analyzer) / `ColumnStringTable` (storage)
- `row[i]` returns `String` (analyzer) / `ColumnString*` (storage)

**Memory**: CSVSourceProcessor allocates the `ColumnStringTable` dynamically via `LocalMemory`.

### Decision 5: Chunked Processing

CSV files are processed in chunks using the standard `ChunkConfig::CHUNK_SIZE` to bound memory usage.

**Rationale**:
- 100GB files cannot be fully materialized in memory
- Uses the same chunk size as other processors for consistency
- Aligns with TuringDB's existing batch processing model (WriteProcessor collects creates)

### Decision 6: Malformed Lines Are Runtime Errors

Malformed CSV lines (wrong column count, unterminated quotes, encoding errors) cause a runtime error that aborts the query.

**Rationale**:
- Fail-fast behavior ensures data integrity
- Users are immediately aware of data quality issues
- Consistent with type conversion errors (e.g., `toInteger("abc")`)

---

## Syntax

### General LOAD CSV

```cypher
LOAD CSV 'path/to/file.csv' AS row
CREATE (n:Person {name: row[0], age: toInteger(row[1])})
```

```cypher
LOAD CSV 'path/to/file.csv' WITH HEADERS AS row
CREATE (n:Person {name: row.name, age: toInteger(row.age)})
```

### Error Handling Mode

By default, malformed lines cause a runtime error (`ON ERROR FAIL`). Use `ON ERROR SKIP` to skip malformed lines and continue processing:

```cypher
LOAD CSV 'path/to/file.csv' ON ERROR SKIP AS row
CREATE (n:Person {name: row[0], age: toInteger(row[1])})
```

```cypher
LOAD CSV 'path/to/file.csv' WITH HEADERS ON ERROR SKIP AS row
CREATE (n:Person {name: row.name, age: toInteger(row.age)})
```

### LOAD EMBEDDING CSV (Specialized)

```cypher
LOAD EMBEDDING CSV 'embeddings.csv' AS (id, embedding[1:768])
MATCH (n:Node {id: id})
SET n.embedding = embedding
```

### Grammar

```yacc
readingStatement
    : matchSt
    | unwindSt
    | callSt
    | loadCSVSt      // NEW
    ;

loadCSVSt
    : LOAD CSV STRING_LITERAL AS symbol
    | LOAD CSV STRING_LITERAL WITH HEADERS AS symbol
    | LOAD CSV STRING_LITERAL ON ERROR SKIP AS symbol
    | LOAD CSV STRING_LITERAL WITH HEADERS ON ERROR SKIP AS symbol
    | LOAD CSV STRING_LITERAL ON ERROR FAIL AS symbol
    | LOAD CSV STRING_LITERAL WITH HEADERS ON ERROR FAIL AS symbol
    ;

// For LOAD EMBEDDING CSV (Phase 2)
loadEmbeddingCSVSt
    : LOAD EMBEDDING CSV STRING_LITERAL AS
      OPAREN symbol COMMA symbol OBRACK INTEGER_LITERAL COLON INTEGER_LITERAL CBRACK CPAREN
    ;

// Index expression for row[e]
atomicExpr
    : propertyOrLabelExpr
    | atomicExpr OBRACK expr CBRACK   // NEW: row[0], row[i], row[1+1]
    ;
```

### Lexer Additions

```lex
CSV        { return token::CSV; }
EMBEDDING  { return token::EMBEDDING; }
SKIP       { return token::SKIP; }
FAIL       { return token::FAIL; }
```

**Note**: `ON` and `ERROR` are existing tokens.

---

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                           Query Pipeline                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌────────────────────┐    │
│  │   Parser     │───►│   Analyzer   │───►│ PipelineGenerator  │    │
│  │              │    │              │    │                    │    │
│  │ LoadCSVStmt  │    │ Type: String │    │ CSVSourceProcessor │    │
│  │ IndexExpr    │    │ Table        │    │                    │    │
│  └──────────────┘    └──────────────┘    └────────────────────┘    │
│                                                   │                  │
│                                                   ▼                  │
│                              ┌─────────────────────────────────┐    │
│                              │     Runtime Pipeline            │    │
│                              │                                 │    │
│                              │  CSVSourceProcessor             │    │
│                              │    │                            │    │
│                              │    ▼ (ColumnStringTable)        │    │
│                              │  FilterProcessor (optional)     │    │
│                              │    │                            │    │
│                              │    ▼                            │    │
│                              │  WriteProcessor                 │    │
│                              └─────────────────────────────────┘    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### File Structure

```
NEW FILES:
├── query/AST/stmt/LoadCSVStmt.h
├── query/AST/stmt/LoadCSVStmt.cpp
├── query/AST/expr/IndexExpr.h
├── query/AST/expr/IndexExpr.cpp
├── storage/columns/ColumnStringTable.h
├── storage/columns/ColumnStringTable.cpp
├── io/CSVParser.h
├── io/CSVParser.cpp
├── query/pipeline/processors/CSVSourceProcessor.h
├── query/pipeline/processors/CSVSourceProcessor.cpp

MODIFIED FILES:
├── query/parser/CypherParser.y          # Grammar additions
├── query/parser/CypherLexer.l           # CSV token
├── query/AST/decl/EvaluatedType.h       # StringTable, DenseVector types
├── query/analyzer/ReadStmtAnalyzer.h
├── query/analyzer/ReadStmtAnalyzer.cpp  # LoadCSVStmt analysis
├── query/analyzer/ExprAnalyzer.h
├── query/analyzer/ExprAnalyzer.cpp      # IndexExpr analysis
├── query/plan/PipelineGenerator.cpp     # CSV pipeline generation
├── query/plan/FunctionDecls.h
├── query/plan/FunctionDecls.cpp         # toInteger, toFloat, etc.
```

---

## Implementation Details

### AST Layer

#### LoadCSVStmt (`query/AST/stmt/LoadCSVStmt.h`)

```cpp
class LoadCSVStmt : public Stmt {
public:
    static LoadCSVStmt* create(CypherAST* ast,
                               std::string_view filePath,
                               Symbol* alias);

    const fs::Path& getFilePath() const;
    Symbol* getAlias() const;
    bool hasHeaders() const;
    bool skipOnError() const;
    VarDecl* getAliasDecl() const;

    void setHasHeaders(bool v);
    void setSkipOnError(bool v);
    void setAliasDecl(VarDecl* decl);

    Kind getKind() const override { return Kind::LOAD_CSV; }

private:
    fs::Path _filePath;
    Symbol* _alias {nullptr};
    VarDecl* _aliasDecl {nullptr};
    bool _hasHeaders {false};
    bool _skipOnError {false};
};
```

The AST and plan layers use `bool skipOnError` rather than a `CSVErrorMode` enum. This keeps the parser/AST/plan layers free of `CSVErrorMode.h`. The bool-to-enum conversion happens at the boundary in `PipelineGenerator::translateLoadCSVNode`.

#### IndexExpr (`query/AST/expr/IndexExpr.h`)

```cpp
class IndexExpr : public Expr {
public:
    static IndexExpr* create(CypherAST* ast, Expr* base, Expr* indexExpr);

    Expr* getBase() const;
    Expr* getIndexExpr() const;
    bool hasLiteralIndex() const;
    size_t getLiteralIndex() const;
    void setLiteralIndex(size_t index);

    Kind getKind() const override { return Kind::INDEX; }

private:
    Expr* _base {nullptr};
    Expr* _indexExpr {nullptr};
    bool _hasLiteralIndex {false};
    size_t _literalIndex {0};
};
```

When the analyzer detects a literal integer index (e.g. `row[0]`), it calls `setLiteralIndex()` so the expression lowering can resolve the field column at compile time.

#### PropertyExpr — CSV Header Access

`PropertyExpr` (`query/AST/expr/PropertyExpr.h`) gained a flag for CSV header access:

```cpp
bool _stringTableHeaderAccess {false};

bool isStringTableHeaderAccess() const;
void setStringTableHeaderAccess(bool v);
```

When the analyzer sees `row.name` where `row` is typed `StringTable`, it sets this flag. The expression lowering then resolves the header name to a field column index.

#### Type System

`EvaluatedType::StringTable` was added to `query/AST/decl/EvaluatedType.h`. This is the type of the `row` variable introduced by `LOAD CSV ... AS row`. Expressions on it yield `String`:

- `row[i]` → `String`
- `row.name` → `String`

### Analyzer Layer

#### ReadStmtAnalyzer (`query/analyzer/ReadStmtAnalyzer.cpp`)

Creates a `VarDecl` of type `StringTable` for the alias:

```cpp
void ReadStmtAnalyzer::analyze(LoadCSVStmt* loadCSV) {
    Symbol* alias = loadCSV->getAlias();
    // validate alias exists and not already declared
    VarDecl* decl = _ctxt->getOrCreateNamedVariable(
        _ast, EvaluatedType::StringTable, alias->getName());
    loadCSV->setAliasDecl(decl);
}
```

#### ExprAnalyzer — IndexExpr (`query/analyzer/ExprAnalyzer.cpp`)

Validates that `[]` is applied to a `StringTable` with an `Integer` index. Detects literal indices for compile-time optimization. Rejects negative literals.

```cpp
void ExprAnalyzer::analyzeIndexExpr(IndexExpr* expr) {
    analyzeExpr(expr->getBase());
    analyzeExpr(expr->getIndexExpr());
    // validate base is StringTable, index is Integer
    // if index is a literal integer >= 0, call expr->setLiteralIndex()
    expr->setType(EvaluatedType::String);
}
```

#### ExprAnalyzer — PropertyExpr on StringTable

When the base of a `PropertyExpr` is `StringTable`, the analyzer sets the `StringTableHeaderAccess` flag and types the result as `String`:

```cpp
if (varDecl->getType() == EvaluatedType::StringTable) {
    expr->setEntityVarDecl(varDecl);
    expr->setStringTableHeaderAccess(true);
    expr->setType(EvaluatedType::String);
    return ValueType::String;
}
```

### Plan Layer

#### LoadCSVNode (`query/plan/nodes/LoadCSVNode.h`)

```cpp
class LoadCSVNode : public PlanGraphNode {
public:
    LoadCSVNode(const fs::Path& path, bool hasHeaders,
                bool skipOnError, const VarDecl* aliasDecl);

    const fs::Path& getFilePath() const;
    bool hasHeaders() const;
    bool skipOnError() const;
    const VarDecl* getAliasDecl() const;

private:
    fs::Path _path;
    bool _hasHeaders {false};
    bool _skipOnError {false};
    const VarDecl* _aliasDecl {nullptr};
};
```

Opcode: `PlanGraphOpcode::LOAD_CSV`.

#### ReadStmtGenerator (`query/plan/ReadStmtGenerator.cpp`)

Converts `LoadCSVStmt` → `LoadCSVNode` and registers the alias as a producer:

```cpp
void ReadStmtGenerator::generateLoadCSVStmt(const LoadCSVStmt* stmt) {
    const VarDecl* aliasDecl = stmt->getAliasDecl();
    LoadCSVNode* loadCSVNode = _tree->create<LoadCSVNode>(
        stmt->getFilePath(), stmt->hasHeaders(),
        stmt->skipOnError(), aliasDecl);
    _variables->setProducer(aliasDecl, loadCSVNode);
}
```

### Pipeline Layer

#### PipelineGenerator — translateLoadCSVNode (`query/plan/PipelineGenerator.cpp`)

This is where the CSV pipeline is assembled:

```cpp
PipelineOutputInterface* PipelineGenerator::translateLoadCSVNode(
        LoadCSVNode* node) {
    // 1. Peek at file to discover field count and headers
    CSVFileInfo fileInfo;
    CSVParser::peekFileStructure(
        node->getFilePath(), node->hasHeaders(), fileInfo);
    _csvHeaders = fileInfo._headers;
    _csvFieldCount = fileInfo._fieldCount;

    // 2. Allocate ColumnStringTable with empty field columns
    auto* table = _mem->alloc<ColumnStringTable>();
    for (size_t i = 0; i < _csvFieldCount; i++) {
        table->addFieldColumn(
            _mem->alloc<ColumnStringTable::StringColumn>());
    }

    // 3. Convert bool → CSVErrorMode at the boundary
    const CSVErrorMode errorMode =
        node->skipOnError() ? CSVErrorMode::Skip : CSVErrorMode::Fail;

    // 4. Create CSVSourceProcessor
    auto* csvSource = CSVSourceProcessor::create(
        _pipeline, node->getFilePath(), node->hasHeaders(),
        errorMode, _csvFieldCount, table);

    // 5. Register ColumnStringTable in output dataframe
    PipelineBlockOutputInterface& output = csvSource->output();
    Dataframe* outDf = output.getDataframe();
    DataframeManager* dfMan = _pipeline->getDataframeManager();
    const ColumnTag tag = dfMan->allocTag();
    NamedColumn* namedCol = NamedColumn::create(dfMan, table, tag);
    outDf->addColumn(namedCol);

    // 6. Map alias VarDecl → ColumnTag
    _declToColumn[node->getAliasDecl()] = tag;

    _builder.getPendingOutput().setInterface(&output);
    return _builder.getPendingOutputInterface();
}
```

`PipelineGenerator` stores `_csvHeaders` and `_csvFieldCount` as member state, exposed via `getCSVHeaders()` for use by `ExprProgramGenerator`.

#### CSVSourceProcessor (`query/pipeline/processors/CSVSourceProcessor.h`)

```cpp
class CSVSourceProcessor : public Processor {
public:
    static CSVSourceProcessor* create(
        PipelineV2* pipeline, const fs::Path& path,
        bool hasHeaders, CSVErrorMode errorMode,
        size_t expectedFieldCount, ColumnStringTable* outputTable);

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineBlockOutputInterface& output();

private:
    fs::Path _path;
    bool _hasHeaders;
    CSVErrorMode _errorMode;
    size_t _expectedFieldCount;
    PipelineBlockOutputInterface _output;
    ColumnStringTable* _outputTable {nullptr};
    CSVParser* _parser {nullptr};
};
```

**Lifecycle**:

1. **create()**: Instantiated by `PipelineGenerator`, registered with pipeline
2. **prepare()**: Opens file, creates `CSVParser`
3. **execute()**: Reads one chunk via `_parser->readChunk()`. On EOF (0 rows returned), calls `finish()` and returns without calling `writeData()`. Otherwise calls `_output.getPort()->writeData()`.
4. **reset()**: Resets state for re-execution

The `execute()` method is critical — it must `return` after `finish()` to avoid writing stale data downstream:

```cpp
void CSVSourceProcessor::execute() {
    const size_t rowsRead =
        _parser->readChunk(_ctxt->getChunkSize(), _outputTable);
    if (rowsRead == 0) {
        // log statistics
        finish();
        return;
    }
    _output.getPort()->writeData();
}
```

### Storage Layer

#### ColumnStringTable (`storage/columns/ColumnStringTable.h`)

```cpp
class ColumnStringTable : public Column {
public:
    using StringColumn = ColumnVector<std::string>;

    ColumnStringTable();
    ~ColumnStringTable() override;

    size_t getFieldCount() const { return _columns.size(); }
    size_t getRowCount() const;
    size_t size() const override { return getRowCount(); }

    StringColumn* getFieldColumn(size_t index) const;
    void addFieldColumn(StringColumn* col);
    void clear();

    void assign(const Column* other) override;
    void assignFromLine(const Column* other,
                        size_t startLine, size_t rowCount) override;

    static consteval auto staticKind() { return _staticKind; }

private:
    static constexpr auto _staticKind =
        ColumnKind::code<ColumnStringTable>();
    std::vector<StringColumn*> _columns;
};
```

Key design points:
- Field columns are `ColumnVector<std::string>` (owned strings, not views — views would dangle after mmap unmap)
- Field columns are allocated externally from `LocalMemory` and added via `addFieldColumn()`
- Header names are **not** stored in the column — they live in `PipelineGenerator::_csvHeaders`
- `assign()` and `assignFromLine()` delegate to each field column, asserting field count match

### CSV Parser

#### CSVParser (`io/csv/CSVParser.h`)

```cpp
class CSVParser {
public:
    static constexpr size_t DEFAULT_MMAP_CHUNK_SIZE =
        512 * 1024 * 1024;

    CSVParser(const fs::Path& path, bool hasHeaders,
              CSVErrorMode errorMode, size_t expectedFieldCount,
              size_t mmapChunkSize = DEFAULT_MMAP_CHUNK_SIZE);
    ~CSVParser();

    size_t readChunk(size_t maxRows, ColumnStringTable* output);

    size_t getLinesRead() const;
    size_t getLinesSkipped() const;

    // RFC 4180 line parser (static, used by peekFileStructure too)
    static bool parseCSVLine(const std::string& line,
                             std::vector<std::string>& fields);

    // Discover field count and headers without full parse
    static void peekFileStructure(const fs::Path& path,
                                  bool hasHeaders,
                                  CSVFileInfo& info);
};
```

#### CSVErrorMode (`io/csv/CSVErrorMode.h`)

```cpp
enum class CSVErrorMode : uint8_t {
    Fail,
    Skip,
};
```

#### CSVFileInfo (`io/csv/CSVFileInfo.h`)

```cpp
struct CSVFileInfo {
    std::vector<std::string> _headers;
    size_t _fieldCount {0};
};
```

#### Parsing details

- **mmap-based I/O**: File mapped in 512MB chunks. Handles lines spanning chunk boundaries via a line buffer.
- **RFC 4180**: Quoted fields, escaped quotes (`""`), empty fields, CRLF/LF line endings.
- **BOM handling**: UTF-8 BOM (0xEF 0xBB 0xBF) skipped at file start.
- **peekFileStructure()**: mmaps a small region (64KB), parses the first one or two lines to discover field count and headers. Used by `PipelineGenerator` before creating the processor.
- **Error handling**: `CSVErrorMode::Fail` throws on malformed lines. `CSVErrorMode::Skip` logs a warning and continues. Statistics tracked via `_linesRead` / `_linesSkipped`.

### Expression Lowering

#### ExprProgramGenerator (`query/plan/ExprProgramGenerator.cpp`)

**IndexExpr** — `row[i]`:

Looks up the `ColumnStringTable` via the alias `VarDecl` → `ColumnTag` mapping, then returns the field column at the literal index. Dynamic (non-literal) indices throw at plan time.

```cpp
Column* ExprProgramGenerator::generateIndexExpr(
        const IndexExpr* indexExpr) {
    if (!indexExpr->hasLiteralIndex()) {
        throw PlannerException(
            "Dynamic CSV row indexing not yet supported");
    }
    // look up ColumnStringTable via VarDecl → ColumnTag
    auto* table = static_cast<ColumnStringTable*>(...);
    const size_t fieldIdx = indexExpr->getLiteralIndex();
    // bounds check against table->getFieldCount()
    return table->getFieldColumn(fieldIdx);
}
```

**PropertyExpr** — `row.name`:

Searches `PipelineGenerator::getCSVHeaders()` for the header name, then returns the corresponding field column.

```cpp
Column* ExprProgramGenerator::generatePropertyExpr(
        const PropertyExpr* propExpr) {
    if (propExpr->isStringTableHeaderAccess()) {
        // look up ColumnStringTable via VarDecl → ColumnTag
        auto* table = static_cast<ColumnStringTable*>(...);
        const auto& headers = _gen->getCSVHeaders();
        // linear search for header name → field index
        return table->getFieldColumn(i);
    }
    // ... normal property handling ...
}
```

**Type conversion functions** — `toInteger()`, `toFloat()`, `toBoolean()`:

Each takes a single `String` argument column and produces an optional result column via a conversion operator:

```cpp
if (funcName == "toInteger") {
    resCol = _gen->memory()
        .alloc<ColumnOptVector<types::Int64::Primitive>>();
    _exprProg->addInstr(OP_TO_INTEGER, resCol, argCol, nullptr);
}
// analogous for toFloat (OP_TO_FLOAT) and toBoolean (OP_TO_BOOLEAN)
```

The result is `ColumnOptVector<T>` (nullable) because the conversion can fail on unparseable values.

### Memory Registration

#### ContainerKind (`storage/columns/ContainerKind.h`)

`ColumnStringTable` is registered as a non-template type:

```cpp
using Types = KindTypes<
    TemplateKind<ColumnVector>,
    TemplateKind<ColumnConst>,
    TemplateKind<ColumnSet>,
    ColumnMask,
    ColumnStringTable>;
```

#### LocalMemory (`memory/LocalMemory.h`)

A memory pool is registered for `ColumnStringTable`:

```cpp
MakeMemoryPool<ColumnStringTable>::type
```

This enables `_mem->alloc<ColumnStringTable>()` and `_mem->allocSame(col)` for ColumnStringTable instances.

### Dataframe Shape Propagation

`ColumnStringTable` is a composite column — `allocSame()` creates an empty shell with no field columns. The `populateStringTableShape()` helper in `PipelineBuilder.cpp` handles this:

```cpp
void populateStringTableShape(LocalMemory* mem,
                              Column* dest, const Column* src) {
    if (dest->getKind() != ColumnStringTable::staticKind()) return;
    const auto* srcTable =
        static_cast<const ColumnStringTable*>(src);
    auto* dstTable = static_cast<ColumnStringTable*>(dest);
    for (size_t i = 0; i < srcTable->getFieldCount(); i++) {
        dstTable->addFieldColumn(
            mem->alloc<ColumnStringTable::StringColumn>());
    }
}
```

This is called from `duplicateDataframeShape()` after each `allocSame()` to ensure downstream dataframes have the right number of field columns.

### Error Handling

**CSV parsing errors** (depend on error mode):
- Wrong field count, unterminated quotes, invalid UTF-8
- `ON ERROR FAIL` (default): throws `TuringException`, aborts query
- `ON ERROR SKIP`: logs warning, increments `_linesSkipped`, continues

**Plan-time errors** (always abort):
- `row[i]` where `i >= fieldCount`: index out of range
- `row.name` where `name` not in headers: header not found
- `row[expr]` where `expr` is not a literal: not yet supported

**File errors** (always abort):
- File not found at `CSVSourceProcessor::prepare()` time

**Logging**: On completion, `CSVSourceProcessor` logs lines read and lines skipped (if any).

### Restrictions

**Phase 1 restrictions:**
- Single `LOAD CSV` per query (no Cartesian product / join semantics)
- Dynamic index expressions (`row[i]` where `i` is not a literal) throw at plan time
- Local files only (no S3/HTTP)
- Comma delimiter only (RFC 4180)
- No `WITH` clause filtering
- UTF-8 only (BOM handled)

---

## LOAD EMBEDDING CSV

A specialized syntax for efficiently loading vector embeddings.

### Syntax

```cypher
LOAD EMBEDDING CSV 'embeddings.csv' AS (id, embedding[1:768])
```

Where:
- `id` is bound to column 0, typed as `Integer` (UInt64)
- `embedding` is bound to columns 1-768, typed as `DenseVector`

### Grammar

```yacc
loadEmbeddingCSVSt
    : LOAD EMBEDDING CSV STRING_LITERAL AS
      OPAREN symbol COMMA symbol OBRACK INTEGER_LITERAL COLON INTEGER_LITERAL CBRACK CPAREN {
        $$ = LoadEmbeddingCSVStmt::create(ast, $4, $7, $9, $11, $13);
        LOC($$, @$);
      }
    ;
```

### AST Node

```cpp
class LoadEmbeddingCSVStmt : public Stmt {
public:
    static LoadEmbeddingCSVStmt* create(
        CypherAST* ast,
        std::string_view filePath,
        Symbol* idAlias,
        Symbol* embeddingAlias,
        int64_t startCol,
        int64_t endCol
    );

    const fs::Path& getFilePath() const { return _filePath; }
    Symbol* getIdAlias() const { return _idAlias; }
    Symbol* getEmbeddingAlias() const { return _embeddingAlias; }
    int64_t getStartColumn() const { return _startCol; }
    int64_t getEndColumn() const { return _endCol; }

    size_t getDimension() const { return _endCol - _startCol + 1; }

private:
    fs::Path _filePath;
    Symbol* _idAlias;        // Typed as Integer (UInt64)
    Symbol* _embeddingAlias; // Typed as DenseVector
    int64_t _startCol;
    int64_t _endCol;
};
```

### Type Bindings

- `id`: `EvaluatedType::Integer` (storage: `types::UInt64`)
- `embedding`: `EvaluatedType::DenseVector` (storage: dense float array)

### Specialized Processor

The embedding CSV processor:
1. Parses column 0 directly as UInt64 (no string intermediate)
2. Parses columns [startCol, endCol] (inclusive) directly as floats into dense vector
3. No `WITH HEADERS` support (always positional)

---

## Examples

### Basic Node Import

```cypher
LOAD CSV 'people.csv' AS row
CREATE (n:Person {
    name: row[0],
    age: toInteger(row[1]),
    salary: toFloat(row[2])
})
```

### Import with Headers

```cypher
LOAD CSV 'people.csv' WITH HEADERS AS row
CREATE (n:Person {
    name: row.name,
    age: toInteger(row.age),
    city: row.city
})
```

### Relationship Import

```cypher
LOAD CSV 'knows.csv' AS row
MATCH (a:Person {id: toInteger(row[0])}), (b:Person {id: toInteger(row[1])})
CREATE (a)-[:KNOWS {since: toInteger(row[2])}]->(b)
```

### Embedding Import

```cypher
LOAD EMBEDDING CSV 'node_embeddings.csv' AS (id, embedding[1:768])
MATCH (n:Document {id: id})
SET n.embedding = embedding
```

### Multiple LOAD CSV (Future)

```cypher
LOAD CSV 'nodes.csv' AS nodeRow
CREATE (n:Entity {id: toInteger(nodeRow[0]), name: nodeRow[1]})

// Then in a separate query:
LOAD CSV 'edges.csv' AS edgeRow
MATCH (a:Entity {id: toInteger(edgeRow[0])}), (b:Entity {id: toInteger(edgeRow[1])})
CREATE (a)-[:RELATED]->(b)
```

---

## Future Work

1. **WITH Clause Support**: Enable filtering during CSV processing
   ```cypher
   LOAD CSV 'file.csv' AS row
   WITH row WHERE toInteger(row.age) > 18
   CREATE (n:Adult {name: row.name})
   ```

2. **Remote File Sources**: S3, HTTP URLs

3. **Custom Delimiters**: TSV, semicolon-separated, etc.

4. **Schema Specification**: User-provided schema to skip auto-detection
   ```cypher
   LOAD CSV 'file.csv' WITH SCHEMA (name: STRING, age: INT) AS row
   ```

5. **Parallel CSV Parsing**: Split file for parallel processing

---

## Implementation State

Phase 1 status as of the `load-csv` branch.

### What's Implemented

The full pipeline is wired across all layers:

- **Parser/Grammar**: `LOAD CSV` as a `readingStatement` — all syntax variants (`WITH HEADERS`, `ON ERROR SKIP/FAIL`)
- **AST**: `LoadCSVStmt`, `IndexExpr` (with literal index optimization)
- **Analyzer**: `ReadStmtAnalyzer` types alias as `StringTable`; `ExprAnalyzer` handles `row[i]` and `row.name`
- **Plan**: `LoadCSVNode` produced by `ReadStmtGenerator`
- **Pipeline**: `CSVSourceProcessor` (mmap chunked reading), `CSVParser` (RFC 4180), `ColumnStringTable` storage
- **Expressions**: `ExprProgramGenerator` resolves `row[i]` to field columns and `row.name` via header lookup
- **Type conversions**: `toInteger()`, `toFloat()`, `toBoolean()`

### What's Tested

Only standalone `LOAD CSV + RETURN` queries (two regression test suites):

- `regress/load_csv/` — index access, header access, LIMIT
- `regress/load_csv_stations/` — real-world 42KB CSV, all cells validated against Python `csv.reader`

### Feature Matrix

| Feature | Spec | Implemented | Tested |
|---------|------|-------------|--------|
| `LOAD CSV ... RETURN` | Yes | Yes | Yes |
| `WITH HEADERS` | Yes | Yes | Yes |
| `ON ERROR SKIP/FAIL` | Yes | Yes | No |
| `row[i]` (literal index) | Yes | Yes | Yes |
| `row[expr]` (dynamic index) | Yes | No (throws) | — |
| `row.name` (header access) | Yes | Yes | Yes |
| `toInteger/toFloat/toBoolean` | Yes | Yes | No |
| `LOAD CSV + CREATE` | Yes | Untested | No |
| `LOAD CSV + MATCH` | Yes | Untested | No |
| `LOAD CSV + MATCH + CREATE` | Yes | Untested | No |
| `LIMIT` | Implicit | Yes | Yes |
| Multiple `LOAD CSV` per query | Phase 2 | No | — |
| `WITH` clause filtering | Phase 2 | No | — |
| Remote sources (S3/HTTP) | Phase 2 | No | — |
| Custom delimiters | Phase 2 | No | — |
| `LOAD EMBEDDING CSV` | Phase 2 | No | — |

### Key Gaps

1. **Graph composability untested**: `LOAD CSV` parses as a `readingStatement` and should compose with `MATCH`/`CREATE`, but no regression test exercises this path.
2. **Dynamic indexing**: `row[expr]` where `expr` is not a literal integer throws at plan time.
3. **Error mode and type conversions**: Implemented but not covered by regression tests.

---

## Appendix: RFC 4180 CSV Format

The CSV parser follows RFC 4180:

1. Each record is on a separate line, delimited by CRLF (or LF)
2. Fields are separated by commas
3. Fields containing commas, quotes, or newlines must be quoted with double-quotes
4. Double-quotes within quoted fields are escaped by doubling: `""`
5. Spaces are significant and not trimmed
6. The first line may be a header line (same format as data lines)

Example:
```csv
name,description,value
"Smith, John","A ""quoted"" value",42
Alice,Simple value,100
```
