# LOAD CSV Specification

This document specifies the design and architecture for CSV loading functionality in TuringDB, modeled after Neo4j's `LOAD CSV` clause.

## Table of Contents

1. [Overview](#overview)
2. [Design Goals](#design-goals)
3. [Key Design Decisions](#key-design-decisions)
4. [Syntax](#syntax)
5. [Architecture](#architecture)
6. [Implementation Details](#implementation-details)
   - [AST Nodes](#ast-nodes)
   - [Type System Additions](#type-system-additions)
   - [ColumnStringTable Storage](#columnstringtable-storage)
   - [CSV Parser](#csv-parser)
   - [CSV Source Processor](#csv-source-processor)
   - [Memory Management](#memory-management)
   - [Analyzer Changes](#analyzer-changes)
   - [Pipeline Generator](#pipeline-generator)
   - [Expression Lowering](#expression-lowering-exprprogramgenerator)
   - [Conversion Functions](#conversion-functions)
   - [Error Handling](#error-handling)
   - [Edge Cases](#edge-cases)
   - [File Path Handling](#file-path-handling)
   - [Encoding](#encoding)
   - [Concurrency](#concurrency)
   - [Restrictions (Phase 1)](#restrictions-phase-1)
   - [Stmt Type Registration](#stmt-type-registration)
   - [Progress and Statistics](#progress-and-statistics)
7. [LOAD EMBEDDING CSV](#load-embedding-csv)
8. [Examples](#examples)
9. [Future Work](#future-work)
10. [Appendix: RFC 4180 CSV Format](#appendix-rfc-4180-csv-format)

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

### AST Nodes

#### LoadCSVStmt

```cpp
// query/AST/stmt/LoadCSVStmt.h
#pragma once

#include "stmt/Stmt.h"
#include "io/Path.h"

namespace db {

class CypherAST;
class Symbol;

// Error handling mode for malformed CSV lines
enum class CSVErrorMode : uint8_t {
    Fail,   // Default: abort query on malformed line
    Skip,   // Skip malformed lines and continue
};

class LoadCSVStmt : public Stmt {
public:
    friend CypherAST;

    static LoadCSVStmt* create(CypherAST* ast,
                                std::string_view filePath,
                                Symbol* alias);

    const fs::Path& getFilePath() const { return _filePath; }
    Symbol* getAlias() const { return _alias; }

    bool hasHeaders() const { return _hasHeaders; }
    void setHasHeaders(bool v) { _hasHeaders = v; }

    CSVErrorMode getErrorMode() const { return _errorMode; }
    void setErrorMode(CSVErrorMode mode) { _errorMode = mode; }

private:
    LoadCSVStmt(std::string_view filePath, Symbol* alias);
    ~LoadCSVStmt() = default;

    fs::Path _filePath;
    Symbol* _alias;
    bool _hasHeaders {false};
    CSVErrorMode _errorMode {CSVErrorMode::Fail};
};

}
```

#### IndexExpr

```cpp
// query/AST/expr/IndexExpr.h
#pragma once

#include "expr/Expr.h"

namespace db {

class CypherAST;

class IndexExpr : public Expr {
public:
    friend CypherAST;

    static IndexExpr* create(CypherAST* ast, Expr* base, Expr* indexExpr);

    Expr* getBase() const { return _base; }
    Expr* getIndexExpr() const { return _indexExpr; }

    // Returns true if the index expression is a literal integer
    bool hasLiteralIndex() const { return _hasLiteralIndex; }

    // Returns the literal index value (only valid if hasLiteralIndex() is true)
    size_t getLiteralIndex() const { return _literalIndex; }

    // Called by analyzer when the index is a literal
    void setLiteralIndex(size_t index) { _hasLiteralIndex = true; _literalIndex = index; }

    Kind getKind() const override { return Kind::INDEX; }

private:
    IndexExpr(Expr* base, Expr* indexExpr);
    ~IndexExpr() = default;

    Expr* _base;
    Expr* _indexExpr;
    bool _hasLiteralIndex {false};
    size_t _literalIndex {0};
};

}
```

**Note**: Add `INDEX` to `Expr::Kind` enum.

### Type System Additions

```cpp
// query/AST/decl/EvaluatedType.h
enum class EvaluatedType : uint8_t {
    Invalid = 0,

    NodePattern,
    EdgePattern,
    GraphPath,

    Null,
    Integer,
    Double,
    String,
    Char,
    Bool,
    List,
    Map,
    Wildcard,
    Tuple,
    ValueType,

    StringTable,       // NEW: CSV row, supports [int] and .columnName
    DenseVector,       // NEW: Dense float vector for embeddings

    _SIZE,
};
```

**Type relationships**:
- `StringTable`: Type of the `row` variable from `LOAD CSV ... AS row`
- `row[i]` expression returns `String` type, extracting the `ColumnString*` from the `ColumnStringTable`

Update `EvaluatedTypeName` accordingly.

### ColumnStringTable Storage

```cpp
// storage/columns/ColumnStringTable.h
#pragma once

#include "storage/columns/Column.h"
#include "storage/columns/ColumnString.h"

namespace db {

// Container column holding multiple ColumnString instances (one per CSV field).
// Fields are already parsed and split during CSV reading.
class ColumnStringTable : public Column {
public:
    ColumnStringTable();
    ~ColumnStringTable() override;

    // Number of CSV columns (fields per row)
    size_t getFieldCount() const { return _columns.size(); }

    // Number of rows
    size_t getRowCount() const;

    // Access a specific field column
    ColumnString* getFieldColumn(size_t fieldIndex);
    const ColumnString* getFieldColumn(size_t fieldIndex) const;

    // Add a new field column (called during parsing as columns are discovered)
    void addFieldColumn(ColumnString* column);

    // Clear all data for reuse
    void clear();

private:
    std::vector<ColumnString*> _columns;  // One per CSV field
};

}
```

**Design notes**:
- `ColumnStringTable` owns the field columns and manages their lifecycle
- `row[i]` extracts the `ColumnString*` pointer at index `i` from the table
- Field columns are allocated via `LocalMemory` by CSVSourceProcessor
- Header names (for `WITH HEADERS`) are stored separately in the processor, not in the column

### CSV Parser

The CSV parser uses memory-mapped I/O (mmap) for efficient reading of large files. The file is mapped in large chunks (512MB) to balance memory usage with I/O efficiency.

**Design Rationale**:
- mmap avoids double-buffering (kernel buffer → user buffer)
- Large chunk sizes minimize system call overhead
- Sequential access pattern benefits from kernel read-ahead
- Memory can be released incrementally as chunks are processed

```cpp
// io/CSVParser.h
#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "io/Path.h"
#include "storage/columns/ColumnStringTable.h"

namespace db {

class LocalMemory;

class CSVParser {
public:
    // Default mmap chunk size: 512MB
    static constexpr size_t DEFAULT_MMAP_CHUNK_SIZE = 512 * 1024 * 1024;

    CSVParser(const fs::Path& path,
              bool hasHeaders,
              CSVErrorMode errorMode,
              LocalMemory* memory,
              size_t mmapChunkSize = DEFAULT_MMAP_CHUNK_SIZE);
    ~CSVParser();

    CSVParser(const CSVParser&) = delete;
    CSVParser& operator=(const CSVParser&) = delete;

    // Read up to maxRows into the output ColumnStringTable
    // Returns number of rows read (0 at EOF)
    // On malformed lines: throws if errorMode is Fail, skips if Skip
    // Field columns are allocated dynamically via LocalMemory
    size_t readChunk(size_t maxRows, ColumnStringTable* output);

    // Header access (only valid after first readChunk if hasHeaders was true)
    const std::vector<std::string>& getHeaders() const { return _headers; }
    size_t getHeaderIndex(std::string_view name) const;

    size_t getLinesRead() const { return _linesRead; }
    size_t getLinesSkipped() const { return _linesSkipped; }  // Only incremented in Skip mode

private:
    int _fd {-1};                          // File descriptor
    size_t _fileSize {0};                  // Total file size
    size_t _fileOffset {0};                // Current position in file
    size_t _mmapChunkSize;                 // Size of each mmap region

    void* _mappedRegion {nullptr};         // Current mmap region
    size_t _mappedOffset {0};              // File offset of mapped region
    size_t _mappedSize {0};                // Size of mapped region

    std::string_view _remaining;           // Unparsed data in current region
    bool _hasHeaders;
    CSVErrorMode _errorMode;
    std::vector<std::string> _headers;     // Populated from first line if hasHeaders
    size_t _fieldCount {0};                // Discovered from first data row

    LocalMemory* _memory;                  // For allocating field columns
    size_t _linesRead {0};
    size_t _linesSkipped {0};              // Lines skipped due to errors (Skip mode only)

    // Memory mapping
    bool mapNextChunk();
    void unmapCurrentChunk();

    // RFC 4180 parsing
    bool parseLine(std::string_view& input, std::vector<std::string>& fields);
    bool parseField(std::string_view& input, std::string& output);

    // Handle line that spans mmap chunk boundary
    bool handlePartialLine(std::string& lineBuffer);

    // Ensure output has enough field columns, allocating if needed
    void ensureFieldColumns(ColumnStringTable* output, size_t fieldCount);
};

}
```

### CSV Source Processor

```cpp
// query/pipeline/processors/CSVSourceProcessor.h
#pragma once

#include "Processor.h"
#include "io/CSVParser.h"
#include "io/Path.h"
#include "dataframe/ColumnTag.h"
#include "interfaces/PipelineBlockOutputInterface.h"
#include "storage/columns/ColumnStringTable.h"

namespace db {

class LocalMemory;

class CSVSourceProcessor : public Processor {
public:
    // Factory method - all processors are created via PipelineV2
    static CSVSourceProcessor* create(PipelineV2* pipeline,
                                       const fs::Path& path,
                                       bool hasHeaders,
                                       CSVErrorMode errorMode,
                                       ColumnTag outputTag);

    // Processor interface
    std::string describe() const override;
    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    // Output interface - produces a single ColumnStringTable
    PipelineBlockOutputInterface& output() { return _output; }

    // Header access (only valid after first execute() if hasHeaders)
    const std::vector<std::string>& getHeaders() const;
    size_t getHeaderIndex(std::string_view name) const;

private:
    fs::Path _path;
    bool _hasHeaders;
    CSVErrorMode _errorMode;
    ColumnTag _outputTag;          // Tag for the ColumnStringTable output

    LocalMemory* _memory {nullptr};       // From ExecutionContext
    CSVParser* _parser {nullptr};         // Owned, created in prepare()
    ColumnStringTable* _outputTable {nullptr};  // Allocated via LocalMemory
    PipelineBlockOutputInterface _output;

    CSVSourceProcessor(const fs::Path& path,
                       bool hasHeaders,
                       CSVErrorMode errorMode,
                       ColumnTag outputTag);
    ~CSVSourceProcessor() override;
};

}
```

#### Processor Lifecycle

1. **create()**: Called by `PipelineGenerator` to instantiate the processor and register it with the pipeline
2. **prepare()**: Called once before execution starts; gets `LocalMemory` from context, opens file, creates `CSVParser`, allocates `ColumnStringTable`
3. **execute()**: Called repeatedly by the pipeline executor; reads one chunk of rows into `ColumnStringTable` and writes to output
4. **reset()**: Called to reset state for re-execution (e.g., if pipeline needs to restart)

#### Output Interface

The processor uses `PipelineBlockOutputInterface` which:
- Produces a `Dataframe` with a single `NamedColumn` containing the `ColumnStringTable`
- The `ColumnStringTable` internally holds N `ColumnString` instances (one per CSV field)
- Column is identified by the single pre-allocated `ColumnTag`
- Downstream processors access individual `ColumnString*` fields via `row[i]` indexing

### Memory Management

#### LocalMemory Allocation

`CSVSourceProcessor` obtains a `LocalMemory` instance from the `ExecutionContext` during `prepare()`. All column allocations go through `LocalMemory`:

- `ColumnStringTable` is allocated via `LocalMemory`
- Each `ColumnString` within the table is allocated via `LocalMemory`
- Memory is released when the pipeline execution completes

#### String Ownership

Strings parsed from CSV are **copied** into `ColumnString` storage, not referenced via `string_view`. This is necessary because:

1. The mmap region is unmapped after each chunk is processed
2. Strings may span mmap chunk boundaries and require assembly
3. Quoted fields require unescaping (removing doubled quotes)

The copy overhead is acceptable because:
- String data is typically small relative to the file size
- Memory is bounded by `ChunkConfig::CHUNK_SIZE`
- Columnar string storage uses efficient arena allocation via `LocalMemory`

### Analyzer Changes

#### ReadStmtAnalyzer

```cpp
// In ReadStmtAnalyzer.cpp

void ReadStmtAnalyzer::analyze(const LoadCSVStmt* stmt) {
    // Create variable declaration for the alias (e.g., "row")
    Symbol* alias = stmt->getAlias();

    if (_ctxt->hasDecl(alias->getName())) {
        throwError("Variable already declared", alias);
    }

    // Type the alias as StringTable
    VarDecl* varDecl = _ctxt->getOrCreateNamedVariable(
        _ast,
        EvaluatedType::StringTable,
        alias->getName()
    );

    alias->setVarDecl(varDecl);

    // Note: Schema validation (column count, header names) happens
    // at pipeline generation time, not here
}
```

#### ExprAnalyzer

```cpp
// In ExprAnalyzer.cpp

void ExprAnalyzer::analyzeIndexExpr(IndexExpr* expr) {
    analyzeExpr(expr->getBase());
    analyzeExpr(expr->getIndexExpr());

    EvaluatedType baseType = expr->getBase()->getType();

    if (baseType != EvaluatedType::StringTable) {
        throwError("Index operator [] only supported on CSV row variables", expr);
    }

    // Index expression must be integer-typed
    EvaluatedType indexType = expr->getIndexExpr()->getType();
    if (indexType != EvaluatedType::Integer) {
        throwError("Index expression must be of type Integer", expr->getIndexExpr());
    }

    // Check if the index is a literal integer for compile-time optimization
    if (expr->getIndexExpr()->getKind() == Expr::Kind::LITERAL) {
        const LiteralExpr* lit = static_cast<const LiteralExpr*>(expr->getIndexExpr());
        int64_t value = lit->getIntegerValue();
        if (value < 0) {
            throwError("Index must be non-negative", expr->getIndexExpr());
        }
        expr->setLiteralIndex(static_cast<size_t>(value));
    }

    // Result type is String (extracts ColumnString* from ColumnStringTable)
    expr->setType(EvaluatedType::String);
}

// For row.columnName (in analyzePropertyExpr)
ValueType ExprAnalyzer::analyzePropertyExpr(PropertyExpr* expr, ...) {
    // ... existing code ...

    // Check if base is StringTable (CSV row with headers)
    if (baseType == EvaluatedType::StringTable) {
        // This is a header-based column access: row.columnName
        // Store the column name; actual index resolution happens at runtime
        expr->setType(EvaluatedType::String);
        expr->setCSVHeaderAccess(true);  // New flag
        return ValueType::String;  // Underlying value type
    }

    // ... existing property access code ...
}
```

### Pipeline Generator

```cpp
// In PipelineGenerator.cpp

void PipelineGenerator::generate(const LoadCSVStmt* stmt) {
    // 1. Allocate a single ColumnTag for the ColumnStringTable output
    ColumnTag csvTag = _dataframeManager->allocTag();

    // 2. Create source processor (no schema reading needed upfront)
    auto csvSource = CSVSourceProcessor::create(
        _pipeline,
        stmt->getFilePath(),
        stmt->hasHeaders(),
        stmt->getErrorMode(),
        csvTag
    );

    // 3. Register the alias variable with its ColumnTag in PipelineBuilder
    // Uses existing variable registration mechanism
    VarDecl* aliasDecl = stmt->getAlias()->getVarDecl();
    _pipelineBuilder->registerVariable(aliasDecl, csvTag);

    // 4. Add to pipeline
    _pipeline->setSource(csvSource);
}
```

**Note**: Schema discovery (column count, headers) happens at runtime during `CSVParser::readChunk()`. Header name validation for `row.columnName` access is deferred to runtime - if the header doesn't exist, a runtime error is raised.

### Expression Lowering (ExprProgramGenerator)

This section describes how `row[i]` and `row.columnName` are lowered to executable form by `ExprProgramGenerator`.

#### Column Tag Allocation

During pipeline generation, a single `ColumnTag` is allocated for the `ColumnStringTable` and registered via the standard `PipelineBuilder::registerVariable()` mechanism.

#### ExprProgramGenerator Handling

`ExprProgramGenerator` is extended to handle `IndexExpr`:

```cpp
// In ExprProgramGenerator.cpp

void ExprProgramGenerator::generate(const IndexExpr* expr) {
    // Get the VarDecl from the base SymbolExpr
    const SymbolExpr* baseExpr = static_cast<const SymbolExpr*>(expr->getBase());
    VarDecl* varDecl = baseExpr->getSymbol()->getVarDecl();

    // Look up the ColumnTag via standard variable lookup
    ColumnTag tableTag = _pipelineBuilder->getVariableTag(varDecl);

    if (expr->hasLiteralIndex()) {
        // Compile-time optimization: index is known at compile time
        size_t fieldIndex = expr->getLiteralIndex();

        // Emit instruction to extract the ColumnString* at fieldIndex
        // from the ColumnStringTable (no runtime bounds check needed if
        // schema is validated at prepare time)
        emitStringTableIndexLiteral(tableTag, fieldIndex);
    } else {
        // Runtime indexing: generate code to evaluate the index expression
        generate(expr->getIndexExpr());

        // Emit instruction to extract the ColumnString* at runtime index
        // from the ColumnStringTable (includes bounds checking)
        emitStringTableIndexDynamic(tableTag);
    }
}

void ExprProgramGenerator::generate(const PropertyExpr* expr) {
    if (expr->isCSVHeaderAccess()) {
        // Get VarDecl and column name
        VarDecl* varDecl = /* from base SymbolExpr */;
        std::string_view columnName = expr->getPropertyName();

        // Look up the ColumnTag via standard variable lookup
        ColumnTag tableTag = _pipelineBuilder->getVariableTag(varDecl);

        // Emit instruction to extract the ColumnString* by header name
        // Header name -> field index resolution happens at runtime
        emitStringTableIndexByHeader(tableTag, columnName);
        return;
    }

    // ... existing property access logic ...
}
```

**Runtime behavior**:
- `emitStringTableIndexLiteral(tag, index)`: Extracts the `ColumnString*` at compile-time known `index` from the `ColumnStringTable`. Bounds can optionally be validated at prepare time if schema is known.
- `emitStringTableIndexDynamic(tag)`: Pops the index from the evaluation stack and extracts the `ColumnString*` at that index from the `ColumnStringTable`. Includes runtime bounds checking; raises a runtime error if index is out of bounds or negative.
- `emitStringTableIndexByHeader(tag, name)`: Looks up header name in `CSVParser::getHeaders()`, then extracts the corresponding `ColumnString*`
- If index is out of bounds or header not found, a runtime error is raised

### Conversion Functions

Add to `FunctionDecls`:

```cpp
// query/plan/FunctionDecls.h

// Type conversion functions for CSV data
struct ToIntegerFunc {
    static constexpr const char* name = "toInteger";
    static EvaluatedType returnType() { return EvaluatedType::Integer; }
    static void validate(const ExprChain* args);  // Expects 1 String arg
};

struct ToFloatFunc {
    static constexpr const char* name = "toFloat";
    static EvaluatedType returnType() { return EvaluatedType::Double; }
    static void validate(const ExprChain* args);  // Expects 1 String arg
};

struct ToBooleanFunc {
    static constexpr const char* name = "toBoolean";
    static EvaluatedType returnType() { return EvaluatedType::Bool; }
    static void validate(const ExprChain* args);  // Expects 1 String arg
};

struct ToStringFunc {
    static constexpr const char* name = "toString";
    static EvaluatedType returnType() { return EvaluatedType::String; }
    static void validate(const ExprChain* args);  // Expects 1 numeric/bool arg
};
```

Implementation in `FuncEvalNode`:

| Function | Input | Output | Error Behavior |
|----------|-------|--------|----------------|
| `toInteger(s)` | String | Integer (int64_t) | Runtime error if unparseable |
| `toFloat(s)` | String | Double | Runtime error if unparseable |
| `toBoolean(s)` | String | Bool | "true"/"false" case-insensitive; error otherwise |
| `toString(v)` | Integer/Double/Bool | String | Format as string |

### Error Handling

#### CSV Parsing Errors

These errors occur when a line cannot be parsed:
- Wrong number of columns in a line
- Unterminated quoted field
- Invalid UTF-8 encoding

**Behavior depends on error mode**:
- `ON ERROR FAIL` (default): Abort query with runtime error
- `ON ERROR SKIP`: Skip the malformed line, log a warning, and continue

#### Runtime Errors (Always Abort Query)

These errors always abort the query regardless of error mode:

**Expression evaluation errors**:
- `row[e]` where `e >= fieldCount`: Index out of bounds
- `row[e]` where `e < 0`: Negative index
- `row.columnName` where `columnName` not in headers: Unknown header
- `toInteger("abc")`: Cannot parse value

**File errors**:
- File not found: Error at `CSVSourceProcessor::prepare()` time

### Edge Cases

#### Empty File
An empty CSV file (0 bytes) produces an empty result set with 0 columns. This is not an error.

#### Header-Only File
A file with only a header line (when using `WITH HEADERS`) produces an empty result set with the schema defined by the headers. This is not an error.

#### Duplicate Column Names in Headers
If a CSV file has duplicate column names in the header row, this is a runtime error when the first chunk is read. Users must fix the CSV or use index-based access without `WITH HEADERS`.

#### Maximum Line Length
No explicit limit on line length. Lines are bounded only by available memory for the line buffer (used when a line spans mmap chunk boundaries). Extremely long lines (>1GB) may cause memory issues.

#### Empty Fields
Empty fields (e.g., `a,,b`) are valid and produce empty strings (`""`).

### File Path Handling

- **Absolute paths**: Used as-is
- **Relative paths**: Resolved relative to the current working directory of the TuringDB server process
- **Path validation**: Paths are validated at `CSVSourceProcessor::prepare()` time; file must exist and be readable
- **Symlinks**: Followed (standard filesystem behavior)

### Encoding

- **Required encoding**: UTF-8
- **BOM handling**: UTF-8 BOM (0xEF 0xBB 0xBF) at file start is skipped if present
- **Invalid UTF-8**: Lines with invalid UTF-8 sequences cause a runtime error
- **Other encodings**: Not supported. Users must convert files to UTF-8 before import.

### Concurrency

#### Thread Safety
- Each `CSVParser` instance operates on its own file descriptor and mmap regions
- Multiple concurrent `LOAD CSV` queries can run simultaneously without interference
- No shared mutable state between parser instances

#### Concurrent File Access
- The CSV file should not be modified during import
- If the file is modified, behavior is undefined (may read partial data or fail)
- Consider using file locking at the application level if concurrent writes are possible

### Restrictions (Phase 1)

#### Single LOAD CSV Per Query
Multiple `LOAD CSV` statements in a single query are **not supported** in Phase 1:

```cypher
-- NOT SUPPORTED in Phase 1
LOAD CSV 'a.csv' AS a
LOAD CSV 'b.csv' AS b
MATCH ...
```

**Rationale**: Multiple CSV sources would require Cartesian product or explicit join semantics, significantly complicating the implementation. Users should use separate queries.

#### RETURN Without CREATE/MATCH
Using `LOAD CSV` with only `RETURN` is **supported** and useful for debugging/preview:

```cypher
LOAD CSV 'file.csv' AS row
RETURN row[0], row[1], toInteger(row[2])
LIMIT 10
```

This streams the first 10 rows without creating any graph data.

### Stmt Type Registration

Add `LOAD_CSV` to the `Stmt::Kind` enum:

```cpp
// In Stmt.h or equivalent
enum class Kind {
    // ... existing ...
    LOAD_CSV,        // NEW
    LOAD_EMBEDDING_CSV,  // NEW (Phase 2)
};
```

### Progress and Statistics

#### Statistics Collection
`CSVParser` tracks and exposes:
- `getLinesRead()`: Total lines successfully parsed
- `getLinesSkipped()`: Lines skipped due to errors (only incremented in `ON ERROR SKIP` mode)

#### Logging

At the end of import, log a summary:
```
[INFO] LOAD CSV completed: 1,234,567 lines read
```

When using `ON ERROR SKIP` and lines were skipped:
```
[INFO] LOAD CSV completed: 1,234,567 lines read, 42 lines skipped
[WARN] LOAD CSV: skipped line 12345 (wrong column count: expected 5, got 3)
[WARN] LOAD CSV: skipped line 67890 (invalid UTF-8 sequence)
```

#### Progress Indication (Future)
Progress reporting during import is deferred to future work. For now, users can monitor file read progress via OS-level tools if needed.

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
