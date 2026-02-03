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
   - [CSV Schema Reader](#csv-schema-reader)
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
5. **Error Handling**: Graceful handling of malformed lines with skip-and-log behavior

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
- Compile-time index resolution (literals only) enables efficient code generation
- Clear semantics specific to CSV use case

### Decision 3: Index Must Be Literal Integer

`row[i]` only supports literal integer indices, not expressions.

**Valid**: `row[0]`, `row[42]`
**Invalid**: `row[i]`, `row[1+1]`

**Rationale**:
- Enables compile-time resolution of column access
- Simpler implementation without runtime bounds checking logic
- Matches common usage patterns

### Decision 4: Schema Discovery at Pipeline Generation

The CSV schema (column count, headers) is determined by reading the first line of the file during pipeline generation, not during parsing or analysis.

**Rationale**:
- Separates parsing/analysis from I/O operations
- Schema reading is encapsulated in a utility class
- Analysis can proceed without file access (useful for query validation)

**Trade-off**: Column name typos in `row.columnName` are caught at pipeline generation time, not analysis time.

### Decision 5: Chunked Processing

CSV files are processed in chunks of 100,000 rows to bound memory usage.

**Rationale**:
- 100GB files cannot be fully materialized in memory
- Chunk size balances memory usage vs. overhead
- Aligns with TuringDB's existing batch processing model (WriteProcessor collects creates)

### Decision 6: Skip Malformed Lines

Malformed CSV lines (wrong column count, encoding errors) are skipped and logged, rather than aborting the entire import.

**Rationale**:
- Practical for large imports where some data quality issues are expected
- Users can review logs to identify problematic lines
- Consistent with data engineering best practices

**Note**: Type conversion errors (e.g., `toInteger("abc")`) are runtime errors that abort the query. This is distinct from CSV parsing errors.

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
    ;

// For LOAD EMBEDDING CSV (Phase 2)
loadEmbeddingCSVSt
    : LOAD EMBEDDING CSV STRING_LITERAL AS
      OPAREN symbol COMMA symbol OBRACK DIGIT COLON DIGIT CBRACK CPAREN
    ;

// Index expression for row[i]
atomicExpr
    : propertyOrLabelExpr
    | atomicExpr OBRACK DIGIT CBRACK   // NEW: row[0]
    ;
```

### Lexer Additions

```lex
CSV        { return token::CSV; }
EMBEDDING  { return token::EMBEDDING; }
```

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
│  │ LoadCSVStmt  │    │ Type: String │    │ CSVSchemaReader    │    │
│  │ IndexExpr    │    │ Table        │    │ CSVSourceProcessor │    │
│  └──────────────┘    └──────────────┘    └────────────────────┘    │
│                                                   │                  │
│                                                   ▼                  │
│                              ┌─────────────────────────────────┐    │
│                              │     Runtime Pipeline            │    │
│                              │                                 │    │
│                              │  CSVSourceProcessor             │    │
│                              │    │                            │    │
│                              │    ▼ (N string columns)         │    │
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
├── io/CSVSchemaReader.h
├── io/CSVSchemaReader.cpp
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

private:
    LoadCSVStmt(std::string_view filePath, Symbol* alias);
    ~LoadCSVStmt() = default;

    fs::Path _filePath;
    Symbol* _alias;
    bool _hasHeaders {false};
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

    static IndexExpr* create(CypherAST* ast, Expr* base, int64_t index);

    Expr* getBase() const { return _base; }
    int64_t getIndex() const { return _index; }

    Kind getKind() const override { return Kind::INDEX; }

private:
    IndexExpr(Expr* base, int64_t index);
    ~IndexExpr() = default;

    Expr* _base;
    int64_t _index;
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

    StringTable,    // NEW: CSV row, supports [int] and .columnName
    DenseVector,    // NEW: Dense float vector for embeddings

    _SIZE,
};
```

Update `EvaluatedTypeName` accordingly.

### CSV Schema Reader

```cpp
// io/CSVSchemaReader.h
#pragma once

#include <string>
#include <vector>

#include "io/Path.h"

namespace db {

class CSVSchemaReader {
public:
    struct Schema {
        size_t columnCount {0};
        std::vector<std::string> headers;  // Empty if no headers

        bool hasHeaders() const { return !headers.empty(); }

        // Returns -1 if not found
        int64_t getColumnIndex(std::string_view name) const;
    };

    // Reads first line(s) to determine schema
    // If hasHeaders is true, first line is treated as header row
    static void readSchema(const fs::Path& path, bool hasHeaders, Schema& outSchema);

private:
    static std::vector<std::string> parseLine(std::string_view line);
};

}
```

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
#include "io/CSVSchemaReader.h"

namespace db {

class CSVParser {
public:
    // Default mmap chunk size: 512MB
    static constexpr size_t DEFAULT_MMAP_CHUNK_SIZE = 512 * 1024 * 1024;

    CSVParser(const fs::Path& path,
              const CSVSchemaReader::Schema& schema,
              size_t mmapChunkSize = DEFAULT_MMAP_CHUNK_SIZE);
    ~CSVParser();

    CSVParser(const CSVParser&) = delete;
    CSVParser& operator=(const CSVParser&) = delete;

    // Read up to maxRows into column-oriented output
    // Returns number of rows read (0 at EOF)
    // Malformed lines are skipped and logged
    // Output columns must be pre-allocated ColumnString instances
    size_t readChunk(size_t maxRows,
                     std::vector<ColumnString*>& outputColumns);

    size_t getLinesRead() const { return _linesRead; }
    size_t getLinesSkipped() const { return _linesSkipped; }

private:
    int _fd {-1};                          // File descriptor
    size_t _fileSize {0};                  // Total file size
    size_t _fileOffset {0};                // Current position in file
    size_t _mmapChunkSize;                 // Size of each mmap region

    void* _mappedRegion {nullptr};         // Current mmap region
    size_t _mappedOffset {0};              // File offset of mapped region
    size_t _mappedSize {0};                // Size of mapped region

    std::string_view _remaining;           // Unparsed data in current region
    CSVSchemaReader::Schema _schema;

    size_t _linesRead {0};
    size_t _linesSkipped {0};

    // Memory mapping
    bool mapNextChunk();
    void unmapCurrentChunk();

    // RFC 4180 parsing
    bool parseLine(std::string_view& input, std::vector<std::string>& fields);
    bool parseField(std::string_view& input, std::string& output);

    // Handle line that spans mmap chunk boundary
    bool handlePartialLine(std::string& lineBuffer);
};

}
```

### CSV Source Processor

```cpp
// query/pipeline/processors/CSVSourceProcessor.h
#pragma once

#include "Processor.h"
#include "io/CSVParser.h"
#include "io/CSVSchemaReader.h"
#include "io/Path.h"
#include "dataframe/ColumnTag.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class CSVSourceProcessor : public Processor {
public:
    static constexpr size_t DEFAULT_CHUNK_SIZE = 100'000;

    // Factory method - all processors are created via PipelineV2
    static CSVSourceProcessor* create(PipelineV2* pipeline,
                                       const fs::Path& path,
                                       const CSVSchemaReader::Schema& schema,
                                       const std::vector<ColumnTag>& columnTags,
                                       size_t chunkSize = DEFAULT_CHUNK_SIZE);

    // Processor interface
    std::string describe() const override;
    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    // Output interface - produces N string columns (one per CSV column)
    PipelineBlockOutputInterface& output() { return _output; }

    const CSVSchemaReader::Schema& getSchema() const { return _schema; }

private:
    fs::Path _path;
    CSVSchemaReader::Schema _schema;
    std::vector<ColumnTag> _columnTags;  // Pre-allocated tags for output columns
    size_t _chunkSize;

    CSVParser* _parser {nullptr};        // Owned, created in prepare()
    PipelineBlockOutputInterface _output;

    CSVSourceProcessor(const fs::Path& path,
                       const CSVSchemaReader::Schema& schema,
                       const std::vector<ColumnTag>& columnTags,
                       size_t chunkSize);
    ~CSVSourceProcessor() override;
};

}
```

#### Processor Lifecycle

1. **create()**: Called by `PipelineGenerator` to instantiate the processor and register it with the pipeline
2. **prepare()**: Called once before execution starts; opens the file, creates `CSVParser`, allocates output columns
3. **execute()**: Called repeatedly by the pipeline executor; reads one chunk of rows and writes to output
4. **reset()**: Called to reset state for re-execution (e.g., if pipeline needs to restart)

#### Output Interface

The processor uses `PipelineBlockOutputInterface` which:
- Produces a `Dataframe` with N `NamedColumn` instances (one per CSV column)
- Each column is a `ColumnString` containing the string values for that CSV column
- Columns are identified by the pre-allocated `ColumnTag` values

### Memory Management

#### String Ownership

Strings parsed from CSV are **copied** into `ColumnString` storage, not referenced via `string_view`. This is necessary because:

1. The mmap region is unmapped after each chunk is processed
2. Strings may span mmap chunk boundaries and require assembly
3. Quoted fields require unescaping (removing doubled quotes)

The copy overhead is acceptable because:
- String data is typically small relative to the file size
- Memory is bounded by chunk size (100K rows)
- Columnar string storage uses efficient arena allocation

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

    EvaluatedType baseType = expr->getBase()->getType();

    if (baseType != EvaluatedType::StringTable) {
        throwError("Index operator [] only supported on CSV row variables", expr);
    }

    // Index is already resolved to literal during parsing
    int64_t index = expr->getIndex();
    if (index < 0) {
        throwError("CSV column index must be non-negative", expr);
    }

    // Result type is String
    expr->setType(EvaluatedType::String);
}

// For row.columnName (in analyzePropertyExpr)
ValueType ExprAnalyzer::analyzePropertyExpr(PropertyExpr* expr, ...) {
    // ... existing code ...

    // Check if base is StringTable (CSV row with headers)
    if (baseType == EvaluatedType::StringTable) {
        // This is a header-based column access: row.columnName
        // Store the column name; actual index resolution happens at pipeline generation
        expr->setType(EvaluatedType::String);
        expr->setCSVHeaderAccess(true);  // New flag
        return ValueType::String;
    }

    // ... existing property access code ...
}
```

### Pipeline Generator

```cpp
// In PipelineGenerator.cpp

void PipelineGenerator::generate(const LoadCSVStmt* stmt) {
    // 1. Read schema from CSV file
    CSVSchemaReader::Schema schema;
    CSVSchemaReader::readSchema(stmt->getFilePath(), stmt->hasHeaders(), schema);

    // 2. Validate any header-based accesses (row.columnName)
    validateCSVHeaderAccesses(stmt, schema);

    // 3. Create source processor
    auto csvSource = std::make_unique<CSVSourceProcessor>(
        stmt->getFilePath(),
        std::move(schema),
        CSVSourceProcessor::DEFAULT_CHUNK_SIZE
    );

    // 4. Register column mappings for expression evaluation
    // Maps row[i] -> column i in the processor output
    // Maps row.colName -> column index from schema
    registerCSVColumnMappings(stmt->getAlias(), csvSource->getSchema());

    // 5. Add to pipeline
    _pipeline->setSource(std::move(csvSource));
}

void PipelineGenerator::validateCSVHeaderAccesses(
    const LoadCSVStmt* stmt,
    const CSVSchemaReader::Schema& schema
) {
    // Walk the AST and find all PropertyExpr nodes that reference
    // the CSV alias with CSV header access flag set
    // Validate that the column name exists in schema.headers
    // Resolve column name to index
}
```

### Expression Lowering (ExprProgramGenerator)

This section describes how `row[i]` and `row.columnName` are lowered to executable form by `ExprProgramGenerator`.

#### Column Tag Allocation

During pipeline generation, CSVSourceProcessor allocates a `ColumnTag` for each CSV column via `DataframeManager`:

```cpp
void PipelineGenerator::generate(const LoadCSVStmt* stmt) {
    // ... schema reading ...

    // Allocate column tags for each CSV column
    std::vector<ColumnTag> csvColumnTags;
    for (size_t i = 0; i < schema.columnCount; ++i) {
        ColumnTag tag = _dataframeManager->allocTag();
        csvColumnTags.push_back(tag);
    }

    // Register mappings: alias + index -> ColumnTag
    // This is used by ExprProgramGenerator to resolve row[i]
    registerIndexedTableColumnTags(stmt->getAlias()->getName(), csvColumnTags);

    // If WITH HEADERS, also register: alias + columnName -> ColumnTag
    if (stmt->hasHeaders()) {
        for (size_t i = 0; i < schema.headers.size(); ++i) {
            registerIndexedTableHeaderTag(stmt->getAlias()->getName(),
                                 schema.headers[i],
                                 csvColumnTags[i]);
        }
    }
}
```

#### ExprProgramGenerator Handling

`ExprProgramGenerator` is extended to handle `IndexExpr`:

```cpp
// In ExprProgramGenerator.cpp

void ExprProgramGenerator::generate(const IndexExpr* expr) {
    // Get the alias from the base SymbolExpr
    const SymbolExpr* baseExpr = static_cast<const SymbolExpr*>(expr->getBase());
    std::string_view alias = baseExpr->getSymbol()->getName();

    // Get the column index
    int64_t colIndex = expr->getIndex();

    // Look up the ColumnTag for this CSV column
    ColumnTag tag = _pipelineGen->getIndexedTableColumnTag(alias, colIndex);

    // Emit instruction to read from this column
    // The result is a string value
    emitColumnRead(tag, EvaluatedType::String);
}

void ExprProgramGenerator::generate(const PropertyExpr* expr) {
    if (expr->isCSVHeaderAccess()) {
        // Get alias and column name
        std::string_view alias = /* from base */;
        std::string_view columnName = expr->getPropertyName();

        // Look up the ColumnTag (resolved during pipeline generation)
        ColumnTag tag = _pipelineGen->getIndexedTableHeaderTag(alias, columnName);

        emitColumnRead(tag, EvaluatedType::String);
        return;
    }

    // ... existing property access logic ...
}
```

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

#### CSV Parsing Errors (Skip and Log)

- Wrong number of columns in a line
- Unterminated quoted field
- Invalid UTF-8 encoding

These errors skip the line and log a warning. The import continues.

#### Runtime Errors (Abort Query)

- `row[i]` where `i >= columnCount`: Runtime error
- `toInteger("abc")`: Runtime error (cannot parse)
- File not found: Error at pipeline generation time

### Edge Cases

#### Empty File
An empty CSV file (0 bytes) produces an empty result set with 0 columns. This is not an error.

#### Header-Only File
A file with only a header line (when using `WITH HEADERS`) produces an empty result set with the schema defined by the headers. This is not an error.

#### Duplicate Column Names in Headers
If a CSV file has duplicate column names in the header row, this is an error at pipeline generation time. Users must fix the CSV or use index-based access without `WITH HEADERS`.

#### Maximum Line Length
No explicit limit on line length. Lines are bounded only by available memory for the line buffer (used when a line spans mmap chunk boundaries). Extremely long lines (>1GB) may cause memory issues.

#### Empty Fields
Empty fields (e.g., `a,,b`) are valid and produce empty strings (`""`).

### File Path Handling

- **Absolute paths**: Used as-is
- **Relative paths**: Resolved relative to the current working directory of the TuringDB server process
- **Path validation**: Paths are validated at pipeline generation time; file must exist and be readable
- **Symlinks**: Followed (standard filesystem behavior)

### Encoding

- **Required encoding**: UTF-8
- **BOM handling**: UTF-8 BOM (0xEF 0xBB 0xBF) at file start is skipped if present
- **Invalid UTF-8**: Lines with invalid UTF-8 sequences are skipped and logged (treated as malformed)
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
- `getLinesSkipped()`: Lines skipped due to errors

#### Logging
At the end of import, log a summary:
```
[INFO] LOAD CSV completed: 1,234,567 lines read, 42 lines skipped
```

If lines were skipped, log at WARNING level with details:
```
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
      OPAREN symbol COMMA symbol OBRACK DIGIT COLON DIGIT CBRACK CPAREN {
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

    size_t getDimension() const { return _endCol - _startCol; }

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
2. Parses columns [startCol, endCol) directly as floats into dense vector
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
MATCH (a:Person {id: row[0]}), (b:Person {id: row[1]})
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
CREATE (n:Entity {id: nodeRow[0], name: nodeRow[1]})

// Then in a separate query:
LOAD CSV 'edges.csv' AS edgeRow
MATCH (a:Entity {id: edgeRow[0]}), (b:Entity {id: edgeRow[1]})
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

6. **Error Modes**: Configurable behavior for malformed lines
   ```cypher
   LOAD CSV 'file.csv' AS row ON ERROR SKIP  // or ON ERROR FAIL
   ```

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
