#pragma once

#include <stddef.h>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "ID.h"
#include "NLStepBuffers.h"
#include "ProcedureContext.h"
#include "ProcedureState.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnStringTable.h"
#include "columns/ColumnVector.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"

#include "NLFunctionData.h"
#include "NLLimitState.h"
#include "NLColumnFunctions.h"

namespace db {

class NLExecutionContext;
class Procedure;
class ProcedureData;
class StringBuffer;

// The literal-list sibling of NLConstScanLoopData: streams a fixed ListView one chunk
// at a time. A homogeneous list fills a nullable value column of _valueType, the shape
// every value-chunk consumer reads; a heterogeneous list fills a
// ColumnVector<CellView> of tagged scalars, in which case _valueType is unused.
// The ListView spans the query-scoped ListBuffer, so the elements outlive the loop. A
// plain source, so a downstream LIMIT can bound it.
class NLUnwindConstLoopData : public NLFunctionData {
public:
    NLUnwindConstLoopData(Column* output, ListView list, bool heterogeneous, ValueType valueType)
        : _output(output),
        _list(list),
        _heterogeneous(heterogeneous),
        _valueType(valueType)
    {
    }

    Column* getOutput() const { return _output; }
    ListView getList() const { return _list; }
    bool isHeterogeneous() const { return _heterogeneous; }
    ValueType getValueType() const { return _valueType; }

    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    Column* _output {nullptr};
    ListView _list;
    bool _heterogeneous {false};
    ValueType _valueType {ValueType::Invalid};
    NLLimitState* _limit {nullptr};
    NLStmtContainer _stmts;
};

// The file sibling of NLUnwindConstLoopData: parses a CSV file one chunk of records at a
// time into one owning string column per field the load produces. The path, unless bound
// when the query runs, and the field selectors are views into the module's attribute
// storage, which the MLIRContext keeps alive for the whole execution; the path is resolved
// against the data directory, and a header name against the file's header line, when the
// loop runs. The output columns are the field columns of _row, which is what the parser
// fills. A plain source, so a downstream LIMIT can bound it.
class NLLoadCSVLoopData : public NLFunctionData {
public:
    // One field the load produces: the position `row[2]` named, or the header `row.age`
    // named, which _index is resolved to against the header line.
    struct Field {
        std::string_view _header;
        size_t _index {0};
        bool _byHeader {false};
    };

    NLLoadCSVLoopData(ColumnStringTable* row,
                      StringBuffer* stringBuffer,
                      std::string_view path,
                      bool hasHeaders,
                      bool skipOnError)
        : _row(row),
        _stringBuffer(stringBuffer),
        _path(path),
        _hasHeaders(hasHeaders),
        _skipOnError(skipOnError)
    {
    }

    ColumnStringTable* getRow() const { return _row; }
    StringBuffer* getStringBuffer() const { return _stringBuffer; }
    std::string_view getPath() const { return _boundPath ? _boundPath->getRaw() : _path; }
    void setBoundPath(const ColumnConst<types::String::Primitive>* boundPath) { _boundPath = boundPath; }
    bool hasHeaders() const { return _hasHeaders; }
    bool skipOnError() const { return _skipOnError; }

    const std::vector<Field>& fields() const { return _fields; }
    void addField(const Field& field) { _fields.push_back(field); }

    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    ColumnStringTable* _row {nullptr};
    StringBuffer* _stringBuffer {nullptr};
    std::string_view _path;
    const ColumnConst<types::String::Primitive>* _boundPath {nullptr};
    std::vector<Field> _fields;
    NLLimitState* _limit {nullptr};
    bool _hasHeaders {false};
    bool _skipOnError {false};
    NLStmtContainer _stmts;
};

// The neighbour sibling of NLUnwindConstLoopData: runs a search against a vector index and
// streams the neighbours it found one chunk at a time, into the node ID column of the nodes
// the index holds the vectors under and a nullable f64 column of the distances they scored.
// The index name, the neighbour count and the query vector are the ones the query spells -
// views into the module's attribute storage, which the MLIRContext keeps alive for the whole
// execution - or the values the bound columns hold when the loop starts. A plain source, so
// a downstream LIMIT can bound it.
class NLVectorSearchLoopData : public NLFunctionData {
public:
    NLVectorSearchLoopData(ColumnNodeIDs* ids,
                           Column* scores,
                           std::string_view indexName,
                           size_t neighbourCount,
                           std::span<const float> queryVector)
        : _ids(ids),
        _scores(scores),
        _indexName(indexName),
        _neighbourCount(neighbourCount),
        _queryVector(queryVector)
    {
    }

    ColumnNodeIDs* getIDs() const { return _ids; }
    Column* getScores() const { return _scores; }
    std::string_view getIndexName() const { return _indexName; }
    size_t getNeighbourCount() const { return _neighbourCount; }
    std::span<const float> getQueryVector() const { return _queryVector; }

    const Column* getBoundIndexName() const { return _boundIndexName; }
    void setBoundIndexName(const Column* indexName) { _boundIndexName = indexName; }

    const Column* getBoundNeighbourCount() const { return _boundNeighbourCount; }
    void setBoundNeighbourCount(const Column* count) { _boundNeighbourCount = count; }

    const Column* getBoundQueryVector() const { return _boundQueryVector; }
    void setBoundQueryVector(const Column* queryVector) { _boundQueryVector = queryVector; }

    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

    // The neighbours the last search reported, nearest first, in the types the two chunks
    // carry. The search is a function of what it searches with, so a loop re-entered over
    // the same index name, count and vector - from an enclosing cross product, or a row
    // holding the same ones - slices these again rather than asking the index a second time.
    bool hasSearched(std::string_view indexName, size_t neighbourCount, std::span<const float> queryVector) const;
    void markSearched(std::string_view indexName, size_t neighbourCount, std::span<const float> queryVector);

    std::vector<NodeID>& neighbourIDs() { return _neighbourIDs; }
    std::vector<types::Double::Primitive>& neighbourScores() { return _neighbourScores; }

private:
    ColumnNodeIDs* _ids {nullptr};
    Column* _scores {nullptr};
    std::string_view _indexName;
    size_t _neighbourCount {0};
    std::span<const float> _queryVector;
    const Column* _boundIndexName {nullptr};
    const Column* _boundNeighbourCount {nullptr};
    const Column* _boundQueryVector {nullptr};
    NLLimitState* _limit {nullptr};
    std::vector<NodeID> _neighbourIDs;
    std::vector<types::Double::Primitive> _neighbourScores;
    std::string _searchedIndexName;
    size_t _searchedNeighbourCount {0};
    std::vector<float> _searchedVector;
    bool _searched {false};
    NLStmtContainer _stmts;
};

// nl.for over nl.unwind data: the per-element expansion of a value column. Holds the
// source column with the handler counting the rows each of its cells contributes, the
// drain filling the element chunk, and the carry set gathered by each emitted row's
// source row - the same NLCarriedColumn shape the edge loops use. The scratch columns
// hold, per emitted row of the current step, the source row it came from and the
// position of the element inside that row's cell.
class NLUnwindLoopData : public NLFunctionData {
public:
    using CarriedColumns = std::vector<NLCarriedColumn>;

    // @param elementEmit and @param elementOutput are null when the source's cells are
    // themselves the elements: they are then gathered through the carry set like any
    // other column.
    NLUnwindLoopData(const Column* source,
                     NLUnwindElementCountFunction elementCount,
                     NLUnwindElementEmitFunction elementEmit,
                     Column* elementOutput)
        : _source(source),
        _elementCount(elementCount),
        _elementEmit(elementEmit),
        _elementOutput(elementOutput)
    {
    }

    const Column* getSource() const { return _source; }
    NLUnwindElementCountFunction getElementCountFunc() const { return _elementCount; }
    NLUnwindElementEmitFunction getElementEmitFunc() const { return _elementEmit; }
    Column* getElementOutput() const { return _elementOutput; }

    const CarriedColumns& carriedColumns() const { return _carriedColumns; }

    void addCarriedColumn(const NLCarriedColumn& carried) {
        _carriedColumns.push_back(carried);
    }

    ColumnVector<size_t>* getRows() { return &_rows; }
    ColumnVector<size_t>* getPositions() { return &_positions; }

    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    const Column* _source {nullptr};
    NLUnwindElementCountFunction _elementCount {nullptr};
    NLUnwindElementEmitFunction _elementEmit {nullptr};
    Column* _elementOutput {nullptr};

    NLLimitState* _limit {nullptr};
    CarriedColumns _carriedColumns;
    NLStmtContainer _stmts;

    ColumnVector<size_t> _rows;
    ColumnVector<size_t> _positions;
};

// Runtime state of one CALL: the live procedure call the nl.procedure statements
// drive. It owns the data the procedure's alloc callback produced (released by the
// dealloc callback here) and the ProcedureState the callbacks read - the procedure,
// the execution context, the step and the finished flag - plus the result columns
// the yielded return values are bound to, in yield order.
//
// Unlike the other accumulators the state being folded into lives inside the
// procedure, not here: this class only drives the callbacks and knows which columns
// they write into. The result columns are borrowed - the translator pool-allocates
// them in the same arena as the loop columns - and are shared by every step, so the
// procedure refills them in place, chunk after chunk.
class NLProcedureState {
public:
    NLProcedureState(const Procedure* procedure,
                     ProcedureData* data,
                     const ProcedureContext* context);
    ~NLProcedureState();

    NLProcedureState(const NLProcedureState&) = delete;
    NLProcedureState& operator=(const NLProcedureState&) = delete;

    const Procedure* getProcedure() const { return _procedure; }
    ProcedureData* getData() const { return _data; }

    // The procedure's own index for each yielded return value, in yield order: a
    // procedure writes a return value through the slot its declaration order names,
    // while the query reads the yields in the order it asked for them. Resolved once,
    // when the call is prepared, so the ops binding columns need no name lookup.
    const std::vector<size_t>& yieldIndices() const { return _yieldIndices; }

    void addYieldIndex(size_t returnIndex) { _yieldIndices.push_back(returnIndex); }

    // The columns the yielded return values are bound to, parallel to yieldIndices.
    // Also the loop variables of a drive loop, so a step fills them in place.
    const std::vector<Column*>& resultColumns() const { return _resultColumns; }

    void addResultColumn(Column* column) { _resultColumns.push_back(column); }

    void setExecutionContext(const NLExecutionContext* context);

    // Prepare the procedure on its first drive and rewind it - the RESET step - on each
    // later one, so a procedure that finished the previous chunk of arguments starts
    // afresh on the next one, dropping whatever per-drive state it kept.
    //
    // Preparing here rather than where the handle is bound is what lets a procedure read
    // its arguments in its prepare step: the producing loop has filled them by the time
    // its drive loop is entered.
    void prepareOrResetForNewDrive();

    // Run the procedure's execute callback once, over whatever its input columns
    // currently hold. Clears the finished flag first, so a procedure that marks
    // itself finished once it has produced a chunk's rows is still driven again.
    void execute();

    // True once the procedure has declared it has no more rows to produce, which is
    // what ends a drive loop.
    bool isFinished() const { return _procedureState.isFinished(); }

    // The rows the last callback filled. Every result column is row-aligned - one
    // row of the call per row of each column - so the first one sizes the step.
    size_t getRowCount() const;

    // The rows of the chunk the procedure was handed this step, read from its first
    // row-aligned argument column. This is the range the input-row indices a carry set
    // is replicated through must fall in; zero for a procedure handed no row-aligned
    // argument, which can carry nothing.
    size_t getInputRowCount() const;

private:
    const Procedure* _procedure {nullptr};
    ProcedureData* _data {nullptr};
    ProcedureState _procedureState;
    std::vector<size_t> _yieldIndices;

    // The query's procedure context, with the lists and strings a step builds kept in
    // buffers of this call's own, emptied at the next step
    ProcedureContext _context;
    NLStepBuffers _stepBuffers;
    std::vector<Column*> _resultColumns;
    bool _prepared {false};

    // Whether the procedure has run since it was prepared or last rewound, which is
    // what makes a rewind necessary before the next drive
    bool _driven {false};

    // The PREPARE step, which lets the procedure build whatever it produces its rows from
    void prepare();

    // The RESET step, which rewinds the procedure to its first row
    void reset();
};

// nl.for over nl.procedure_init data: the drive loop of a row-producing procedure. The
// loop is reset at entry and then runs the procedure once per step - filling the result
// columns, which are the loop variables - until it declares itself finished, so one
// chunk of arguments may be answered with several chunks of rows. The procedure sibling
// of NLEdgeLoopData: a loop whose chunks come from a callback rather than a graph
// iterator, but which replicates its carry set exactly the same way.
//
// A procedure may emit several rows per input row or none, which would leave a carried
// column misaligned with what it emitted, so each step rebuilds every carried column
// through the input-row index the procedure reports per emitted row - the same
// (input, output, gather) shape, and the same NLGatherFunction, an edge hop uses. The
// index column is owned here: the translator hands it to the procedure once, and the
// loop clears it before each step.
class NLProcedureLoopData : public NLFunctionData {
public:
    using CarriedColumns = std::vector<NLCarriedColumn>;

    NLProcedureLoopData(NLProcedureState* state)
        : _state(state)
    {
    }

    NLProcedureState* getState() const { return _state; }

    // The governing limit counter, or null for an unbounded loop. The loop driver
    // stops once it reaches zero, so a LIMIT ends the drive early instead of running
    // the procedure out.
    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    const CarriedColumns& carriedColumns() const { return _carriedColumns; }

    void addCarriedColumn(const NLCarriedColumn& carried) {
        _carriedColumns.push_back(carried);
    }

    ColumnIndices* getIndices() { return &_indices; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    NLProcedureState* _state {nullptr};
    NLLimitState* _limit {nullptr};
    CarriedColumns _carriedColumns;

    // One index per row the procedure emitted this step: the input row it came from
    ColumnIndices _indices;

    NLStmtContainer _stmts;
};

// The run of each row's range an nl.unwind_range keeps: the slice bounds, absent where the
// query gave none, and whether the run is counted out backwards.
struct NLRangeWindowBounds {
    NLRangeBound _from;
    NLRangeBound _to;
    bool _reversed {false};
};

// nl.unwind_range loop data: the bounds each row is counted out by and the run of it kept,
// the element chunk a step fills with the next integers, and the carry set gathered by the
// row each integer came from.
class NLUnwindRangeLoopData : public NLFunctionData {
public:
    using CarriedColumns = std::vector<NLCarriedColumn>;

    NLUnwindRangeLoopData(const NLRangeBound& start,
                          const NLRangeBound& end,
                          const NLRangeBound& step,
                          const NLRangeWindowBounds& window,
                          Column* elementOutput)
        : _start(start),
        _end(end),
        _step(step),
        _window(window),
        _elementOutput(elementOutput)
    {
    }

    const NLRangeBound& getStart() const { return _start; }
    const NLRangeBound& getEnd() const { return _end; }
    const NLRangeBound& getStep() const { return _step; }
    const NLRangeWindowBounds& getWindow() const { return _window; }
    Column* getElementOutput() const { return _elementOutput; }

    const CarriedColumns& carriedColumns() const { return _carriedColumns; }

    void addCarriedColumn(const NLCarriedColumn& carried) {
        _carriedColumns.push_back(carried);
    }

    ColumnVector<size_t>* getRows() { return &_rows; }

    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    NLRangeBound _start;
    NLRangeBound _end;
    NLRangeBound _step;
    NLRangeWindowBounds _window;
    Column* _elementOutput {nullptr};

    NLLimitState* _limit {nullptr};
    CarriedColumns _carriedColumns;
    NLStmtContainer _stmts;

    ColumnVector<size_t> _rows;
};

// nl.unwind_computed loop data: what an nl.unwind's loop data holds, plus the region
// computing the source over a run of input rows - its statements, the carried inputs it
// reads cut to the run, and the rows of the run. The source, and how its cells unwind, are
// only known once the region is translated. The carried columns are gathered from the
// whole input chunk, whose rows @param input holds.
class NLUnwindComputedLoopData : public NLFunctionData {
public:
    using CarriedColumns = std::vector<NLCarriedColumn>;

    explicit NLUnwindComputedLoopData(const Column* input)
        : _input(input)
    {
    }

    // @param elementEmit and @param elementOutput are null when the source's cells are
    // themselves the elements, as on NLUnwindLoopData; they are then gathered through
    // @param elementCell, which is null where nothing reads them.
    void setSource(const Column* source,
                   NLUnwindElementCountFunction elementCount,
                   NLUnwindElementEmitFunction elementEmit,
                   Column* elementOutput,
                   const NLCarriedColumn* elementCell) {
        _source = source;
        _elementCount = elementCount;
        _elementEmit = elementEmit;
        _elementOutput = elementOutput;

        if (elementCell) {
            _elementCell.emplace(*elementCell);
        }
    }

    size_t getInputRows() const { return _input->size(); }
    const Column* getSource() const { return _source; }
    NLUnwindElementCountFunction getElementCountFunc() const { return _elementCount; }
    NLUnwindElementEmitFunction getElementEmitFunc() const { return _elementEmit; }
    Column* getElementOutput() const { return _elementOutput; }
    const NLCarriedColumn* getElementCell() const { return _elementCell ? &*_elementCell : nullptr; }

    const CarriedColumns& sourceArguments() const { return _sourceArguments; }

    void addSourceArgument(const NLCarriedColumn& argument) {
        _sourceArguments.push_back(argument);
    }

    const CarriedColumns& carriedColumns() const { return _carriedColumns; }

    void addCarriedColumn(const NLCarriedColumn& carried) {
        _carriedColumns.push_back(carried);
    }

    ColumnVector<size_t>* getRunRows() { return &_runRows; }
    ColumnVector<size_t>* getRows() { return &_rows; }
    ColumnVector<size_t>* getPositions() { return &_positions; }

    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getSourceStmts() { return &_sourceStmts; }
    const NLStmtContainer* getSourceStmts() const { return &_sourceStmts; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    const Column* _input {nullptr};
    const Column* _source {nullptr};
    NLUnwindElementCountFunction _elementCount {nullptr};
    NLUnwindElementEmitFunction _elementEmit {nullptr};
    Column* _elementOutput {nullptr};
    std::optional<NLCarriedColumn> _elementCell;

    NLLimitState* _limit {nullptr};
    CarriedColumns _sourceArguments;
    CarriedColumns _carriedColumns;
    NLStmtContainer _sourceStmts;
    NLStmtContainer _stmts;

    ColumnVector<size_t> _runRows;
    ColumnVector<size_t> _rows;
    ColumnVector<size_t> _positions;
};

}
