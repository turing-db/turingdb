#pragma once

#include <stddef.h>
#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ID.h"
#include "LocalMemory.h"
#include "ProcedureState.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnStringTable.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"

namespace db {

class NLExecutionContext;
class NLFunctionData;
struct NLMergeWorkingSet;
class Procedure;
class ProcedureContext;
class ProcedureData;
class LocalMemory;

// Translation resolves every chunk SSA value
// to a concrete ColumnVector type through this kind
//
// The first three are the ID columns a scan or a hop binds. The rest are what a CALL
// yields: a procedure's declared return types map onto these, so a yielded column can be
// crossed and carried like any other. Every one of them is a plain (never-null)
// ColumnVector of the element type its name says - String is a borrowed string_view,
// OwnedString a std::string the column owns.
enum class NLChunkKind {
    NodeID,
    EdgeID,
    EdgeTypeID,
    LabelID,
    PropertyTypeID,
    ValueTypeCode,
    UInt64,
    Int64,
    Double,
    Bool,
    String,
    OwnedString,
    List,
};

// Invoke handler with the column element type a chunk kind stands for, so the families of
// per-element-type handlers are selected in one place rather than through a switch
// repeated in each selector. handler is a template lambda - [&]<typename ElementType>(){}
// - which is therefore instantiated for every kind, so only a family defined for all of
// them dispatches this way: copying values (gather, broadcast, range copy) and allocating
// the column. Ordering, key serialization and reduction are not defined for every kind, so
// their selectors spell out the kinds they support and reject the rest.
template <typename Handler>
void dispatchChunkKind(NLChunkKind kind, Handler&& handler) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return handler.template operator()<NodeID>();
        break;

        case NLChunkKind::EdgeID:
            return handler.template operator()<EdgeID>();
        break;

        case NLChunkKind::EdgeTypeID:
            return handler.template operator()<EdgeTypeID>();
        break;

        case NLChunkKind::LabelID:
            return handler.template operator()<LabelID>();
        break;

        case NLChunkKind::PropertyTypeID:
            return handler.template operator()<PropertyTypeID>();
        break;

        case NLChunkKind::ValueTypeCode:
            return handler.template operator()<ValueType>();
        break;

        case NLChunkKind::UInt64:
            return handler.template operator()<types::UInt64::Primitive>();
        break;

        case NLChunkKind::Int64:
            return handler.template operator()<types::Int64::Primitive>();
        break;

        case NLChunkKind::Double:
            return handler.template operator()<types::Double::Primitive>();
        break;

        case NLChunkKind::Bool:
            return handler.template operator()<types::Bool::Primitive>();
        break;

        case NLChunkKind::String:
            return handler.template operator()<types::String::Primitive>();
        break;

        case NLChunkKind::OwnedString:
            return handler.template operator()<std::string>();
        break;

        case NLChunkKind::List:
            return handler.template operator()<ListView>();
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
}

// Type of function pointers implementing operations
using NLHandlerFunction = void (*)(NLExecutionContext* context, NLFunctionData* data);

// Function descriptor representing translated statements
// It consists of a function pointer and function-specific data
class NLFunctionDescriptor {
public:
    NLFunctionDescriptor(NLHandlerFunction function,
                         NLFunctionData* data)
        : _function(function),
        _data(data)
    {
    }

    NLHandlerFunction getFunction() const { return _function; }
    NLFunctionData* getData() const { return _data; }

private:
    NLHandlerFunction _function {nullptr};
    NLFunctionData* _data {nullptr};
};

// Base class of function-specific data
class NLFunctionData {
public:
    virtual ~NLFunctionData() = default;
};

// Runtime state of one LIMIT: the remaining row budget and how many rows the
// current innermost step should emit. nl.limit resets it, nl.limit_update is the
// sole mutator (called once per innermost step before nl.output), and the loops
// and nl.output only read it. emitThisStep is what output emits; remaining is
// what the loops test to early-exit.
class NLLimitState {
public:
    void reset(size_t budget) {
        _remaining = budget;
        _emitThisStep = 0;
    }

    size_t getRemaining() const { return _remaining; }
    size_t getEmitThisStep() const { return _emitThisStep; }

    // Called once per innermost step by nl.limit_update, before nl.output: emit
    // min(rowsAvailable, remaining) this step and charge it against the budget.
    void update(size_t rowsAvailable) {
        _emitThisStep = std::min(rowsAvailable, _remaining);
        _remaining -= _emitThisStep;
    }

private:
    size_t _remaining {0};
    size_t _emitThisStep {0};
};

// Runtime state of one SKIP: the remaining rows still to drop, and - for the
// current innermost step - how many of its rows to drop off the front
// (skipThisStep, the offset into the chunk) and how many survive (emitThisStep).
// nl.skip resets it, nl.skip_update is the sole mutator (called once per innermost
// step before the truncate), and nl.skip_truncate only reads it. The skip sibling
// of NLLimitState - but it never gates a loop, since every row past the dropped
// prefix must still be produced.
class NLSkipState {
public:
    void reset(size_t toSkip) {
        _remaining = toSkip;
        _skipThisStep = 0;
        _emitThisStep = 0;
    }

    size_t getSkipThisStep() const { return _skipThisStep; }
    size_t getEmitThisStep() const { return _emitThisStep; }

    // Called once per innermost step by nl.skip_update, before nl.skip_truncate:
    // drop min(rowsAvailable, remaining) rows off the front of this step, emit the
    // rest, and charge the dropped count against the budget.
    void update(size_t rowsAvailable) {
        _skipThisStep = std::min(rowsAvailable, _remaining);
        _remaining -= _skipThisStep;
        _emitThisStep = rowsAvailable - _skipThisStep;
    }

private:
    size_t _remaining {0};
    size_t _skipThisStep {0};
    size_t _emitThisStep {0};
};

// Holds the translated statements of a program or loop body
class NLStmtContainer {
public:
    using Stmts = std::vector<NLFunctionDescriptor>;

    const Stmts& stmts() const { return _stmts; }

    void addStmt(const NLFunctionDescriptor& stmt) {
        _stmts.push_back(stmt);
    }

    template <typename... Args>
    void emplaceStmt(Args&&... args) {
        _stmts.emplace_back(std::forward<Args>(args)...);
    }

private:
    Stmts _stmts;
};

// Type of handles per column type that writes the input rows selected
// by the indices column into output
using NLGatherFunction = void (*)(const Column* input,
                                  const ColumnVector<size_t>* indices,
                                  Column* output);

// Type of handle that appends the indices of the rows an nl.filter keeps into the
// indices column. One per mask nullability: a plain mask keeps every true row, a
// nullable mask keeps only present-and-true rows (a null drops).
using NLMaskSurvivorFunction = void (*)(const Column* mask,
                                        ColumnVector<size_t>* indices);

// Wraps a "carried" column from previous operations
class NLCarriedColumn {
public:
    NLCarriedColumn(const Column* input,
                    Column* output,
                    NLGatherFunction gather)
        : _input(input),
        _output(output),
        _gather(gather)
    {
    }

    const Column* getInput() const { return _input; }
    Column* getOutput() const { return _output; }
    NLGatherFunction getGatherFunc() const { return _gather; }

private:
    // Carried column pre-filtering
    const Column* _input {nullptr};

    // Output column after filtering
    Column* _output {nullptr};

    // Gather function to be used to process indices
    NLGatherFunction _gather {nullptr};
};

// nl.scan_nodes loop data
class NLScanLoopData : public NLFunctionData {
public:
    NLScanLoopData(ColumnNodeIDs* nodeIDs)
        : _nodeIDs(nodeIDs)
    {
    }

    ColumnNodeIDs* getNodeIDs() const { return _nodeIDs; }

    // The governing limit counter, or null for an unbounded loop. The loop
    // driver stops once it reaches zero.
    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    ColumnNodeIDs* _nodeIDs {nullptr};
    NLLimitState* _limit {nullptr};
    NLStmtContainer _stmts;
};

// nl.scan_nodes_by_label loop data: a node scan restricted to the nodes whose
// label set is a superset of _labelset. Reuses the plain scan's node chunk,
// limit and body; adds the resolved label set the ScanNodesByLabelChunkWriter
// filters by.
class NLScanByLabelLoopData : public NLScanLoopData {
public:
    // The label set is owned here so the LabelSetHandle the executor builds each
    // run points at storage that outlives it (this data lives for the whole
    // program). _matchable is false when a requested label was absent from the
    // schema, leaving the conjunction unsatisfiable, so the loop emits no row.
    NLScanByLabelLoopData(ColumnNodeIDs* nodeIDs, const LabelSet& labelset, bool matchable)
        : NLScanLoopData(nodeIDs),
        _labelset(labelset),
        _matchable(matchable)
    {
    }

    const LabelSet& getLabelSet() const { return _labelset; }
    bool isMatchable() const { return _matchable; }

private:
    LabelSet _labelset;
    bool _matchable {true};
};

// nl.const_scan_nodes loop data: a node scan over a fixed, explicitly listed set
// of node IDs rather than a walk of the graph. Reuses the plain scan's node chunk,
// limit and body; adds the owned list of node IDs the loop emits, one chunk-sized
// slice per step. The translator fills the list in place through addNodeID after
// allocating the data, so the resolved IDs are never staged in a temporary; the
// list is owned here so it outlives the op's attribute storage (this data lives
// for the whole program).
class NLConstScanLoopData : public NLScanLoopData {
public:
    NLConstScanLoopData(ColumnNodeIDs* nodeIDs)
        : NLScanLoopData(nodeIDs)
    {
    }

    std::span<const NodeID> getConstNodeIDs() const { return _constNodeIDs; }

    // Reserve room for the resolved list, then append each valid ID in turn: the
    // translator fills the list here directly rather than building and copying one.
    void reserveNodeIDs(size_t count) { _constNodeIDs.reserve(count); }
    void addNodeID(NodeID nodeID) { _constNodeIDs.push_back(nodeID); }

private:
    std::vector<NodeID> _constNodeIDs;
};

// The literal-list sibling of NLConstScanLoopData: streams a fixed ListView one chunk
// at a time. A homogeneous list fills a nullable value column of _valueType, the shape
// every value-chunk consumer reads; a heterogeneous list fills a
// ColumnVector<ListElementView> of tagged scalars, in which case _valueType is unused.
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
// time into one owning string column per field the load produces. The path and the field
// selectors are views into the module's attribute storage, which the MLIRContext keeps
// alive for the whole execution; the path is resolved against the data directory, and a
// header name against the file's header line, when the loop runs. The output columns are
// the field columns of _row, which is what the parser fills. A plain source, so a
// downstream LIMIT can bound it.
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
                      std::string_view path,
                      bool hasHeaders,
                      bool skipOnError)
        : _row(row),
        _path(path),
        _hasHeaders(hasHeaders),
        _skipOnError(skipOnError)
    {
    }

    ColumnStringTable* getRow() const { return _row; }
    std::string_view getPath() const { return _path; }
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
    std::string_view _path;
    std::vector<Field> _fields;
    NLLimitState* _limit {nullptr};
    bool _hasHeaders {false};
    bool _skipOnError {false};
    NLStmtContainer _stmts;
};

// The neighbour sibling of NLUnwindConstLoopData: runs one search against a vector index
// and streams the neighbours it found one chunk at a time, into the node ID column of the
// nodes the index holds the vectors under and a nullable f64 column of the distances they
// scored. The index name and the query vector are views into the module's attribute
// storage, which the MLIRContext keeps alive for the whole execution. A plain source, so
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

    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

    // The neighbours the index reported, nearest first, in the types the two chunks
    // carry. The search reads no column and its op is Pure, so one search answers every
    // step: a loop re-entered from an enclosing cross product slices these again rather
    // than asking the index a second time.
    bool hasSearched() const { return _searched; }
    void markSearched() { _searched = true; }

    std::vector<NodeID>& neighbourIDs() { return _neighbourIDs; }
    std::vector<std::optional<types::Double::Primitive>>& neighbourScores() { return _neighbourScores; }

private:
    ColumnNodeIDs* _ids {nullptr};
    Column* _scores {nullptr};
    std::string_view _indexName;
    size_t _neighbourCount {0};
    std::span<const float> _queryVector;
    NLLimitState* _limit {nullptr};
    std::vector<NodeID> _neighbourIDs;
    std::vector<std::optional<types::Double::Primitive>> _neighbourScores;
    bool _searched {false};
    NLStmtContainer _stmts;
};

// The rows one cell of an unwound column contributes: a list its element count, a null
// none, and any other value the single row it is. One per source column shape, selected
// during translation.
using NLUnwindElementCountFunction = size_t (*)(const Column* source, size_t row);

// Fill the element chunk of one nl.unwind step from a column whose cells hold more than
// the element: output row i is the element at positions[i] of the cell at rows[i].
using NLUnwindElementEmitFunction = void (*)(const Column* source,
                                             const ColumnVector<size_t>* rows,
                                             const ColumnVector<size_t>* positions,
                                             Column* output);

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

// nl.scan_edges loop data: the edge sibling of NLScanLoopData. A source loop
// (no input column, no carry set), so - unlike NLEdgeLoopData - it holds only
// the four fixed output chunks a step fills (sources, edge IDs, edge type IDs,
// targets), plus the limit and body a scan loop carries.
class NLScanEdgesLoopData : public NLFunctionData {
public:
    NLScanEdgesLoopData(ColumnNodeIDs* sources,
                        ColumnEdgeIDs* edgeIDs,
                        ColumnEdgeTypes* edgeTypes,
                        ColumnNodeIDs* targets)
        : _sources(sources),
        _edgeIDs(edgeIDs),
        _edgeTypes(edgeTypes),
        _targets(targets)
    {
    }

    ColumnNodeIDs* getSources() const { return _sources; }
    ColumnEdgeIDs* getEdgeIDs() const { return _edgeIDs; }
    ColumnEdgeTypes* getEdgeTypes() const { return _edgeTypes; }
    ColumnNodeIDs* getTargets() const { return _targets; }

    // The governing limit counter, or null for an unbounded loop. The loop
    // driver stops once it reaches zero.
    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    // The four fixed chunks of an edge iterator step, in loop-variable order
    ColumnNodeIDs* _sources {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnEdgeTypes* _edgeTypes {nullptr};
    ColumnNodeIDs* _targets {nullptr};

    NLLimitState* _limit {nullptr};
    NLStmtContainer _stmts;
};

// nl.scan_edges_by_type loop data: the edge scan restricted to one type. Reuses
// the plain edge scan's chunks, limit and body; adds the resolved EdgeTypeID the
// ScanEdgesByTypeChunkWriter filters by. _matchable is false when the type name
// was absent from the schema, so no edge can match and the loop emits no row -
// the scan sibling of NLEdgeByTypeLoopData.
class NLScanEdgesByTypeLoopData : public NLScanEdgesLoopData {
public:
    NLScanEdgesByTypeLoopData(ColumnNodeIDs* sources,
                              ColumnEdgeIDs* edgeIDs,
                              ColumnEdgeTypes* edgeTypes,
                              ColumnNodeIDs* targets,
                              EdgeTypeID edgeType,
                              bool matchable)
        : NLScanEdgesLoopData(sources, edgeIDs, edgeTypes, targets),
        _edgeType(edgeType),
        _matchable(matchable)
    {
    }

    EdgeTypeID getEdgeType() const { return _edgeType; }
    bool isMatchable() const { return _matchable; }

private:
    EdgeTypeID _edgeType;
    bool _matchable {true};
};

// nl.get_out_edges and nl.get_in_edges loop data
// The state is the same for get_out_edges/get_in_edges
class NLEdgeLoopData : public NLFunctionData {
public:
    using CarriedColumns = std::vector<NLCarriedColumn>;

    NLEdgeLoopData(const ColumnNodeIDs* input,
                   ColumnNodeIDs* sources,
                   ColumnEdgeIDs* edgeIDs,
                   ColumnEdgeTypes* edgeTypes,
                   ColumnNodeIDs* targets)
        : _inputNodeIDs(input),
        _sources(sources),
        _edgeIDs(edgeIDs),
        _edgeTypes(edgeTypes),
        _targets(targets)
    {
    }

    const ColumnNodeIDs* getInput() const { return _inputNodeIDs; }

    ColumnNodeIDs* getSources() const { return _sources; }
    ColumnEdgeIDs* getEdgeIDs() const { return _edgeIDs; }
    ColumnEdgeTypes* getEdgeTypes() const { return _edgeTypes; }
    ColumnNodeIDs* getTargets() const { return _targets; }

    ColumnVector<size_t>* getIndices() { return &_indices; }

    const CarriedColumns& carriedColumns() const { return _carriedColumns; }

    // The governing limit counter, or null for an unbounded loop. The loop
    // driver stops once it reaches zero.
    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

    void addCarriedColumn(const NLCarriedColumn& carried) {
        _carriedColumns.push_back(carried);
    }

private:
    const ColumnNodeIDs* _inputNodeIDs {nullptr};
    NLLimitState* _limit {nullptr};

    // The four fixed chunks of an edge iterator step, in loop-variable order
    ColumnNodeIDs* _sources {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnEdgeTypes* _edgeTypes {nullptr};
    ColumnNodeIDs* _targets {nullptr};

    CarriedColumns _carriedColumns;
    NLStmtContainer _stmts;

    // Scratch for the writer's row-to-input-row map, which drives the gathers
    ColumnVector<size_t> _indices;
};

// nl.get_out_edges_by_type / nl.get_in_edges_by_type loop data: an edge hop
// restricted to one edge type. Reuses the plain edge hop's chunks, limit, carry
// set and body; adds the resolved EdgeTypeID the Get{Out,In}EdgesByTypeChunkWriter
// filters by. _matchable is false when the type name was absent from the schema,
// so no edge can match and the loop emits no row - the edge sibling of
// NLScanByLabelLoopData.
class NLEdgeByTypeLoopData : public NLEdgeLoopData {
public:
    NLEdgeByTypeLoopData(const ColumnNodeIDs* input,
                         ColumnNodeIDs* sources,
                         ColumnEdgeIDs* edgeIDs,
                         ColumnEdgeTypes* edgeTypes,
                         ColumnNodeIDs* targets,
                         EdgeTypeID edgeType,
                         bool matchable)
        : NLEdgeLoopData(input, sources, edgeIDs, edgeTypes, targets),
        _edgeType(edgeType),
        _matchable(matchable)
    {
    }

    EdgeTypeID getEdgeType() const { return _edgeType; }
    bool isMatchable() const { return _matchable; }

private:
    EdgeTypeID _edgeType;
    bool _matchable {true};
};

// nl.get_node_properties / nl.get_edge_properties data: a with-null property
// read that maps the input ID column to a nullable value column, one value per
// input row (missing values are null, no row dropped). The node-vs-edge ID type
// and the value type are baked into the chosen handler; the PropertyTypeID was
// resolved from the name against the schema during translation.
class NLPropertyFetchData : public NLFunctionData {
public:
    NLPropertyFetchData(const Column* input, Column* output, PropertyTypeID propertyTypeID)
        : _input(input),
        _output(output),
        _propertyTypeID(propertyTypeID)
    {
    }

    const Column* getInput() const { return _input; }
    Column* getOutput() const { return _output; }
    PropertyTypeID getPropertyTypeID() const { return _propertyTypeID; }

    // The rows holding a write-buffer offset rather than an ID the graph knows, or null
    // for an input no merge produced. Such a row reads its value out of the write buffer.
    const ColumnMask* getPending() const { return _pending; }
    void setPending(const ColumnMask* pending) { _pending = pending; }

private:
    const Column* _input {nullptr};
    Column* _output {nullptr};
    const ColumnMask* _pending {nullptr};
    PropertyTypeID _propertyTypeID;
};

class NLGetNodeLabelSetData : public NLFunctionData {
public:
    NLGetNodeLabelSetData(const ColumnNodeIDs* input, ColumnLabelSetIDs* output)
        : _input(input),
        _output(output)
    {
    }

    const ColumnNodeIDs* getInput() const { return _input; }
    ColumnLabelSetIDs* getOutput() const { return _output; }

private:
    const ColumnNodeIDs* _input {nullptr};
    ColumnLabelSetIDs* _output {nullptr};
};

class NLCheckLabelConstraintData : public NLFunctionData {
public:
    NLCheckLabelConstraintData(const ColumnLabelSetIDs* input, ColumnMask* output)
        : _input(input),
        _output(output)
    {
    }

    const ColumnLabelSetIDs* getInput() const { return _input; }
    ColumnMask* getOutput() const { return _output; }

    void addMatchingID(LabelSetID id) { _matchingIDs.insert(id.getValue()); }
    bool isMatching(LabelSetID id) const { return _matchingIDs.count(id.getValue()) > 0; }

private:
    const ColumnLabelSetIDs* _input {nullptr};
    ColumnMask* _output {nullptr};
    std::unordered_set<uint32_t> _matchingIDs;
};

class NLCheckEdgeTypeConstraintData : public NLFunctionData {
public:
    NLCheckEdgeTypeConstraintData(const ColumnEdgeTypes* input, ColumnMask* output)
        : _input(input),
        _output(output)
    {
    }

    const ColumnEdgeTypes* getInput() const { return _input; }
    ColumnMask* getOutput() const { return _output; }

    void addMatchingID(EdgeTypeID id) { _matchingIDs.insert(id.getValue()); }
    bool isMatching(EdgeTypeID id) const { return _matchingIDs.count(id.getValue()) > 0; }

private:
    const ColumnEdgeTypes* _input {nullptr};
    ColumnMask* _output {nullptr};
    std::unordered_set<uint64_t> _matchingIDs;
};

// A cross product emits N*M rows (every outer row paired with every inner row),
// but an input column holds only its N or M values, so its values must be
// repeated to fill an N*M-row output column. That repetition is the broadcast.
// With outer [a0,a1] (N=2) and inner [b0,b1,b2] (M=3) the result is:
//   outer -> [a0,a0,a0, a1,a1,a1]   each outer row repeated M times (block-repeat)
//   inner -> [b0,b1,b2, b0,b1,b2]   whole inner chunk repeated N times (tile)
// so row k across all columns is one (outer, inner) pair. `factor` is M for an
// outer column, N for an inner one; both directions share this signature.
// `outputRowCount` caps the fill: it is min(N*M, remaining) under a limit, so the
// broadcast lays out only the product's first outputRowCount rows (its last block
// or tile may be partial) and skips the rest the limit could never emit. Without
// a limit it is the full N*M.
using NLBroadcastFunction = void (*)(const Column* input,
                                     size_t factor,
                                     size_t outputRowCount,
                                     Column* output);

// One column used as operand of nl.cross_product: its input chunk, the output
// chunk to fill, and the broadcast that fills one from the other.
class NLCrossColumn {
public:
    NLCrossColumn(const Column* input,
                  Column* output,
                  NLBroadcastFunction broadcast)
        : _input(input),
        _output(output),
        _broadcast(broadcast)
    {
    }

    const Column* getInput() const { return _input; }
    Column* getOutput() const { return _output; }
    NLBroadcastFunction getBroadcast() const { return _broadcast; }

private:
    const Column* _input {nullptr};
    Column* _output {nullptr};
    NLBroadcastFunction _broadcast {nullptr};
};

// nl.cross_product data: the outer and inner columns of a cartesian product.
// N (outer rows) and M (inner rows) are read at run time from the first column
// of each group. Each outer column is block-repeated x M and each inner column
// is tiled x N, so the product has N*M rows.
class NLCrossProductData : public NLFunctionData {
public:
    using Columns = std::vector<NLCrossColumn>;

    const Columns& outerColumns() const { return _outerColumns; }
    const Columns& innerColumns() const { return _innerColumns; }

    void addOuterColumn(const NLCrossColumn& column) {
        _outerColumns.push_back(column);
    }

    void addInnerColumn(const NLCrossColumn& column) {
        _innerColumns.push_back(column);
    }

    // The governing limit counter, or null for an unbounded product. When set,
    // the product is built only up to getRemaining() rows; it never mutates the
    // counter (the following nl.limit_update does).
    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

private:
    Columns _outerColumns;
    Columns _innerColumns;
    NLLimitState* _limit {nullptr};
};

// Lay a constant column's single value out over rowCount rows of a fresh output
// chunk, as one present value per row. The cross product's broadcast repeats a
// chunk's several values; this one repeats the single value a ColumnConst holds,
// so a fold that walks rows sees the step's rows rather than the one row the
// constant is. One per value type, selected during translation.
using NLBroadcastConstantFunction = void (*)(const Column* value,
                                             size_t rowCount,
                                             Column* output);

// nl.broadcast_constant data: the constant column to repeat, the driving
// relation's chunk whose row count says how many times - null when no relation
// drives the projection, which is a single row - the output chunk to fill and the
// fill that writes it.
class NLBroadcastConstantData : public NLFunctionData {
public:
    NLBroadcastConstantData(const Column* value,
                            const Column* cardinality,
                            Column* output,
                            NLBroadcastConstantFunction fill)
        : _value(value),
        _cardinality(cardinality),
        _output(output),
        _fill(fill)
    {
    }

    const Column* getValue() const { return _value; }
    const Column* getCardinality() const { return _cardinality; }
    Column* getOutput() const { return _output; }
    NLBroadcastConstantFunction getFill() const { return _fill; }

private:
    const Column* _value {nullptr};
    const Column* _cardinality {nullptr};
    Column* _output {nullptr};
    NLBroadcastConstantFunction _fill {nullptr};
};

// nl.limit data: resets a counter to its budget each time the block it lives in
// runs - once at function scope for a top-level LIMIT, or per enclosing step for
// a nested one.
class NLLimitInitData : public NLFunctionData {
public:
    NLLimitInitData(NLLimitState* state, size_t count)
        : _state(state),
        _count(count)
    {
    }

    NLLimitState* getState() const { return _state; }
    size_t getCount() const { return _count; }

private:
    NLLimitState* _state {nullptr};
    size_t _count {0};
};

// nl.limit_update data: the counter to charge and the representative column
// whose row count is charged against it (the same column nl.output emits).
class NLLimitUpdateData : public NLFunctionData {
public:
    NLLimitUpdateData(NLLimitState* state, const Column* rows)
        : _state(state),
        _rows(rows)
    {
    }

    NLLimitState* getState() const { return _state; }
    const Column* getRows() const { return _rows; }

private:
    NLLimitState* _state {nullptr};
    const Column* _rows {nullptr};
};

// nl.limit_truncate data: the counter whose emitThisStep sizes the copy and the
// columns to cut. Each NLCrossColumn pairs an input chunk with its fresh output
// chunk and the broadcast that fills one from the other - a block-repeat with
// factor 1, which copies each input row once and stops at emitThisStep, i.e. the
// prefix [0, emitThisStep). It never mutates the counter (nl.limit_update does).
class NLLimitTruncateData : public NLFunctionData {
public:
    NLLimitTruncateData(NLLimitState* state)
        : _state(state)
    {
    }

    NLLimitState* getState() const { return _state; }

    const std::vector<NLCrossColumn>& columns() const { return _columns; }

    void addColumn(const NLCrossColumn& column) {
        _columns.push_back(column);
    }

private:
    NLLimitState* _state {nullptr};
    std::vector<NLCrossColumn> _columns;
};

// Copies rows [inputOffset, inputOffset + rowCount) of an input chunk into a
// fresh, front-aligned output chunk. The skip suffix copy: nl.skip_truncate sets
// inputOffset = skipThisStep and rowCount = emitThisStep, lifting the surviving
// suffix to the front of the output. The block-repeat/tile broadcasts copy from
// row zero and so cannot express the offset; this family does. Cleared and
// resized to rowCount, the copy is a plain range copy (lowered to memcpy).
using NLCopyFunction = void (*)(const Column* input,
                                size_t inputOffset,
                                size_t rowCount,
                                Column* output);

// One column used as operand of nl.skip_truncate: its input chunk, the fresh
// output chunk to fill, and the range copy that lifts the surviving suffix from
// one into the other. The skip sibling of NLCrossColumn - it pairs the same input
// and output, but its copy takes an offset rather than a broadcast factor.
class NLSkipColumn {
public:
    NLSkipColumn(const Column* input,
                 Column* output,
                 NLCopyFunction copy)
        : _input(input),
        _output(output),
        _copy(copy)
    {
    }

    const Column* getInput() const { return _input; }
    Column* getOutput() const { return _output; }
    NLCopyFunction getCopy() const { return _copy; }

private:
    const Column* _input {nullptr};
    Column* _output {nullptr};
    NLCopyFunction _copy {nullptr};
};

// nl.skip data: resets a counter to the number of rows to drop each time the
// block it lives in runs - once at function scope for a top-level SKIP. The skip
// sibling of NLLimitInitData.
class NLSkipInitData : public NLFunctionData {
public:
    NLSkipInitData(NLSkipState* state, size_t count)
        : _state(state),
        _count(count)
    {
    }

    NLSkipState* getState() const { return _state; }
    size_t getCount() const { return _count; }

private:
    NLSkipState* _state {nullptr};
    size_t _count {0};
};

// nl.skip_update data: the counter to charge and the representative column whose
// row count is charged against it (the same column the consumer reads). The skip
// sibling of NLLimitUpdateData.
class NLSkipUpdateData : public NLFunctionData {
public:
    NLSkipUpdateData(NLSkipState* state, const Column* rows)
        : _state(state),
        _rows(rows)
    {
    }

    NLSkipState* getState() const { return _state; }
    const Column* getRows() const { return _rows; }

private:
    NLSkipState* _state {nullptr};
    const Column* _rows {nullptr};
};

// nl.skip_truncate data: the counter whose skipThisStep/emitThisStep size the copy
// and the columns to cut. Each NLSkipColumn pairs an input chunk with its fresh
// output chunk and the range copy that lifts the surviving suffix
// [skipThisStep, skipThisStep + emitThisStep) into it. It never mutates the counter
// (nl.skip_update does). The skip sibling of NLLimitTruncateData.
class NLSkipTruncateData : public NLFunctionData {
public:
    using SkipColumns = std::vector<NLSkipColumn>;

    NLSkipTruncateData(NLSkipState* state)
        : _state(state)
    {
    }

    NLSkipState* getState() const { return _state; }

    const SkipColumns& columns() const { return _columns; }

    void addColumn(const NLSkipColumn& column) {
        _columns.push_back(column);
    }

private:
    NLSkipState* _state {nullptr};
    SkipColumns _columns;
};

// Type of handle that appends one input chunk's rows to a growing sort buffer of
// the same element type. One per column kind / value type, selected during
// translation, the same way the gather and broadcast families are.
using NLAppendFunction = void (*)(const Column* input, Column* buffer);

// Type of handle that 3-way compares two rows of one sort key column: negative
// if row a sorts before row b, positive if after, zero if they tie on this key.
// Direction (ascending vs descending) is applied by the caller, so the function
// itself is direction-free; it is selected per key from the column's element type.
using NLCompareFunction = int (*)(const Column* column, size_t a, size_t b);

// Runtime state of one ORDER BY: the growing per-column row buffers, the sort
// keys (each a buffer column, a direction and a 3-way comparator), and the row
// permutation computed once when the emit loop first steps. nl.sort_buffer
// resets it, nl.sort_collect appends one chunk to every buffer, and the nl.for
// over nl.sort sorts it (once) and reads the rows in permutation order. The
// buffer columns are borrowed: the translator pool-allocates them in the same
// arena as the loop columns; the state only records the pointers.
//
// A bounded accumulator (the fused ORDER BY ... LIMIT k) keeps only the best k
// rows: nl.sort_collect appends a chunk then calls trimIfNeeded, which selects
// the best k and reclaims the rest, so memory stays O(k) and the final sort runs
// over O(k) rows rather than every row.
class NLSortState {
public:
    // One sort key: the buffer column it orders by, its direction (true =
    // ascending), and the 3-way comparator for that column's element type.
    struct Key {
        const Column* _buffer {nullptr};
        bool _ascending {true};
        NLCompareFunction _compare {nullptr};
    };

    // Bound the accumulator to the best topK rows (the fused top-K form). Left
    // unbounded, it keeps every collected row.
    void setTopK(size_t topK) {
        _topK = topK;
        _bounded = true;
    }

    bool isBounded() const { return _bounded; }

    // Record one column: its buffer, the scratch the trim gathers the survivors
    // into (null when unbounded - no trimming happens), and the gather that fills
    // one from the other. Kept parallel, one entry per collected column.
    void addColumnBuffer(Column* buffer, Column* temp, NLGatherFunction gather) {
        _buffers.push_back(buffer);
        _tempBuffers.push_back(temp);
        _gathers.push_back(gather);
    }

    const std::vector<Column*>& buffers() const { return _buffers; }
    Column* buffer(size_t index) const { return _buffers[index]; }

    void addKey(const Key& key) { _keys.push_back(key); }

    // Clear every buffer and drop the computed order, so the accumulator is empty
    // again. Runs each time nl.sort_buffer's block runs (once at function scope).
    void reset();

    // For a bounded accumulator, once the buffers have grown well past the bound,
    // drop all but the best topK rows so memory stays O(topK). A no-op when
    // unbounded or still within the bound.
    void trimIfNeeded();

    // Compute the row permutation that orders the buffers by the keys, stable so
    // rows equal on every key keep their collected order; for a bounded
    // accumulator the permutation is then capped to the best topK. Idempotent:
    // the first emit step sorts, later calls are no-ops.
    void sort();

    const ColumnVector<size_t>& permutation() const { return _permutation; }

private:
    // Strict-weak-ordering row comparison by the keys, most significant first;
    // each key's direction flips the sign. Shared by sort() and the trim.
    bool rowLess(size_t leftRow, size_t rightRow) const;

    // Select the best topK rows into _keptIndices and compact every buffer to
    // them, reclaiming the rest. Called by trimIfNeeded once the buffers overflow.
    void trimToTopK(size_t rowCount);

    // One buffer per collected column, row-aligned, grown by nl.sort_collect.
    std::vector<Column*> _buffers;

    // Per-buffer compaction scratch and gather, used only by a bounded trim.
    std::vector<Column*> _tempBuffers;
    std::vector<NLGatherFunction> _gathers;

    // The sort keys, most significant first.
    std::vector<Key> _keys;

    // Row order computed by sort(); read by the emit loop in chunk-sized slices.
    ColumnVector<size_t> _permutation;

    // The kept-row indices a trim selects, fed to the per-buffer gather.
    ColumnVector<size_t> _keptIndices;

    // The top-K bound (valid only when _bounded); 0 with _bounded means keep none.
    size_t _topK {0};
    bool _bounded {false};

    bool _sorted {false};
};

// nl.sort_buffer data: resets an accumulator to empty each time the block it
// lives in runs - once at function scope for a top-level ORDER BY.
class NLSortResetData : public NLFunctionData {
public:
    NLSortResetData(NLSortState* state)
        : _state(state)
    {
    }

    NLSortState* getState() const { return _state; }

private:
    NLSortState* _state {nullptr};
};

// nl.sort_collect data: appends the current chunk of every column to the matching
// buffer of the accumulator. Each entry pairs the loop's input chunk with the
// buffer it grows and the append that copies one into the other.
class NLSortCollectData : public NLFunctionData {
public:
    struct Append {
        const Column* _input {nullptr};
        Column* _buffer {nullptr};
        NLAppendFunction _append {nullptr};
    };

    NLSortCollectData(NLSortState* state)
        : _state(state)
    {
    }

    NLSortState* getState() const { return _state; }

    const std::vector<Append>& appends() const { return _appends; }

    void addAppend(const Append& append) {
        _appends.push_back(append);
    }

private:
    NLSortState* _state {nullptr};
    std::vector<Append> _appends;
};

// nl.for over nl.sort data: the emit phase of an ORDER BY. Holds the accumulator
// (sorted on the first step), and per column the buffer to read, the loop
// variable to fill and the gather that copies a chunk of rows by index - reusing
// the same NLCarriedColumn (input, output, gather) shape the edge loops use. The
// indices scratch holds the permutation slice of the current emit step.
class NLSortLoopData : public NLFunctionData {
public:
    NLSortLoopData(NLSortState* state)
        : _state(state)
    {
    }

    NLSortState* getState() const { return _state; }

    const std::vector<NLCarriedColumn>& columns() const { return _columns; }

    void addColumn(const NLCarriedColumn& column) {
        _columns.push_back(column);
    }

    ColumnVector<size_t>* getIndices() { return &_indices; }

    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    NLSortState* _state {nullptr};
    NLLimitState* _limit {nullptr};
    std::vector<NLCarriedColumn> _columns;
    ColumnVector<size_t> _indices;
    NLStmtContainer _stmts;
};

// Type of handle that appends the raw bytes of one row of a column to a growing
// row-key buffer. One per column kind / value type, selected during translation,
// the same way the gather and append families are. The concatenated key of a row
// (all its columns in order) is what nl.distinct_filter looks up in the seen-set:
// bytewise key equality is row equality. A null serializes to a fixed tag byte, so
// all nulls share a key and DISTINCT dedups them together, matching Cypher; a
// string serializes as a length prefix then its characters, so two rows never
// collide by concatenation (e.g. "a"+"b" versus "ab"+"").
using NLKeyAppendFunction = void (*)(const Column* column, size_t row, std::string& key);

// Runtime state of one DISTINCT: the set of serialized row keys already emitted.
// nl.distinct resets it (once at function scope for a top-level or mid-query
// DISTINCT), and nl.distinct_filter is the sole reader and mutator - it looks up
// each incoming row's key and inserts the new ones. The streaming sibling of
// NLSortState: it filters rows as they arrive rather than accumulating them, so it
// holds only the keys, not the row values (a survivor is emitted from the incoming
// chunk it appeared in, never rebuilt).
class NLDistinctState {
public:
    // Empty the seen-set, so DISTINCT starts fresh. Runs each time nl.distinct's
    // block runs (once at function scope for a top-level / mid-query DISTINCT).
    void reset() { _seen.clear(); }

    // Record a row's serialized key; returns true if the row is new (not seen
    // before) and should survive, false if it duplicates an earlier row. The sole
    // mutator of the set.
    bool insertIfNew(const std::string& key) { return _seen.insert(key).second; }

private:
    // One serialized key per distinct row seen so far.
    std::unordered_set<std::string> _seen;
};

// nl.distinct data: resets a seen-set to empty each time the block it lives in
// runs - once at function scope for a top-level DISTINCT. The distinct sibling of
// NLSortResetData.
class NLDistinctResetData : public NLFunctionData {
public:
    NLDistinctResetData(NLDistinctState* state)
        : _state(state)
    {
    }

    NLDistinctState* getState() const { return _state; }

private:
    NLDistinctState* _state {nullptr};
};

// nl.distinct_filter data: the seen-set to look each row up in, and per column its
// input chunk, the fresh output chunk to fill, the key-append that serializes one
// of its rows into the row key, and the gather that copies the surviving rows into
// the output. All columns together form each row's key; the surviving rows are
// gathered from the incoming chunk (reusing the NLGatherFunction the edge loops
// use), so no row values are stored. The indices scratch holds this step's
// surviving row indices and the key scratch is reused per row.
class NLDistinctFilterData : public NLFunctionData {
public:
    struct FilterColumn {
        const Column* _input {nullptr};
        Column* _output {nullptr};
        NLKeyAppendFunction _keyAppend {nullptr};
        NLGatherFunction _gather {nullptr};
    };

    NLDistinctFilterData(NLDistinctState* state)
        : _state(state)
    {
    }

    NLDistinctState* getState() const { return _state; }

    const std::vector<FilterColumn>& columns() const { return _columns; }

    void addColumn(const FilterColumn& column) {
        _columns.push_back(column);
    }

    ColumnVector<size_t>* getIndices() { return &_indices; }
    std::string* getKeyScratch() { return &_key; }

private:
    NLDistinctState* _state {nullptr};
    std::vector<FilterColumn> _columns;

    // Scratch for this step's surviving row indices, fed to the per-column gather
    ColumnVector<size_t> _indices;

    // Scratch reused to build each row's key, cleared once per row
    std::string _key;
};

class NLFilterData : public NLFunctionData {
public:
    struct FilterColumn {
        const Column* _input {nullptr};
        Column* _output {nullptr};
        NLGatherFunction _gather {nullptr};
    };

    NLFilterData(const Column* mask, NLMaskSurvivorFunction survivors)
        : _mask(mask),
        _survivors(survivors)
    {
    }

    const Column* getMask() const { return _mask; }
    NLMaskSurvivorFunction getSurvivors() const { return _survivors; }

    const std::vector<FilterColumn>& columns() const { return _columns; }

    void addColumn(const FilterColumn& column) {
        _columns.push_back(column);
    }

    ColumnVector<size_t>* getIndices() { return &_indices; }

private:
    const Column* _mask {nullptr};
    NLMaskSurvivorFunction _survivors {nullptr}; // handles nullable vs non-nullable masks
    std::vector<FilterColumn> _columns;

    // Scratch for this step's surviving row indices, fed to gather
    ColumnVector<size_t> _indices;
};

// Type of handle that returns how many rows of a column are non-null. One per
// column kind / value type, selected during translation the same way the gather
// and append families are. An ID column has no nulls, so its handle returns the
// row count; a nullable value column's handle returns its present-value count.
// This is what nl.count_update adds to the tally each step.
using NLCountFunction = size_t (*)(const Column* column);

// Runtime state of one COUNT: the running tally of non-null rows seen so far.
// nl.count resets it (once at function scope for a top-level or mid-query COUNT),
// nl.count_update is the sole incrementer, and nl.count_result reads the final
// value to emit the single result row. The counting sibling of NLSortState: it
// tallies rows as they arrive rather than accumulating them, so it holds only a
// count, not the row values.
class NLCountState {
public:
    // Zero the tally, so COUNT starts fresh. Runs each time nl.count's block runs
    // (once at function scope for a top-level / mid-query COUNT).
    void reset() { _count = 0; }

    // Add this step's non-null row count to the tally. The sole mutator.
    void add(size_t rows) { _count += rows; }

    // The final tally, read once by nl.count_result after the producing loop.
    size_t getCount() const { return _count; }

private:
    size_t _count {0};
};

// nl.count data: zeroes a tally each time the block it lives in runs - once at
// function scope for a top-level COUNT. The count sibling of NLSortResetData.
class NLCountResetData : public NLFunctionData {
public:
    NLCountResetData(NLCountState* state)
        : _state(state)
    {
    }

    NLCountState* getState() const { return _state; }

private:
    NLCountState* _state {nullptr};
};

// nl.count_update data: the tally to charge, the input chunk to measure, and the
// count-of-non-null handle that measures it (all rows for an ID chunk, the present
// values for a nullable value chunk). Runs once per producing-loop step.
class NLCountUpdateData : public NLFunctionData {
public:
    NLCountUpdateData(NLCountState* state, const Column* rows, NLCountFunction count)
        : _state(state),
        _rows(rows),
        _count(count)
    {
    }

    NLCountState* getState() const { return _state; }
    const Column* getRows() const { return _rows; }
    NLCountFunction getCount() const { return _count; }

private:
    NLCountState* _state {nullptr};
    const Column* _rows {nullptr};
    NLCountFunction _count {nullptr};
};

// nl.count_result data: the emit step of a COUNT. Holds the tally and the output
// column to fill with the single result row - an unsigned i64 count column
// (ColumnVector<uint64_t>). Runs once, after the producing loop, so the tally is
// final; it materializes the chunk in place, with no body of its own (nl.output
// consumes it at function scope).
class NLCountResultData : public NLFunctionData {
public:
    NLCountResultData(NLCountState* state, Column* output)
        : _state(state),
        _output(output)
    {
    }

    NLCountState* getState() const { return _state; }
    Column* getOutput() const { return _output; }

private:
    NLCountState* _state {nullptr};
    Column* _output {nullptr};
};

// The reduction one nl.aggregate applies. The runtime counterpart of the MLIR
// storage::AggregateKind: the translator maps the op's kind onto this, and it -
// with the column value type - selects the reset/update/result handlers below.
enum class AggregateKind {
    Sum,
    Min,
    Max,
    Avg,
};

// Runtime state of one SUM/MIN/MAX/AVG: the running accumulator. The value
// sibling of NLCountState - where a count holds a tally, this holds the reduced
// value in a single-row nullable value column (owned elsewhere; a borrowed
// pointer here), plus a count of the non-null rows folded in (only avg divides by
// it). nl.aggregate resets it, nl.aggregate_update is the sole folder, and
// nl.aggregate_result reads it to emit the single result row. The accumulator's
// element type is fixed by the handlers, so the state itself is type-erased.
class NLAggregateState {
public:
    // The single-row accumulator column (a ColumnOptVector of the accumulator's
    // primitive), reset by the reset handler and folded into by the update handler.
    Column* getAccumulator() const { return _accumulator; }
    void setAccumulator(Column* accumulator) { _accumulator = accumulator; }

    // The number of non-null rows folded in so far, used only by avg.
    size_t getCount() const { return _count; }
    void setCount(size_t count) { _count = count; }
    void addCount(size_t count) { _count += count; }

private:
    Column* _accumulator {nullptr};
    size_t _count {0};
};

// Handlers of one aggregate, selected during translation from the reduction and
// the column value type (as the count / gather / append families are), so the
// per-type fan-out lives in one place. Reset re-initializes the accumulator (a
// present zero for sum/avg, null for min/max) and zeroes the count; update folds
// a chunk's non-null values into the accumulator; result writes the single
// reduced row into the output column.
using NLAggregateResetFunction = void (*)(NLAggregateState* state);
using NLAggregateUpdateFunction = void (*)(NLAggregateState* state, const Column* input);
using NLAggregateResultFunction = void (*)(const NLAggregateState* state, Column* output);

// nl.aggregate data: re-initializes an accumulator each time the block it lives in
// runs - once at function scope for a top-level aggregate. The value sibling of
// NLCountResetData; the reset handler knows the accumulator's type and kind.
class NLAggregateResetData : public NLFunctionData {
public:
    NLAggregateResetData(NLAggregateState* state, NLAggregateResetFunction reset)
        : _state(state),
        _reset(reset)
    {
    }

    NLAggregateState* getState() const { return _state; }
    NLAggregateResetFunction getReset() const { return _reset; }

private:
    NLAggregateState* _state {nullptr};
    NLAggregateResetFunction _reset {nullptr};
};

// nl.aggregate_update data: the accumulator to fold into, the input chunk to
// reduce, and the fold handler that reduces it (selected from the kind and the
// input value type). Runs once per producing-loop step. The value sibling of
// NLCountUpdateData.
class NLAggregateUpdateData : public NLFunctionData {
public:
    NLAggregateUpdateData(NLAggregateState* state, const Column* input, NLAggregateUpdateFunction update)
        : _state(state),
        _input(input),
        _update(update)
    {
    }

    NLAggregateState* getState() const { return _state; }
    const Column* getInput() const { return _input; }
    NLAggregateUpdateFunction getUpdate() const { return _update; }

private:
    NLAggregateState* _state {nullptr};
    const Column* _input {nullptr};
    NLAggregateUpdateFunction _update {nullptr};
};

// nl.aggregate_result data: the emit step of an aggregate. Holds the accumulator,
// the output column to fill with the single result row (a nullable value column),
// and the emit handler (a copy for sum/min/max, a divide-by-count for avg). Runs
// once, after the producing loop, so the accumulator is final. The value sibling
// of NLCountResultData.
class NLAggregateResultData : public NLFunctionData {
public:
    NLAggregateResultData(NLAggregateState* state, Column* output, NLAggregateResultFunction result)
        : _state(state),
        _output(output),
        _result(result)
    {
    }

    NLAggregateState* getState() const { return _state; }
    Column* getOutput() const { return _output; }
    NLAggregateResultFunction getResult() const { return _result; }

private:
    NLAggregateState* _state {nullptr};
    Column* _output {nullptr};
    NLAggregateResultFunction _result {nullptr};
};

// The reduction one function of a grouped aggregation applies. The runtime
// counterpart of the MLIR storage::GroupAggregateKind: the translator maps the
// op's kind onto this and - with the column value type - selects the grow/fold/
// emit handlers below. Unlike the scalar AggregateKind it includes Count, since a
// grouped aggregate tallies each group's rows the same way it reduces their
// values.
enum class GroupAggregateKind {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    CountDistinct,
    SumDistinct,
    AvgDistinct,
    CountRows,
};

// The (group, value) pairs one DISTINCT aggregate has already charged. A single set
// covers every group of the aggregate: the group index is prefixed onto the
// serialized value, so two groups never share an entry. The distinct fold builds one
// row's key in the scratch, then charges the group only when the pair is new - a
// tally for count(DISTINCT x), a reduction for sum/avg(DISTINCT x). The grouped
// sibling of NLDistinctState, which keys whole rows rather than one value per group.
class NLGroupDistinctTally {
public:
    // Start this row's key with the group it belongs to, so the value bytes the fold
    // appends next are told apart per group.
    void beginKey(size_t group) {
        _key.assign(reinterpret_cast<const char*>(&group), sizeof(group));
    }

    std::string& getKey() { return _key; }

    // Record the key built for this row; true when the (group, value) pair is new,
    // so the caller charges the group's tally.
    bool insertIfNew() { return _seen.insert(_key).second; }

    // Drop every pair, so the aggregation starts fresh. Runs with the rest of the
    // per-group state when nl.group_aggregate_buffer resets.
    void clear() { _seen.clear(); }

private:
    std::unordered_set<std::string> _seen;
    std::string _key;
};

// Append the values of the given rows of an input chunk onto the tail of a
// growing key buffer of the same element type. nl.group_aggregate_update calls
// this with the rows that created a new group this step, so the buffer holds one
// key value per group in group-creation order. One per column kind / value type,
// selected during translation the way the gather and append families are.
using NLGroupKeyGatherFunction = void (*)(const Column* input,
                                          const std::vector<size_t>& rows,
                                          Column* buffer);

// Grow one aggregate's per-group state to hold groupCount groups, initializing the
// newly added groups to the reduction's identity - a present zero (sum/avg), a
// null (min/max) or a zero tally (count). The accumulator column holds the reduced
// value per group (null for count); counts holds the per-group non-null tally
// (used by count and avg). One per kind / value type, selected during translation.
using NLGroupAggregateGrowFunction = void (*)(Column* accumulator,
                                              std::vector<uint64_t>& counts,
                                              size_t groupCount);

// Fold this step's chunk into the per-group state: for each input row, reduce its
// value (or tally it) into the group named by groups[row]. groups is the row ->
// group-index map nl.group_aggregate_update built for this step. One per kind /
// value type. The parameters are the union of what any reduction needs, so each fold
// reads only its own: sum/min/max ignore counts, count ignores the accumulator, and
// only the distinct kinds touch the tally of already-charged (group, value) pairs.
using NLGroupAggregateFoldFunction = void (*)(Column* accumulator,
                                              std::vector<uint64_t>& counts,
                                              const Column* input,
                                              const std::vector<size_t>& groups,
                                              NLGroupDistinctTally& distinct);

// Materialize the reduction of groups [begin, begin + count) into an emit output
// column: a copy of the accumulator slice (sum/min/max), a per-group divide
// (avg), or the tally slice (count). Runs once per emit chunk. One per kind /
// value type.
using NLGroupAggregateEmitFunction = void (*)(const Column* accumulator,
                                              const std::vector<uint64_t>& counts,
                                              size_t begin,
                                              size_t count,
                                              Column* output);

// The group table of one grouped aggregation: it maps a serialized grouping-key
// tuple to a dense group index (0, 1, 2, ... in first-seen order). Each distinct
// key tuple takes the next index the first time it is seen through assign(); that
// index is the row the group occupies in every key buffer and accumulator.
// nl.group_aggregate_update drives it one row at a time and nl.group_aggregate_buffer
// clears it.
//
// This is the single global open-addressing table conceptually; it is a
// std::unordered_map here (the "simplest correct thing" first cut), reified behind
// assign() / getGroupCount() so callers never poke the map and a separate group
// counter in lockstep - and so a future open-addressing rewrite stays local to
// this class.
class NLGroupTable {
public:
    // The outcome of assigning one row's grouping-key tuple to a group: the dense
    // group index, and whether this call created the group (the tuple was seen for
    // the first time). The update gathers a new group's key values from the row
    // that created it, so it keys off _created.
    struct Assignment {
        size_t _index {0};
        bool _created {false};
    };

    // Map a serialized grouping-key tuple to its dense group index, creating the
    // group with the next index the first time the tuple is seen.
    Assignment assign(const std::string& key);

    // The number of distinct groups seen so far - the dense index the next new
    // group would take.
    size_t getGroupCount() const { return _groups.size(); }

    // Drop every group, so the next assign() numbers groups from zero again.
    void clear();

private:
    std::unordered_map<std::string, size_t> _groups;
};

// Runtime state of one grouped aggregation: an NLGroupTable from the serialized
// grouping-key tuple to a dense group index, the distinct key values per group,
// and one accumulator per aggregate function. nl.group_aggregate_buffer resets it,
// nl.group_aggregate_update assigns each row to its group and folds the aggregate
// inputs, and the nl.for over nl.group_aggregate emits one row per group. The
// grouped sibling of NLSortState: it keys rows by the grouping tuple and reduces
// within each group rather than buffering every row for a global reorder.
//
// The accumulators are designed row-per-group so a future two-phase merge can
// combine two tables group by group.
class NLGroupAggregateState {
public:
    // One grouping-key column. The distinct key value per group accumulates in
    // _buffer (grown as groups appear, in creation order); the update reads the
    // incoming chunk _input and the emit fills the loop variable _output.
    // _keyAppend serializes a row into the group key (the DISTINCT serializer),
    // _gatherAppend copies the new-group rows into _buffer, and _emitCopy copies a
    // buffer slice into _output at emit.
    struct KeyColumn {
        const Column* _input {nullptr};
        Column* _buffer {nullptr};
        Column* _output {nullptr};
        NLKeyAppendFunction _keyAppend {nullptr};
        NLGroupKeyGatherFunction _gatherAppend {nullptr};
        NLCopyFunction _emitCopy {nullptr};
    };

    // One aggregate. The update folds the incoming chunk _input into the per-group
    // state (_accumulator, one reduced value per group - null for count - and
    // _counts, one non-null tally per group, used by count and avg); the emit fills
    // the loop variable _output. _grow/_fold/_emit are baked from the kind and the
    // value type. _distinct is the tally of a DISTINCT aggregate, left empty by every
    // other reduction.
    struct Aggregate {
        const Column* _input {nullptr};
        Column* _accumulator {nullptr};
        Column* _output {nullptr};
        std::vector<uint64_t> _counts;
        NLGroupDistinctTally _distinct;
        NLGroupAggregateGrowFunction _grow {nullptr};
        NLGroupAggregateFoldFunction _fold {nullptr};
        NLGroupAggregateEmitFunction _emit {nullptr};
    };

    void addKeyColumn(const KeyColumn& key) { _keyColumns.push_back(key); }
    void addAggregate(const Aggregate& aggregate) { _aggregates.push_back(aggregate); }

    // Mutable so the emit-loop translation can fill in each column's _output after
    // the update translation set up the rest.
    std::vector<KeyColumn>& keyColumns() { return _keyColumns; }
    std::vector<Aggregate>& aggregates() { return _aggregates; }

    NLGroupTable& groupTable() { return _groupTable; }

    // Scratch reused per update step: the row key being built, the per-row group
    // index map, and the incoming rows that created a new group this step.
    std::string& keyScratch() { return _key; }
    std::vector<size_t>& groupIndicesScratch() { return _groupIndices; }
    std::vector<size_t>& newGroupRowsScratch() { return _newGroupRows; }

    // Empty the group table, key buffers, accumulators and tallies, so the
    // aggregation starts fresh. Runs each time nl.group_aggregate_buffer's block
    // runs (once at function scope for a top-level grouped RETURN).
    void reset();

private:
    std::vector<KeyColumn> _keyColumns;
    std::vector<Aggregate> _aggregates;

    NLGroupTable _groupTable;

    std::string _key;
    std::vector<size_t> _groupIndices;
    std::vector<size_t> _newGroupRows;
};

// nl.group_aggregate_buffer data: resets a grouped accumulator to empty each time
// the block it lives in runs - once at function scope for a top-level grouped
// RETURN. The grouped sibling of NLSortResetData.
class NLGroupAggregateResetData : public NLFunctionData {
public:
    NLGroupAggregateResetData(NLGroupAggregateState* state)
        : _state(state)
    {
    }

    NLGroupAggregateState* getState() const { return _state; }

private:
    NLGroupAggregateState* _state {nullptr};
};

// nl.group_aggregate_update data: the accumulator to fold this step's chunk into.
// The key columns and aggregates it reads live on the shared state (their _input
// chunks are the loop variables refilled each step), so this only names the state.
// The grouped sibling of NLSortCollectData.
class NLGroupAggregateUpdateData : public NLFunctionData {
public:
    NLGroupAggregateUpdateData(NLGroupAggregateState* state)
        : _state(state)
    {
    }

    NLGroupAggregateState* getState() const { return _state; }

private:
    NLGroupAggregateState* _state {nullptr};
};

// nl.for over nl.group_aggregate data: the emit phase of a grouped aggregation.
// The key buffers, accumulators and emit outputs all live on the shared state, so
// this holds the state (fully folded by now) and the loop body to run per emit
// chunk. The grouped sibling of NLSortLoopData.
class NLGroupAggregateLoopData : public NLFunctionData {
public:
    NLGroupAggregateLoopData(NLGroupAggregateState* state)
        : _state(state)
    {
    }

    NLGroupAggregateState* getState() const { return _state; }

    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    NLGroupAggregateState* _state {nullptr};
    NLLimitState* _limit {nullptr};
    NLStmtContainer _stmts;
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

// Append this step's present values to a collect accumulator's flat value buffer,
// recording each element's position in its group's list. nl.collect_update calls this
// after assigning rows to groups; a null value is skipped (Cypher collect ignores
// nulls). One per value type, selected during translation the way the fold families
// are.
using NLCollectFoldFunction = void (*)(Column* values,
                                       const Column* input,
                                       const std::vector<size_t>& groups,
                                       std::vector<std::vector<size_t>>& groupPositions,
                                       NLGroupDistinctTally& distinct);

// Emit a chunk of unwound values (the nl.unwind_collect drain): for each flat-buffer position
// this chunk covers, write the present value into the nullable value output. One per
// value type, selected during translation.
using NLUnwindCollectValueEmitFunction = void (*)(const Column* values,
                                           const ColumnVector<size_t>* positions,
                                           Column* output);

// Emit a chunk of per-group lists (the nl.collect drain): for each group in
// [begin, begin + count), gather its elements from the flat buffer (by its position
// list) into the list buffer as one contiguous run and store the resulting ListView in
// the list output. One per value type, selected during translation.
using NLCollectListEmitFunction = void (*)(const Column* values,
                                           const std::vector<std::vector<size_t>>& groupPositions,
                                           size_t begin,
                                           size_t count,
                                           ListBuffer<>& listBuffer,
                                           Column* output);

// Runtime state of one collect: an NLGroupTable from the serialized grouping-key tuple
// to a dense group index, the distinct key values per group, and - the accumulator - a
// single flat buffer of collected values plus, per group, the positions of its
// elements in that buffer. nl.collect_buffer resets it, nl.collect_update assigns each
// row to its group and appends its value, and the drain (nl.unwind_collect per element, or
// nl.collect per group) reads it. The list-valued sibling of NLGroupAggregateState: it
// keeps every value per group rather than reducing them to one.
//
// The flat buffer keeps values in global append order; a group's elements are the
// buffer entries its position list names (not contiguous, since groups interleave). A
// future drain that needs a contiguous per-group run gathers those positions into a
// ListBuffer - the stage-then-insert path of docs/UNWIND.md.
class NLCollectState {
public:
    // One grouping-key column - identical to NLGroupAggregateState's: the distinct key
    // value per group accumulates in _buffer (grown as groups appear), the update reads
    // the incoming chunk _input, _keyAppend serializes a row into the group key,
    // _gatherAppend copies new-group rows into _buffer, and the drain fills _output
    // through _emitCopy.
    struct KeyColumn {
        const Column* _input {nullptr};
        Column* _buffer {nullptr};
        Column* _output {nullptr};
        NLKeyAppendFunction _keyAppend {nullptr};
        NLGroupKeyGatherFunction _gatherAppend {nullptr};
        NLCopyFunction _emitCopy {nullptr};
        // Used only by the nl.unwind_collect drain: gather the key buffer by a per-emitted-row
        // group index, so a group's key value repeats once per collected element.
        NLGatherFunction _gather {nullptr};
    };

    // One collected column - Cypher's collect(x). _distinct holds the (group, value) pairs
    // already charged, so collect(DISTINCT x) takes each value once and a plain collect
    // leaves it empty; _output is filled either as the per-group list cell or as the
    // unwound value, which is why the drain carries an emit handler for each.
    struct ValueColumn {
        const Column* _input {nullptr};
        Column* _buffer {nullptr};
        Column* _output {nullptr};
        NLCollectFoldFunction _fold {nullptr};
        NLCollectListEmitFunction _listEmit {nullptr};
        NLUnwindCollectValueEmitFunction _unwindCollectEmit {nullptr};
        NLGroupDistinctTally _distinct;
        std::vector<std::vector<size_t>> _groupPositions;
    };

    void addKeyColumn(const KeyColumn& key) { _keyColumns.push_back(key); }
    std::vector<KeyColumn>& keyColumns() { return _keyColumns; }

    NLGroupTable& groupTable() { return _groupTable; }

    void addValueColumn(const ValueColumn& value) { _valueColumns.push_back(value); }
    std::vector<ValueColumn>& valueColumns() { return _valueColumns; }
    const std::vector<ValueColumn>& valueColumns() const { return _valueColumns; }

    // An nl.unwind_collect drain reads exactly one collected column, so it names it
    // rather than indexing the list every caller would otherwise have to bound.
    ValueColumn& unwoundColumn();

    // The reductions taken over the same groups as the list - Cypher's collect(x) beside
    // count(y) - each folded and emitted exactly as a grouped aggregation does, since one
    // accumulator holds the groups both read.
    void addAggregate(const NLGroupAggregateState::Aggregate& aggregate) { _aggregates.push_back(aggregate); }
    std::vector<NLGroupAggregateState::Aggregate>& aggregates() { return _aggregates; }

    // The query-scoped list buffer the nl.collect drain materializes per-group runs
    // into; the ListViews it hands out span it and stay valid for the whole run. Every
    // collected column shares it, since each run is contiguous per insert.
    ListBuffer<>& listBuffer() { return _listBuffer; }

    // Scratch reused per update step: the row key being built, the per-row group index
    // map, and the incoming rows that created a new group this step.
    std::string& keyScratch() { return _key; }
    std::vector<size_t>& groupIndicesScratch() { return _groupIndices; }
    std::vector<size_t>& newGroupRowsScratch() { return _newGroupRows; }

    // Empty the group table, key buffers, value buffers and per-group positions, so the
    // collect starts fresh. Runs each time nl.collect_buffer's block runs. An ungrouped
    // collect comes back with its single empty group already created, since that group
    // does not depend on any row arriving.
    void reset();

private:
    std::vector<KeyColumn> _keyColumns;

    std::vector<ValueColumn> _valueColumns;

    ListBuffer<> _listBuffer;

    std::vector<NLGroupAggregateState::Aggregate> _aggregates;

    NLGroupTable _groupTable;

    std::string _key;
    std::vector<size_t> _groupIndices;
    std::vector<size_t> _newGroupRows;
};

// nl.collect_buffer data: resets a collect accumulator to empty each time the block it
// lives in runs - once at function scope for a top-level collect. The list-valued
// sibling of NLGroupAggregateResetData.
class NLCollectResetData : public NLFunctionData {
public:
    NLCollectResetData(NLCollectState* state)
        : _state(state)
    {
    }

    NLCollectState* getState() const { return _state; }

private:
    NLCollectState* _state {nullptr};
};

// nl.collect_update data: the accumulator to append this step's chunk to. The key
// columns and the value column it reads live on the shared state (their _input chunks
// are the loop variables refilled each step), so this only names the state. The
// list-valued sibling of NLGroupAggregateUpdateData.
class NLCollectUpdateData : public NLFunctionData {
public:
    NLCollectUpdateData(NLCollectState* state)
        : _state(state)
    {
    }

    NLCollectState* getState() const { return _state; }

private:
    NLCollectState* _state {nullptr};
};

// nl.for over nl.unwind_collect data: the per-element drain of a collect. The state (fully
// filled by now) holds the key buffers, flat value buffer, per-group positions and the
// emit outputs; this holds the state, the loop body, and per-chunk scratch for the
// group index and value position of each emitted row.
class NLUnwindCollectLoopData : public NLFunctionData {
public:
    NLUnwindCollectLoopData(NLCollectState* state)
        : _state(state)
    {
    }

    NLCollectState* getState() const { return _state; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

    ColumnVector<size_t>* getGroupIndices() { return &_groupIndices; }
    ColumnVector<size_t>* getPositions() { return &_positions; }

private:
    NLCollectState* _state {nullptr};
    NLStmtContainer _stmts;

    // This step's per-row group index (to gather the repeated key values) and value
    // position (to emit the element), one entry per emitted row.
    ColumnVector<size_t> _groupIndices;
    ColumnVector<size_t> _positions;
};

// nl.for over nl.collect data: the per-group drain of a collect. The key/list outputs
// and the list buffer live on the shared state, so this holds the state (fully filled
// by now) and the loop body to run per emit chunk. The list-valued sibling of
// NLGroupAggregateLoopData.
class NLCollectLoopData : public NLFunctionData {
public:
    NLCollectLoopData(NLCollectState* state)
        : _state(state)
    {
    }

    NLCollectState* getState() const { return _state; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    NLCollectState* _state {nullptr};
    NLStmtContainer _stmts;
};

class NLCreateNodeData : public NLFunctionData {
public:
    struct Property {
        PropertyTypeID _propertyTypeID;
        const Column* _values {nullptr};
    };

    NLCreateNodeData(LabelSetHandle labelsetHandle, ColumnNodeIDs* result)
        : _result(result),
        _labelsetHandle(labelsetHandle)
    {
    }

    LabelSetHandle getLabelSetHandle() const { return _labelsetHandle; }
    ColumnNodeIDs* getResult() const { return _result; }
    const std::vector<Property>& properties() const { return _properties; }

    size_t getRowCount() const { return _cardinality ? _cardinality->size() : 1; }

    void addProperty(const Property& property) {
        _properties.push_back(property);
    }

    void setCardinality(const Column* cardinality) {
        _cardinality = cardinality;
    }

private:
    std::vector<Property> _properties;
    ColumnNodeIDs* _result {nullptr};
    const Column* _cardinality {nullptr};
    LabelSetHandle _labelsetHandle;
};

class NLCreateEdgeData : public NLFunctionData {
public:
    struct Property {
        PropertyTypeID _propertyTypeID;
        const Column* _values {nullptr};
    };

    NLCreateEdgeData(EdgeTypeID edgeTypeID,
                     const ColumnNodeIDs* src,
                     bool srcIsPending,
                     const ColumnNodeIDs* tgt,
                     bool tgtIsPending,
                     ColumnEdgeIDs* result)
        : _edgeTypeID(edgeTypeID),
        _src(src),
        _tgt(tgt),
        _result(result),
        _srcIsPending(srcIsPending),
        _tgtIsPending(tgtIsPending)
    {
    }

    EdgeTypeID getEdgeTypeID() const { return _edgeTypeID; }

    const ColumnNodeIDs* getSrc() const { return _src; }

    bool isSrcPending() const { return _srcIsPending; }

    const ColumnNodeIDs* getTgt() const { return _tgt; }

    bool isTgtPending() const { return _tgtIsPending; }

    // Row by row, which endpoints are pending, for a column a merge produced and so
    // mixed. Null for a column that is pending in every row or in none, which the two
    // flags above then answer for.
    const ColumnMask* getSrcPendingMask() const { return _srcPendingMask; }
    const ColumnMask* getTgtPendingMask() const { return _tgtPendingMask; }

    void setSrcPendingMask(const ColumnMask* mask) { _srcPendingMask = mask; }
    void setTgtPendingMask(const ColumnMask* mask) { _tgtPendingMask = mask; }

    ColumnEdgeIDs* getResult() const { return _result; }

    const std::vector<Property>& properties() const { return _properties; }

    void addProperty(const Property& property) {
        _properties.push_back(property);
    }

private:
    std::vector<Property> _properties;
    EdgeTypeID _edgeTypeID;
    const ColumnNodeIDs* _src {nullptr};
    const ColumnNodeIDs* _tgt {nullptr};
    const ColumnMask* _srcPendingMask {nullptr};
    const ColumnMask* _tgtPendingMask {nullptr};
    ColumnEdgeIDs* _result {nullptr};
    bool _srcIsPending {false};
    bool _tgtIsPending {false};
};

// Which way a merge pattern's hop points, from the chain node ahead of it to the one
// behind. The interpreter's copy of the storage-dialect EdgeDirection, set during
// translation so execution never reaches into MLIR.
enum class NLMergeDirection {
    Undirected = 0,
    Backward,
    Forward,
};

// A node or edge one row of a merge bound: an ID the graph holds, or - when pending -
// an offset into the change's write buffer for an entity this query wrote and has not
// committed. The two spaces overlap numerically, so the flag is what tells them apart.
struct NLMergeRef {
    uint64_t _id {0};
    bool _pending {false};

    bool operator==(const NLMergeRef& other) const {
        return _id == other._id && _pending == other._pending;
    }

    // The two spaces packed into one integer, so a ref can key a map or a set
    uint64_t asKey() const { return (_id << 1) | static_cast<uint64_t>(_pending); }
};

// One property constraint a merge pattern's node or hop puts on a row: the property it
// names and the column the row's asked-for value comes from, with the appender that
// turns that value into its part of the key the match looks up.
struct NLMergeProperty {
    PropertyType _propertyType;
    const Column* _values {nullptr};
    NLKeyAppendFunction _keyAppend {nullptr};
};

// The graph side of that key: the same property, read out of the graph into a scratch
// column. Its appender keys a fetched (and so nullable) value the way the row's own
// appender keys the asked-for one, which is what lets the two keys be compared.
struct NLMergeScanProperty {
    PropertyType _propertyType;
    Column* _values {nullptr};
    NLKeyAppendFunction _keyAppend {nullptr};
};

// The nodes the graph holds under one chain-node spec, indexed by the spec's property
// values. Scanned once, on first use, so a merge driven by many rows pays for one pass
// over its label set rather than a lookup per row.
class NLMergeNodeIndex {
public:
    NLMergeNodeIndex(const LabelSet& labels, bool matchable, ColumnNodeIDs* scanNodes);
    ~NLMergeNodeIndex();

    const LabelSet& getLabels() const { return _labels; }

    // False when a label or a key property of the spec is absent from the graph's
    // schema: no committed node can carry it, so the graph offers no candidate and only
    // the pending nodes this query wrote can match
    bool isMatchable() const { return _matchable; }

    bool isBuilt() const { return _built; }
    void markBuilt() { _built = true; }

    ColumnNodeIDs* getScanNodes() const { return _scanNodes; }

    const std::vector<NLMergeScanProperty>& scanProperties() const { return _scanProperties; }
    void addScanProperty(const NLMergeScanProperty& property) { _scanProperties.push_back(property); }

    void add(const std::string& key, const NLMergeRef& ref) { _byKey[key].push_back(ref); }

    std::span<const NLMergeRef> find(const std::string& key) const;

private:
    LabelSet _labels;
    std::vector<NLMergeScanProperty> _scanProperties;
    std::unordered_map<std::string, std::vector<NLMergeRef>> _byKey;
    ColumnNodeIDs* _scanNodes {nullptr};
    bool _matchable {false};
    bool _built {false};
};

// The nodes this query's merges wrote, by the spec that wrote them: a chain node's
// signature - its label set and key property types - followed by the row's key values.
// A pending node is in no graph the match reads, so this is where a later row, of this
// merge or of another one writing the same pattern, binds it rather than writing a
// second copy. One log per program, shared by every merge op in it.
class NLMergePendingNodes {
public:
    NLMergePendingNodes();
    ~NLMergePendingNodes();

    void add(const std::string& key, const NLMergeRef& ref) { _byKey[key].push_back(ref); }

    std::span<const NLMergeRef> find(const std::string& key) const;

private:
    std::unordered_map<std::string, std::vector<NLMergeRef>> _byKey;
};

// Every edge this query's merges wrote, under each of the two nodes it joins: a pending
// edge is in no graph the match reads, so this is where a later row's hop finds it. One
// log per program, shared by every merge op in it. The entries under one pair of
// endpoints come from every hop spec the query has, so each carries its hop's signature
// ahead of its property values - two hops constraining different properties to values
// with the same bytes would otherwise bind each other's edge.
class NLMergePendingEdges {
public:
    struct Entry {
        NLMergeRef _other;
        EdgeTypeID _edgeType;
        uint64_t _offset {0};
        std::string _propertyKey;
    };

    NLMergePendingEdges();
    ~NLMergePendingEdges();

    void add(const NLMergeRef& source,
             const NLMergeRef& target,
             EdgeTypeID edgeType,
             uint64_t offset,
             const std::string& propertyKey);

    std::span<const Entry> outOf(const NLMergeRef& node) const;
    std::span<const Entry> into(const NLMergeRef& node) const;

private:
    std::unordered_map<uint64_t, std::vector<Entry>> _outgoing;
    std::unordered_map<uint64_t, std::vector<Entry>> _incoming;

    static std::span<const Entry> lookup(const std::unordered_map<uint64_t, std::vector<Entry>>& edges,
                                         const NLMergeRef& node);
};

// nl.merge data: one entry per chain node and one per hop of the pattern, the columns
// each row's values and bound entities come from, and the chunks the op fills.
class NLMergeData : public NLFunctionData {
public:
    // One node of the chain. A bound node reads its rows from _boundColumn and is never
    // written; every other one is looked up through _index and, when the pattern is
    // missing, written under _labelSetHandle with _properties' values. Only a looked-up
    // node holds output chunks: a bound one's rows come back through the carry set.
    struct Node {
        std::vector<NLMergeProperty> _properties;

        // What the pending log's keys for this spec start with, so two specs writing
        // under the same labels and key properties share their entries and two that
        // do not never collide
        std::string _signature;

        const ColumnNodeIDs* _boundColumn {nullptr};
        const ColumnMask* _boundPending {nullptr};
        NLMergeNodeIndex* _index {nullptr};
        LabelSetHandle _labelSetHandle;
        ColumnNodeIDs* _output {nullptr};
        ColumnMask* _outputPending {nullptr};
    };

    // One hop of the chain, joining the node ahead of it to the one behind. The match
    // edge type is invalid when the graph's schema does not have it, which leaves only
    // the pending edges to match. The scratch chunks below are what a hop constraining
    // properties reads its candidates' values through.
    struct Hop {
        std::vector<NLMergeProperty> _properties;
        std::vector<NLMergeScanProperty> _scanProperties;

        // What the pending log's keys for this spec start with, the sibling of Node's:
        // the edge log is keyed by the endpoints alone, so without it two hops
        // constraining different properties to values with the same bytes would collide
        std::string _signature;

        EdgeTypeID _matchEdgeType;
        EdgeTypeID _writeEdgeType;
        NLMergeDirection _direction {NLMergeDirection::Forward};
        ColumnNodeIDs* _scanSources {nullptr};
        ColumnEdgeIDs* _scanEdges {nullptr};
        ColumnEdgeIDs* _output {nullptr};
        ColumnMask* _outputPending {nullptr};
    };

    NLMergeData(NLMergePendingNodes* pendingNodes,
                NLMergePendingEdges* pendingEdges,
                ColumnMask* created);
    ~NLMergeData() override;

    const std::vector<Node>& nodes() const { return _nodes; }
    const std::vector<Hop>& hops() const { return _hops; }
    const std::vector<NLCarriedColumn>& carriedColumns() const { return _carriedColumns; }

    std::vector<Node>& nodes() { return _nodes; }
    std::vector<Hop>& hops() { return _hops; }

    NLMergePendingNodes* getPendingNodes() const { return _pendingNodes; }
    NLMergePendingEdges* getPendingEdges() const { return _pendingEdges; }
    ColumnMask* getCreated() const { return _created; }

    // The column whose length is the number of rows the op runs over: a chunk some part
    // of the pattern reads. Null for a pattern reading none, which is the single row a
    // MERGE standing on its own writes.
    const Column* getRowCarrier() const { return _rowCarrier; }
    void setRowCarrier(const Column* rowCarrier) { _rowCarrier = rowCarrier; }

    void addNode(const Node& node) { _nodes.push_back(node); }
    void addHop(const Hop& hop) { _hops.push_back(hop); }
    void addCarriedColumn(const NLCarriedColumn& carried) { _carriedColumns.push_back(carried); }

    ColumnVector<size_t>* getIndices() { return &_indices; }

    // The scratch every step of the op works in, allocated once with the op rather than
    // once per chunk
    NLMergeWorkingSet* getWorkingSet() const { return _workingSet.get(); }

private:
    std::vector<Node> _nodes;
    std::vector<Hop> _hops;
    std::vector<NLCarriedColumn> _carriedColumns;
    NLMergePendingNodes* _pendingNodes {nullptr};
    NLMergePendingEdges* _pendingEdges {nullptr};
    ColumnMask* _created {nullptr};
    const Column* _rowCarrier {nullptr};
    std::unique_ptr<NLMergeWorkingSet> _workingSet;

    // This step's row map, from each emitted row to the input row behind it, fed to the
    // per-column gather that rebuilds the carry set
    ColumnVector<size_t> _indices;
};

class NLSetNodePropertyData : public NLFunctionData {
public:
    NLSetNodePropertyData(PropertyTypeID propertyTypeID,
                          const ColumnNodeIDs* input,
                          const Column* value)
        : _input(input),
        _value(value),
        _propertyTypeID(propertyTypeID)
    {
    }

    PropertyTypeID getPropertyTypeID() const { return _propertyTypeID; }
    const ColumnNodeIDs* getInput() const { return _input; }
    const Column* getValue() const { return _value; }

    // The rows whose node this change wrote and has not committed: those are updated in
    // the write buffer's pending node rather than recorded as an update to a committed
    // one. Null for an input no merge produced.
    const ColumnMask* getPending() const { return _pending; }
    void setPending(const ColumnMask* pending) { _pending = pending; }

    // The rows the write touches, or null for a write that touches every one: Cypher's
    // ON CREATE hands over the merge's created mask and ON MATCH its negation.
    const ColumnMask* getRows() const { return _rows; }
    void setRows(const ColumnMask* rows) { _rows = rows; }

private:
    const ColumnNodeIDs* _input {nullptr};
    const Column* _value {nullptr};
    const ColumnMask* _pending {nullptr};
    const ColumnMask* _rows {nullptr};
    PropertyTypeID _propertyTypeID;
};

class NLSetEdgePropertyData : public NLFunctionData {
public:
    NLSetEdgePropertyData(PropertyTypeID propertyTypeID,
                          const ColumnEdgeIDs* input,
                          const Column* value)
        : _input(input),
        _value(value),
        _propertyTypeID(propertyTypeID)
    {
    }

    PropertyTypeID getPropertyTypeID() const { return _propertyTypeID; }
    const ColumnEdgeIDs* getInput() const { return _input; }
    const Column* getValue() const { return _value; }

    const ColumnMask* getPending() const { return _pending; }
    void setPending(const ColumnMask* pending) { _pending = pending; }

    const ColumnMask* getRows() const { return _rows; }
    void setRows(const ColumnMask* rows) { _rows = rows; }

private:
    const ColumnEdgeIDs* _input {nullptr};
    const Column* _value {nullptr};
    const ColumnMask* _pending {nullptr};
    const ColumnMask* _rows {nullptr};
    PropertyTypeID _propertyTypeID;
};

class NLDeleteNodeData : public NLFunctionData {
public:
    NLDeleteNodeData(const ColumnNodeIDs* input, bool detaching, ColumnNodeIDs* committed)
        : _input(input),
        _committed(committed),
        _detaching(detaching)
    {
    }

    const ColumnNodeIDs* getInput() const { return _input; }
    bool isDetaching() const { return _detaching; }

    // The rows naming a node this change wrote and has not committed - a write-buffer
    // offset rather than an ID the graph holds - or null when no row does. A column a
    // create produced is such a row throughout, which is what _allPending says.
    const ColumnMask* getPending() const { return _pending; }
    void setPending(const ColumnMask* pending) { _pending = pending; }

    bool isAllPending() const { return _allPending; }
    void setAllPending(bool allPending) { _allPending = allPending; }

    // The scratch the committed rows of a mixed column are gathered into, since the
    // deletion of those goes through the graph's own IDs
    ColumnNodeIDs* getCommitted() const { return _committed; }

private:
    const ColumnNodeIDs* _input {nullptr};
    const ColumnMask* _pending {nullptr};
    ColumnNodeIDs* _committed {nullptr};
    bool _detaching {false};
    bool _allPending {false};
};

class NLDeleteEdgeData : public NLFunctionData {
public:
    NLDeleteEdgeData(const ColumnEdgeIDs* input, ColumnEdgeIDs* committed)
        : _input(input),
        _committed(committed)
    {
    }

    // The edge siblings of NLDeleteNodeData's
    const ColumnMask* getPending() const { return _pending; }
    void setPending(const ColumnMask* pending) { _pending = pending; }

    bool isAllPending() const { return _allPending; }
    void setAllPending(bool allPending) { _allPending = allPending; }

    ColumnEdgeIDs* getCommitted() const { return _committed; }

    const ColumnEdgeIDs* getInput() const { return _input; }

private:
    const ColumnEdgeIDs* _input {nullptr};
    const ColumnMask* _pending {nullptr};
    ColumnEdgeIDs* _committed {nullptr};
    bool _allPending {false};
};

// nl.output data
class NLOutputData : public NLFunctionData {
public:
    using OutputColumns = std::vector<const Column*>;

    const OutputColumns& outputs() const { return _columns; }

    // The columns a step's row count is read off. A constant column holds one value
    // standing for every row of the step, so it cannot say how many there are: only a
    // projection made of constants alone is sized by them.
    const OutputColumns& rowCountColumns() const { return _rowCountColumns.empty() ? _columns : _rowCountColumns; }

    void addOutputColumn(const Column* col, bool carriesRows) {
        _columns.push_back(col);

        if (carriesRows) {
            _rowCountColumns.push_back(col);
        }
    }

    // The governing limit counter, or null for a limit-oblivious output. When
    // set (the folded terminal-LIMIT form), output emits only its
    // getEmitThisStep() prefix; it never mutates the counter.
    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    // The governing skip counter, or null for a skip-oblivious output. When set
    // (the folded terminal-SKIP form), output emits the surviving suffix at
    // offset getSkipThisStep() for getEmitThisStep() rows; it never mutates the
    // counter. A folded output carries at most one of limit/skip.
    NLSkipState* getSkip() const { return _skip; }
    void setSkip(NLSkipState* skip) { _skip = skip; }

    const Column* getCardinality() const { return _cardinality; }

    void setCardinality(const Column* cardinality) { _cardinality = cardinality; }

private:
    std::vector<const Column*> _columns;
    std::vector<const Column*> _rowCountColumns;
    NLLimitState* _limit {nullptr};
    NLSkipState* _skip {nullptr};
    // Column which may define the cardinality of the output
    const Column* _cardinality {nullptr};
};

// Binary function to execute
using NLBinaryFn = void (*)(Column* result, const Column* lhs, const Column* rhs, LocalMemory* mem);

class NLBinaryData : public NLFunctionData {
public:
    NLBinaryData(const Column* lhs, const Column* rhs, Column* result, NLBinaryFn fn, LocalMemory* mem)
        : _lhs(lhs),
        _rhs(rhs),
        _result(result),
        _fn(fn),
        _mem(mem)
    {
    }

    const Column* getLhs() const { return _lhs; }
    const Column* getRhs() const { return _rhs; }
    Column* getResult() const { return _result; }
    NLBinaryFn getFn() const { return _fn; }
    LocalMemory* getMemory() const { return _mem; }

private:
    const Column* _lhs {nullptr};
    const Column* _rhs {nullptr};
    Column* _result {nullptr};
    NLBinaryFn _fn {nullptr};
    LocalMemory* _mem {nullptr};
};

// Unary function to execute
using NLUnaryFn = void (*)(Column* result, const Column* operand);

// Unary function to execute, plus operand and result
class NLUnaryData : public NLFunctionData {
public:
    NLUnaryData(const Column* operand, Column* result, NLUnaryFn fn)
        : _operand(operand),
        _result(result),
        _fn(fn)
    {
    }

    const Column* getOperand() const { return _operand; }
    Column* getResult() const { return _result; }
    NLUnaryFn getFn() const { return _fn; }

private:
    const Column* _operand {nullptr};
    Column* _result {nullptr};
    NLUnaryFn _fn {nullptr};
};

using NLUnaryFunctionKernel = void (*)(NLExecutionContext* context, Column* result, const Column* input);

class NLUnaryFunctionData : public NLFunctionData {
public:
    NLUnaryFunctionData(const Column* input, Column* result, NLUnaryFunctionKernel kernel)
        : _input(input),
        _result(result),
        _kernel(kernel)
    {
    }

    const Column* getInput() const { return _input; }
    Column* getResult() const { return _result; }
    NLUnaryFunctionKernel getKernel() const { return _kernel; }

private:
    const Column* _input {nullptr};
    Column* _result {nullptr};
    NLUnaryFunctionKernel _kernel {nullptr};
};

class NLProgram {
public:
    NLProgram();
    ~NLProgram();

    NLProgram(const NLProgram&) = delete;
    NLProgram& operator=(const NLProgram&) = delete;

    template <typename DataType, typename... Args>
    DataType* allocFunctionData(Args... args) {
        auto data = std::make_unique<DataType>(args...);
        DataType* dataPtr = data.get();
        _functionData.push_back(std::move(data));
        return dataPtr;
    }

    // Allocate one LIMIT's runtime counter, owned by the program; the
    // statements that reset, charge and read it hold a borrowed pointer.
    NLLimitState* allocLimitState() {
        auto state = std::make_unique<NLLimitState>();
        NLLimitState* statePtr = state.get();
        _limitStates.push_back(std::move(state));
        return statePtr;
    }

    // Allocate one SKIP's runtime counter, owned by the program; the statements
    // that reset, charge and read it hold a borrowed pointer. The skip sibling
    // of allocLimitState.
    NLSkipState* allocSkipState() {
        auto state = std::make_unique<NLSkipState>();
        NLSkipState* statePtr = state.get();
        _skipStates.push_back(std::move(state));
        return statePtr;
    }

    // Allocate one ORDER BY's runtime accumulator, owned by the program; the
    // collect, reset and emit statements that share it hold a borrowed pointer.
    NLSortState* allocSortState() {
        auto state = std::make_unique<NLSortState>();
        NLSortState* statePtr = state.get();
        _sortStates.push_back(std::move(state));
        return statePtr;
    }

    // Allocate one DISTINCT's runtime seen-set, owned by the program; the reset
    // and filter statements that share it hold a borrowed pointer. The distinct
    // sibling of allocSortState.
    NLDistinctState* allocDistinctState() {
        auto state = std::make_unique<NLDistinctState>();
        NLDistinctState* statePtr = state.get();
        _distinctStates.push_back(std::move(state));
        return statePtr;
    }

    // Allocate one COUNT's runtime tally, owned by the program; the reset, update
    // and emit statements that share it hold a borrowed pointer. The count sibling
    // of allocSortState.
    NLCountState* allocCountState() {
        auto state = std::make_unique<NLCountState>();
        NLCountState* statePtr = state.get();
        _countStates.push_back(std::move(state));
        return statePtr;
    }

    // Allocate one aggregate's runtime accumulator, owned by the program; the
    // reset, update and emit statements that share it hold a borrowed pointer. The
    // value sibling of allocCountState.
    NLAggregateState* allocAggregateState() {
        auto state = std::make_unique<NLAggregateState>();
        NLAggregateState* statePtr = state.get();
        _aggregateStates.push_back(std::move(state));
        return statePtr;
    }

    // Allocate one grouped aggregation's runtime accumulator, owned by the program;
    // the reset, update and emit statements that share it hold a borrowed pointer.
    // The grouped sibling of allocSortState.
    NLGroupAggregateState* allocGroupAggregateState() {
        auto state = std::make_unique<NLGroupAggregateState>();
        NLGroupAggregateState* statePtr = state.get();
        _groupAggregateStates.push_back(std::move(state));
        return statePtr;
    }

    // Allocate one collect's runtime accumulator, owned by the program; the reset,
    // update and drain statements that share it hold a borrowed pointer. The
    // list-valued sibling of allocGroupAggregateState.
    NLCollectState* allocCollectState() {
        auto state = std::make_unique<NLCollectState>();
        NLCollectState* statePtr = state.get();
        _collectStates.push_back(std::move(state));
        return statePtr;
    }

    // Allocate one CALL's runtime state, owned by the program; the statements that
    // prepare and drive the call hold a borrowed pointer. Releasing the
    // procedure's data is this state's business, so it outlives every statement
    // naming it.
    NLProcedureState* allocProcedureState(const Procedure* procedure,
                                          ProcedureData* data,
                                          const ProcedureContext* context) {
        auto state = std::make_unique<NLProcedureState>(procedure, data, context);
        NLProcedureState* statePtr = state.get();
        _procedureStates.push_back(std::move(state));
        return statePtr;
    }

    // The candidate index one chain-node signature already has, or a null pointer for a
    // signature no chain node of the program has reached yet. Two nodes of the same
    // labels and key properties look their candidates up in one index, so the label set
    // is scanned once however many times the query merges the pattern.
    NLMergeNodeIndex* findMergeNodeIndex(const std::string& signature) const;

    // Adds the index of a signature findMergeNodeIndex found none for, owned by the
    // program; every chain node of that signature holds a borrowed pointer.
    NLMergeNodeIndex* addMergeNodeIndex(const std::string& signature,
                                        const LabelSet& labels,
                                        bool matchable,
                                        ColumnNodeIDs* scanNodes);

    // The log of every edge this program's merges wrote, owned by the program and
    // shared by all of them: one merge's pending edge is what another's hop extends
    // a candidate with.
    NLMergePendingEdges* getMergePendingEdges() { return &_mergePendingEdges; }

    // The log of every node this program's merges wrote, the node sibling of the edge
    // log above and shared the same way.
    NLMergePendingNodes* getMergePendingNodes() { return &_mergePendingNodes; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

    size_t getChunkSize() const { return _chunkSize; }
    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }

    std::span<const std::string_view> columnNames() const { return _columnNames; }
    void setColumnNames(std::span<const std::string_view> names);

private:
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
    // The result column names nl.output carried, one per emitted column, or empty when it
    // named none. The views point into the MLIRContext's uniqued attribute storage, which
    // outlives the module the names were read from.
    std::vector<std::string_view> _columnNames;
    std::vector<std::unique_ptr<NLFunctionData>> _functionData;
    std::vector<std::unique_ptr<NLLimitState>> _limitStates;
    std::vector<std::unique_ptr<NLSkipState>> _skipStates;
    std::vector<std::unique_ptr<NLSortState>> _sortStates;
    std::vector<std::unique_ptr<NLDistinctState>> _distinctStates;
    std::vector<std::unique_ptr<NLCountState>> _countStates;
    std::vector<std::unique_ptr<NLAggregateState>> _aggregateStates;
    std::vector<std::unique_ptr<NLGroupAggregateState>> _groupAggregateStates;
    std::vector<std::unique_ptr<NLCollectState>> _collectStates;
    std::vector<std::unique_ptr<NLProcedureState>> _procedureStates;
    std::unordered_map<std::string, std::unique_ptr<NLMergeNodeIndex>> _mergeNodeIndexes;
    NLMergePendingEdges _mergePendingEdges;
    NLMergePendingNodes _mergePendingNodes;
    NLStmtContainer _stmts;
};

}
