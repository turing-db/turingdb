#pragma once

#include <stddef.h>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "GraphPath.h"
#include "ID.h"
#include "LocalMemory.h"
#include "NLStepBuffers.h"
#include "ProcedureContext.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListView.h"
#include "map/MapView.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"

#include "NLFunctionData.h"
#include "NLLimitState.h"
#include "NLColumnFunctions.h"
#include "NLProgramScans.h"
#include "NLProgramHops.h"
#include "NLProgramPaths.h"
#include "NLProgramSources.h"
#include "NLProgramProperties.h"
#include "NLProgramLists.h"
#include "NLProgramExpressions.h"
#include "NLProgramFunctions.h"
#include "NLProgramRows.h"
#include "NLProgramJoins.h"
#include "NLProgramAggregates.h"
#include "NLProgramSubqueries.h"
#include "NLProgramWrites.h"

#include "IRException.h"

namespace db {

class NLRuntimeProperty;
class Procedure;
class ProcedureData;
class LocalMemory;

Column* allocOptValueColumn(LocalMemory* memory, ValueType valueType, size_t reserveSize);

// Translation resolves every chunk SSA value
// to a concrete ColumnVector type through this kind
//
// The first three are the ID columns a scan or a hop binds, and the fourth the label sets
// read off a node column. The rest are what a CALL yields: a procedure's declared return
// types map onto these, so a yielded column can be crossed and carried like any other.
// Each names the element type of a plain (never-null) ColumnVector, and of the
// ColumnOptVector a nullable chunk of that element is.
enum class NLChunkKind {
    NodeID,
    EdgeID,
    EdgeTypeID,
    LabelSetID,
    LabelID,
    PropertyTypeID,
    ValueTypeCode,
    UInt64,
    Int64,
    Double,
    Bool,
    String,
    Embedding,
    List,
    Map,
    Path,
    DateTime,
    Duration,
    Point,
    PathRef,
};

// The column a chunk read as a truth value is held in, as its element type names it:
// booleans (!storage.bool or i1), nullable booleans (!storage.nullable<i1>), tagged cells
// (!storage.any), or the null literal
enum class NLTruthColumnKind {
    Booleans,
    NullableBooleans,
    TaggedCells,
    UntypedNull,
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

        case NLChunkKind::LabelSetID:
            return handler.template operator()<LabelSetID>();
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

        case NLChunkKind::Embedding:
            return handler.template operator()<types::Embedding::Primitive>();
        break;

        case NLChunkKind::List:
            return handler.template operator()<ListView>();
        break;

        case NLChunkKind::Map:
            return handler.template operator()<MapView>();
        break;

        case NLChunkKind::Path:
            return handler.template operator()<GraphPath>();
        break;

        case NLChunkKind::DateTime:
            return handler.template operator()<types::DateTime::Primitive>();
        break;

        case NLChunkKind::Duration:
            return handler.template operator()<types::Duration::Primitive>();
        break;

        case NLChunkKind::Point:
            return handler.template operator()<types::Point::Primitive>();
        break;

        case NLChunkKind::PathRef:
            return handler.template operator()<PathRef>();
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
}

// The sibling of dispatchChunkKind for a nullable chunk, whose column is a ColumnOptVector
// of the element handler is invoked with. A node, an edge or a path holds its own null, so
// no nullable chunk wraps one.
template <typename Handler>
void dispatchOptChunkKind(NLChunkKind kind, Handler&& handler) {
    switch (kind) {
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

        case NLChunkKind::Embedding:
            return handler.template operator()<types::Embedding::Primitive>();
        break;

        case NLChunkKind::List:
            return handler.template operator()<ListView>();
        break;

        case NLChunkKind::Map:
            return handler.template operator()<MapView>();
        break;

        case NLChunkKind::DateTime:
            return handler.template operator()<types::DateTime::Primitive>();
        break;

        case NLChunkKind::Duration:
            return handler.template operator()<types::Duration::Primitive>();
        break;

        case NLChunkKind::Point:
            return handler.template operator()<types::Point::Primitive>();
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("A node, an edge or a path column holds its own null, so no nullable chunk wraps one");
        break;

        case NLChunkKind::LabelSetID:
            throw IRException("A label set column is read as rows, so no nullable chunk wraps one");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
}

// The kind of the column a property of @param valueType is laid out in
NLChunkKind chunkKindFromValueType(ValueType valueType);

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

    // Allocate the buffers one op builds a run's values in, owned by the program
    NLStepBuffers* allocStepBuffers() {
        _stepBuffers.push_back(std::make_unique<NLStepBuffers>());
        return _stepBuffers.back().get();
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

    // Allocate one hash join's runtime build side, owned by the program; the reset,
    // collect and probe statements that share it hold a borrowed pointer. The joining
    // sibling of allocSortState.
    NLHashJoinState* allocHashJoinState() {
        auto state = std::make_unique<NLHashJoinState>();
        NLHashJoinState* statePtr = state.get();
        _hashJoinStates.push_back(std::move(state));
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

    // Allocate the tally of a DISTINCT aggregate or collect, owned by the program
    NLGroupDistinctTally* allocGroupDistinctTally() {
        _groupDistinctTallies.emplace_back(std::make_unique<NLGroupDistinctTally>());
        return _groupDistinctTallies.back().get();
    }

    NLNodeSetState* allocNodeSetState() {
        _nodeSetStates.emplace_back(std::make_unique<NLNodeSetState>());

        return _nodeSetStates.back().get();
    }

    template <SupportedType T>
    NLShortestPathState* allocShortestPathState(ValueType valueType, PropertyTypeID propertyType) {
        _shortestPathStates.emplace_back(
            std::make_unique<NLShortestPathStateImpl<T>>(valueType, propertyType));
        NLShortestPathState* ptr = _shortestPathStates.back().get();
        return ptr;
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

    // Allocate one OPTIONAL MATCH's runtime accumulator, owned by the program; the reset,
    // collect and drain statements that share it hold a borrowed pointer. The pattern
    // sibling of allocSortState.
    NLOptionalState* allocOptionalState() {
        auto state = std::make_unique<NLOptionalState>();
        NLOptionalState* statePtr = state.get();
        _optionalStates.push_back(std::move(state));
        return statePtr;
    }

    // Allocate one EXISTS's runtime accumulator, owned by the program; the reset, mark and
    // result statements that share it hold a borrowed pointer. The semi-join sibling of
    // allocOptionalState.
    NLExistsState* allocExistsState() {
        auto state = std::make_unique<NLExistsState>();
        NLExistsState* statePtr = state.get();
        _existsStates.push_back(std::move(state));
        return statePtr;
    }

    NLCountSubqueryState* allocCountSubqueryState() {
        auto state = std::make_unique<NLCountSubqueryState>();
        NLCountSubqueryState* statePtr = state.get();
        _countSubqueryStates.push_back(std::move(state));
        return statePtr;
    }

    // Allocate one row lists accumulator, owned by the program; the
    // reset, collect and build statements that share it hold a borrowed pointer.
    NLRowListsState* allocRowListsState() {
        auto state = std::make_unique<NLRowListsState>();
        NLRowListsState* statePtr = state.get();
        _rowListsStates.push_back(std::move(state));
        return statePtr;
    }

    NLRowState* allocRowState() {
        auto state = std::make_unique<NLRowState>();
        NLRowState* statePtr = state.get();
        _rowStates.push_back(std::move(state));
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
                                        const LabelSet& writeLabels,
                                        bool matchable,
                                        ColumnNodeIDs* scanNodes);

    // The log of every edge this program wrote, owned by the program and shared by all
    // of its merges: a pending edge is what a merge's hop extends a candidate with.
    NLMergePendingEdges* getMergePendingEdges() { return &_mergePendingEdges; }

    // The property every write of @param name the program types as it runs reaches, owned
    // by the program
    NLRuntimeProperty* getRuntimeProperty(std::string_view name);

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

    size_t getChunkSize() const { return _chunkSize; }
    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }

    std::span<const std::string_view> columnNames() const { return _columnNames; }
    void setColumnNames(std::span<const std::string_view> names);

    // The output whose columns the result schema is declared from. A union emits one
    // output per branch, all naming the same columns of the same types, so the first is
    // the whole result's schema.
    NLOutputData* getOutputData() const { return _outputData; }
    void setOutputData(NLOutputData* outputData);

private:
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
    // The result column names nl.output carried, one per emitted column, or empty when it
    // named none. The views point into the MLIRContext's uniqued attribute storage, which
    // outlives the module the names were read from.
    std::vector<std::string_view> _columnNames;
    // The first output statement's payload, owned by _functionData; null for a program
    // that emits nothing.
    NLOutputData* _outputData {nullptr};
    std::vector<std::unique_ptr<NLFunctionData>> _functionData;
    std::vector<std::unique_ptr<NLStepBuffers>> _stepBuffers;
    std::vector<std::unique_ptr<NLLimitState>> _limitStates;
    std::vector<std::unique_ptr<NLSkipState>> _skipStates;
    std::vector<std::unique_ptr<NLSortState>> _sortStates;
    std::vector<std::unique_ptr<NLHashJoinState>> _hashJoinStates;
    std::vector<std::unique_ptr<NLDistinctState>> _distinctStates;
    std::vector<std::unique_ptr<NLCountState>> _countStates;
    std::vector<std::unique_ptr<NLAggregateState>> _aggregateStates;
    std::vector<std::unique_ptr<NLGroupAggregateState>> _groupAggregateStates;
    std::vector<std::unique_ptr<NLCollectState>> _collectStates;
    std::vector<std::unique_ptr<NLGroupDistinctTally>> _groupDistinctTallies;
    std::vector<std::unique_ptr<NLNodeSetState>> _nodeSetStates;
    std::vector<std::unique_ptr<NLShortestPathState>> _shortestPathStates;
    std::vector<std::unique_ptr<NLOptionalState>> _optionalStates;
    std::vector<std::unique_ptr<NLExistsState>> _existsStates;
    std::vector<std::unique_ptr<NLCountSubqueryState>> _countSubqueryStates;
    std::vector<std::unique_ptr<NLRowListsState>> _rowListsStates;
    std::vector<std::unique_ptr<NLRowState>> _rowStates;
    std::vector<std::unique_ptr<NLProcedureState>> _procedureStates;
    std::unordered_map<std::string, std::unique_ptr<NLMergeNodeIndex>> _mergeNodeIndexes;
    std::unordered_map<std::string, std::unique_ptr<NLRuntimeProperty>> _runtimeProperties;
    NLMergePendingEdges _mergePendingEdges;
    NLStmtContainer _stmts;
};

}
