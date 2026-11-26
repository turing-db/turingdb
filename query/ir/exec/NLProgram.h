#pragma once

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "GraphPath.h"
#include "ID.h"
#include "LocalMemory.h"
#include "NLGroupHashTable.h"
#include "NLHashJoinIndex.h"
#include "NLStepBuffers.h"
#include "NLValueStore.h"
#include "ProcedureContext.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "map/MapView.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitWriteBuffer.h"

#include "NLWriteProperties.h"
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

#include "IRException.h"
#include "StringHashMap.h"

namespace db {

class NLRuntimeProperty;
struct NLMergeWorkingSet;
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

// nl.cross_product loop data: the outer and inner columns of a cartesian product,
// and the body run once per chunk of the pairs they make. N (outer rows) and M
// (inner rows) are read at run time from the first column of each group, so the
// product has N*M pairs; the loop walks them chunk by chunk rather than laying all
// of them out at once.
class NLCrossProductLoopData : public NLFunctionData {
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

    // The governing limit counter, or null for an unbounded loop. The loop stops
    // once it reaches zero and a step lays out at most that many pairs; it never
    // mutates the counter (the nl.limit_update in the body does).
    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    Columns _outerColumns;
    Columns _innerColumns;
    NLLimitState* _limit {nullptr};
    NLStmtContainer _stmts;
};

// Type of handle that answers whether one row of a join key column can match at all: whether
// `=` holds the key equal to itself, which a null, a NaN and a list or map holding a null
// at any depth do not.
using NLKeyIsMatchableFunction = bool (*)(const Column* column, size_t row);

// Type of handle that hashes every row of a join key column, one hash per row, for the
// build index to chain rows under and the probe to look them up by. The key is read where
// it lives and never copied; a row the matchable gate rejects hashes to anything, since
// no lookup ever reaches it.
using NLKeyHashFunction = void (*)(const Column* column, std::vector<uint64_t>& hashes);

// Type of handle that answers whether a probe row's key equals a build row's, both read in
// place from their columns.
using NLKeyEqualFunction = bool (*)(const Column* probe, size_t probeRow, const Column* build, size_t buildRow);

// The three handlers a join reads a key column through, selected together from its chunk
// type. The equality compares two columns of that type.
struct NLJoinKeyFunctions {
    NLKeyHashFunction _hash {nullptr};
    NLKeyEqualFunction _equal {nullptr};
    NLKeyIsMatchableFunction _isMatchable {nullptr};
};

// Runtime state of one hash join's build side: the build rows, materialized column by
// column, and the index chaining each row under its key's hash. nl.hash_join_buffer
// empties it, nl.hash_join_collect grows it once per build-loop step, and
// nl.hash_join_probe reads it once per probe-loop step. The joining sibling of
// NLSortState: it indexes the rows it accumulates rather than ordering them, and they
// come back out through the probe rather than through a loop of its own.
class NLHashJoinState {
public:
    // Empty the buffers and the index, so the build side starts fresh. Runs each time
    // nl.hash_join_buffer's block runs - once at function scope for a top-level join.
    void reset();

    // Record one build column's buffer. Kept in collect order, so the key column index
    // the nl.hash_join_buffer carries indexes this list too.
    void addColumnBuffer(Column* buffer) { _buffers.push_back(buffer); }

    const std::vector<Column*>& buffers() const { return _buffers; }
    Column* buffer(size_t index) const { return _buffers[index]; }

    NLHashJoinIndex& getIndex() { return _index; }
    const NLHashJoinIndex& getIndex() const { return _index; }

    // The values the buffers hold, copied in as NLSortState::values' are
    NLValueStore& values() { return _values; }

private:
    // One buffer per collected build column, row-aligned, grown by nl.hash_join_collect.
    std::vector<Column*> _buffers;

    NLHashJoinIndex _index;
    NLValueStore _values;
};

// nl.hash_join_buffer data: empties a build side each time the block it lives in runs -
// once at function scope for a top-level join, once per enclosing step for a nested one.
class NLHashJoinResetData : public NLFunctionData {
public:
    NLHashJoinResetData(NLHashJoinState* state)
        : _state(state)
    {
    }

    NLHashJoinState* getState() const { return _state; }

private:
    NLHashJoinState* _state {nullptr};
};

// nl.hash_join_collect data: appends the current chunk of every build column to its
// buffer and indexes each row's key. The appends share NLSortCollectData::Append's shape;
// the key is one of those columns, read again through the serializer and the null test.
class NLHashJoinCollectData : public NLFunctionData {
public:
    NLHashJoinCollectData(NLHashJoinState* state)
        : _state(state)
    {
    }

    NLHashJoinState* getState() const { return _state; }

    const std::vector<NLSortCollectData::Append>& appends() const { return _appends; }

    void addAppend(const NLSortCollectData::Append& append) {
        _appends.push_back(append);
    }

    // The collected key chunk and the buffer it is appended to, whose rows a new row's key
    // is compared against to find the group already holding it.
    void setKeyColumns(const Column* key,
                       const Column* buildKey,
                       const NLJoinKeyFunctions& keyFunctions) {
        _key = key;
        _buildKey = buildKey;
        _keyFunctions = keyFunctions;
    }

    const Column* getKeyColumn() const { return _key; }
    const Column* getBuildKeyColumn() const { return _buildKey; }
    NLKeyHashFunction getKeyHash() const { return _keyFunctions._hash; }
    NLKeyEqualFunction getKeyEqual() const { return _keyFunctions._equal; }
    NLKeyIsMatchableFunction getKeyIsMatchable() const { return _keyFunctions._isMatchable; }

    std::vector<uint64_t>* getHashScratch() { return &_hashScratch; }

private:
    NLHashJoinState* _state {nullptr};
    std::vector<NLSortCollectData::Append> _appends;

    const Column* _key {nullptr};
    const Column* _buildKey {nullptr};
    NLJoinKeyFunctions _keyFunctions;

    // This step's key hashes, one per row of the collected chunk
    std::vector<uint64_t> _hashScratch;
};

// nl.hash_join_probe data: per probe column its input chunk, the fresh output chunk and
// the gather that repeats a row once per match; per build column its buffer, the fresh
// output chunk and the gather that reads the matched rows back - both in the
// NLCarriedColumn (input, output, gather) shape the edge and sort loops use. The two
// index scratches hold this step's matched pairs, one row of each side per output row.
class NLHashJoinProbeLoopData : public NLFunctionData {
public:
    NLHashJoinProbeLoopData(NLHashJoinState* state)
        : _state(state)
    {
    }

    NLHashJoinState* getState() const { return _state; }

    const std::vector<NLCarriedColumn>& probeColumns() const { return _probeColumns; }
    const std::vector<NLCarriedColumn>& buildColumns() const { return _buildColumns; }

    void addProbeColumn(const NLCarriedColumn& column) { _probeColumns.push_back(column); }
    void addBuildColumn(const NLCarriedColumn& column) { _buildColumns.push_back(column); }

    // The governing limit counter, or null for an unbounded probe. When set, only
    // getRemaining() matched pairs are laid out; it never mutates the counter (the
    // following nl.limit_update does).
    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    // The probe key chunk and the build key buffer it is matched against: the hash is the
    // probe chunk type's, the equality compares that type to the build key's.
    void setKeyColumns(const Column* key,
                       const Column* buildKey,
                       const NLJoinKeyFunctions& keyFunctions) {
        _key = key;
        _buildKey = buildKey;
        _keyFunctions = keyFunctions;
    }

    const Column* getKeyColumn() const { return _key; }
    const Column* getBuildKeyColumn() const { return _buildKey; }
    NLKeyHashFunction getKeyHash() const { return _keyFunctions._hash; }
    NLKeyEqualFunction getKeyEqual() const { return _keyFunctions._equal; }
    NLKeyIsMatchableFunction getKeyIsMatchable() const { return _keyFunctions._isMatchable; }

    std::vector<uint64_t>* getHashScratch() { return &_hashScratch; }

    ColumnVector<size_t>* getProbeIndices() { return &_probeIndices; }
    ColumnVector<size_t>* getBuildIndices() { return &_buildIndices; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    NLHashJoinState* _state {nullptr};
    std::vector<NLCarriedColumn> _probeColumns;
    std::vector<NLCarriedColumn> _buildColumns;

    const Column* _key {nullptr};
    const Column* _buildKey {nullptr};
    NLJoinKeyFunctions _keyFunctions;
    NLLimitState* _limit {nullptr};

    // This step's key hashes, one per row of the probe chunk
    std::vector<uint64_t> _hashScratch;

    // This step's matched pairs: output row i reads _probeIndices[i] of the probe chunk
    // and _buildIndices[i] of the buffers.
    ColumnVector<size_t> _probeIndices;
    ColumnVector<size_t> _buildIndices;

    NLStmtContainer _stmts;
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

// nl.count_scan_rows data: the whole of a COUNT that reads the graph's node counts
// instead of walking rows. Holds one label set per counted scan and the output column
// to fill with the single product row - the same unsigned i64 count column
// NLCountResultData fills. The label sets are owned here so the LabelSetHandle the
// executor builds each run points at storage that outlives it (this data lives for the
// whole program); the translator resolves them in place through addLabelSet after
// allocating the data. An invalid property type ID is a tally over the scanned nodes
// themselves rather than over the ones holding a property; a valid one narrows the scan
// _propertyScan indexes and leaves the others whole. _matchable is false when a requested
// label or the property was absent from the schema, leaving the conjunction unsatisfiable,
// so the product counts 0.
class NLCountScanRowsData : public NLFunctionData {
public:
    NLCountScanRowsData(Column* output)
        : _output(output)
    {
    }

    Column* getOutput() const { return _output; }
    std::span<const LabelSet> getLabelSets() const { return _labelSets; }
    PropertyTypeID getPropertyTypeID() const { return _propertyTypeID; }
    size_t getPropertyScan() const { return _propertyScan; }
    bool isMatchable() const { return _matchable; }

    void reserveLabelSets(size_t count) { _labelSets.reserve(count); }
    void addLabelSet(const LabelSet& labelset) { _labelSets.push_back(labelset); }
    void setProperty(PropertyTypeID propertyTypeID, size_t propertyScan) {
        _propertyTypeID = propertyTypeID;
        _propertyScan = propertyScan;
    }
    void markUnmatchable() { _matchable = false; }

private:
    Column* _output {nullptr};
    std::vector<LabelSet> _labelSets;
    PropertyTypeID _propertyTypeID;
    size_t _propertyScan {0};
    bool _matchable {true};
};

// The reduction one nl.aggregate applies. The runtime counterpart of the MLIR
// storage::AggregateKind: the translator maps the op's kind onto this, and it -
// with the column value type - selects the state and the fold of the aggregate.
enum class AggregateKind {
    Sum,
    Min,
    Max,
    Avg,
    StDev,
    StDevP,
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
    StDev,
    StDevP,
    StDevDistinct,
    StDevPDistinct,
    First,
};

// The (group, value) pairs a DISTINCT aggregate has charged. One group hash table numbers the
// values, keyed by their type, and a second keys the pairs of a row's group and its value's
// number. A row whose pair is new is the first of its group to carry its value, so the fold
// charges that row alone. The grouped sibling of NLDistinctState.
class NLGroupDistinctTally {
public:
    NLGroupDistinctTally();
    ~NLGroupDistinctTally();

    void setValueKey(const NLGroupHashKey& valueKey);
    NLGroupHashTableKind getValueKind() const { return _values.getKind(); }

    void assign(const Column* rowGroups, const Column* values);

    // Whether this row of the last step was the first to carry its pair
    bool isNew(size_t row) const { return _newRows[row] != 0; }

    void clear();

private:
    NLGroupHashTable _values;
    NLGroupHashTable _pairs;
    std::vector<uint8_t> _newRows;
};

// Append the values of the given rows of an input chunk onto the tail of a
// growing key buffer of the same element type. nl.group_aggregate_update calls
// this with the rows that created a new group this step, so the buffer holds one
// key value per group in group-creation order. One per column kind / value type,
// selected during translation the way the gather and append families are.
using NLGroupKeyGatherFunction = void (*)(const Column* input,
                                          const std::vector<size_t>& rows,
                                          Column* buffer,
                                          NLValueStore& values);

// Where one aggregate's state sits in the block of every group: group g's at getPlace(g).
// ClickHouse's places, each with the aggregate's offset already added.
class NLGroupStatePlaces {
public:
    NLGroupStatePlaces(std::byte* first, size_t stride)
        : _first(first),
        _stride(stride)
    {
    }

    std::byte* getPlace(size_t group) const { return _first + group * _stride; }

    template <typename State>
    State& getState(size_t group) const { return *std::launder(reinterpret_cast<State*>(getPlace(group))); }

private:
    std::byte* _first {nullptr};
    size_t _stride {0};
};

// Construct the identity of one aggregate - a zero tally, sum or spread, no value kept yet - in
// the blocks of groups [begin, begin + count), the groups this step created. One per state type.
using NLGroupAggregateInitFunction = void (*)(NLGroupStatePlaces places, size_t begin, size_t count);

// Fold rows [beginRow, endRow) of this step's chunk into one aggregate's states: each row into
// the state of the group groups[row] names, groups being the row -> group-index map
// nl.group_aggregate_update built for this step. A DISTINCT kind charges a group only for a
// row its tally marks new, and min, max and first copy the value they keep into values.
using NLGroupAggregateFoldFunction = void (*)(NLGroupStatePlaces places,
                                              const Column* input,
                                              const std::vector<size_t>& groups,
                                              size_t beginRow,
                                              size_t endRow,
                                              const NLGroupDistinctTally* distinct,
                                              NLValueStore& values);

// Finalize the states of groups [begin, begin + count) into an emit output column, one row per
// group: the tally, the sum, the value kept, or the average or spread computed from the state.
// A value it computes - the sum or average of tagged cells - is built in the step buffers. Runs
// once per emit chunk. One per state type.
using NLGroupAggregateEmitFunction = void (*)(NLGroupStatePlaces places,
                                              size_t begin,
                                              size_t count,
                                              Column* output,
                                              NLStepBuffers* stepBuffers);

// Copy the views the states of groups [0, groupCount) hold into @param values, so the store
// they were copied into before can be emptied
using NLGroupAggregateReownFunction = void (*)(NLGroupStatePlaces places, size_t groupCount, NLValueStore& values);

// The state one aggregate keeps per group: the size and alignment it takes in a group's block,
// and the handlers that construct, finalize and re-own it. reown is null for a state holding no
// view. Selected from the kind and the value type, beside the fold that updates the state.
struct NLGroupAggregateStateType {
    size_t _size {0};
    size_t _alignment {1};
    NLGroupAggregateInitFunction _init {nullptr};
    NLGroupAggregateEmitFunction _emit {nullptr};
    NLGroupAggregateReownFunction _reown {nullptr};
};

// The aggregates one grouping reduces and their states, laid out as ClickHouse's Aggregator
// lays them out: a block per group holding every aggregate's state at that aggregate's offset.
// The blocks sit in one buffer at a fixed stride, since a group is a dense index, so the buffer
// moves them as it grows and every state is trivially copyable.
class NLGroupAggregates {
public:
    // One aggregate. The update folds the incoming chunk _input into the state at _stateOffset
    // in each group's block, and the emit finalizes the states into the loop variable _output.
    // _distinct is the tally of a DISTINCT aggregate, null for every other reduction.
    struct Aggregate {
        const Column* _input {nullptr};
        Column* _output {nullptr};
        NLGroupAggregateStateType _stateType;
        size_t _stateOffset {0};
        NLGroupAggregateFoldFunction _fold {nullptr};
        NLGroupDistinctTally* _distinct {nullptr};
    };

    // Place the aggregate's state in the block of every group, after the states placed before
    void add(const Aggregate& aggregate);

    // Mutable so the emit-loop translation can fill in each aggregate's _output after the
    // update translation set up the rest
    std::span<Aggregate> getAggregates() { return _aggregates; }

    // Give every group up to groupCount a block, its states the identity of their aggregate
    void grow(size_t groupCount);

    // Fold this step's chunk into the states: row r into the block of group rowGroups[r]
    void fold(const ColumnVector<size_t>* rowGroups);

    // Finalize the states of groups [begin, begin + count) into each aggregate's output
    void emit(size_t begin, size_t count, NLStepBuffers* stepBuffers);

    // The values min, max and first keep
    NLValueStore& getValues() { return _values.getActive(); }
    void compactValues();

    // Drop every group's block and every tally, so the aggregation starts fresh
    void clear();

private:
    std::vector<Aggregate> _aggregates;
    std::vector<std::byte> _blocks;
    size_t _statesEnd {0};
    size_t _alignment {1};
    size_t _stride {0};
    size_t _groupCount {0};
    NLCompactingValueStore _values;

    NLGroupStatePlaces getPlaces(const Aggregate& aggregate);
};

// Fold the present values of an input chunk into the state of a keyless aggregate: the state its
// kind keeps for a group of a grouped aggregation, folded through locals as ClickHouse's
// addBatchSinglePlace folds one. A min or a max copies the value it ends the chunk on into the
// spare store of values, then flips the stores.
using NLAggregateFoldFunction = void (*)(std::byte* place, const Column* input, NLCompactingValueStore& values);

// Runtime state of one keyless SUM/MIN/MAX/AVG/STDEV: the state its kind keeps for a group of a
// grouped aggregation, kept for the one group every row falls in - ClickHouse's aggregation
// without a key. nl.aggregate resets it, nl.aggregate_update is the sole folder, and
// nl.aggregate_result finalizes it into the single result row. The value sibling of
// NLCountState.
class NLAggregateState {
public:
    NLAggregateState();
    ~NLAggregateState();

    void setStateType(const NLGroupAggregateStateType& stateType);

    std::byte* getPlace() { return _place.data(); }
    NLCompactingValueStore& getValues() { return _values; }

    void reset();

    // Finalize the state into the single row of output
    void emit(Column* output, NLStepBuffers* stepBuffers);

private:
    NLGroupAggregateStateType _stateType;
    std::vector<std::byte> _place;
    NLCompactingValueStore _values;
};

// nl.aggregate data: re-initializes the state of a keyless aggregate each time the block it
// lives in runs - once at function scope for a top-level aggregate. The value sibling of
// NLCountResetData.
class NLAggregateResetData : public NLFunctionData {
public:
    NLAggregateResetData(NLAggregateState* state)
        : _state(state)
    {
    }

    NLAggregateState* getState() const { return _state; }

private:
    NLAggregateState* _state {nullptr};
};

// nl.aggregate_update data: the state to fold into, the input chunk to reduce, and the fold
// (selected from the kind and the input value type). Runs once per producing-loop step. The
// value sibling of NLCountUpdateData.
class NLAggregateUpdateData : public NLFunctionData {
public:
    NLAggregateUpdateData(NLAggregateState* state, const Column* input, NLAggregateFoldFunction fold)
        : _state(state),
        _input(input),
        _fold(fold)
    {
    }

    NLAggregateState* getState() const { return _state; }
    const Column* getInput() const { return _input; }
    NLAggregateFoldFunction getFold() const { return _fold; }

private:
    NLAggregateState* _state {nullptr};
    const Column* _input {nullptr};
    NLAggregateFoldFunction _fold {nullptr};
};

// nl.aggregate_result data: the emit step of a keyless aggregate. Holds the state, the output
// column to fill with the single result row, and the step buffers a value it computes - the sum
// of tagged cells - is built in. Runs once, after the producing loop, so the state is final. The
// value sibling of NLCountResultData.
class NLAggregateResultData : public NLFunctionData {
public:
    NLAggregateResultData(NLAggregateState* state, Column* output, NLStepBuffers* stepBuffers)
        : _state(state),
        _output(output),
        _stepBuffers(stepBuffers)
    {
    }

    NLAggregateState* getState() const { return _state; }
    Column* getOutput() const { return _output; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

private:
    NLAggregateState* _state {nullptr};
    Column* _output {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
};

// The grouping keys of a grouped aggregation or collect: the hash table mapping the keys of a row
// to its group, and the key values of every group, kept in the order the groups appeared
class NLGroupTable {
public:
    // The values of one grouping key the groups keep. They accumulate in _buffer as groups appear,
    // copied from the chunk _input by _gatherAppend, and a drain fills the loop variable _output,
    // sliced by _emitCopy or repeated per row by _gather.
    struct KeyColumn {
        const Column* _input {nullptr};
        Column* _buffer {nullptr};
        Column* _output {nullptr};
        NLGroupKeyGatherFunction _gatherAppend {nullptr};
        NLCopyFunction _emitCopy {nullptr};
        NLGatherFunction _gather {nullptr};
    };

    NLGroupTable();
    ~NLGroupTable();

    void addKeyColumn(const NLGroupHashKey& hashKey, const KeyColumn& key);

    // Mutable so the drain translation can fill in each column's _output after the update
    // translation set up the rest
    std::vector<KeyColumn>& keyColumns() { return _keyColumns; }

    NLGroupHashTable& hashTable() { return _hashTable; }

    // Map each of the rowCount rows of the key chunks to its group, keeping the key values of the
    // groups it creates
    void assign(size_t rowCount);

    const ColumnVector<size_t>* getRowGroupColumn() const { return _hashTable.getRowGroupColumn(); }
    const std::vector<size_t>& getRowGroups() const { return _hashTable.getRowGroups(); }
    size_t getGroupCount() const { return _hashTable.getGroupCount(); }

    // Slice the key values of groups [begin, begin + count) into the outputs
    void emitKeys(size_t begin, size_t count);

    // Fill the outputs with the key values of the group each row of groups names
    void gatherKeys(const ColumnVector<size_t>* groups);

    void clear();

private:
    NLGroupHashTable _hashTable;
    std::vector<KeyColumn> _keyColumns;
    std::vector<const Column*> _keyInputs;
    NLValueStore _keyValues;
};

// Runtime state of one grouped aggregation: the group table mapping each row's grouping keys to
// its group, and the aggregates with their state per group. nl.group_aggregate_buffer resets it,
// nl.group_aggregate_update assigns each row to its group and folds the aggregate inputs, and the
// nl.for over nl.group_aggregate emits one row per group. The grouped sibling of NLSortState: it
// keys rows by the grouping tuple and reduces within each group rather than buffering every row
// for a global reorder.
//
// The states are kept one block per group so a future two-phase merge can
// combine two tables group by group.
class NLGroupAggregateState {
public:
    NLGroupTable& groupTable() { return _groupTable; }
    NLGroupAggregates& aggregates() { return _aggregates; }

    // Empty the group table, key buffers, states and tallies, so the
    // aggregation starts fresh. Runs each time nl.group_aggregate_buffer's block
    // runs (once at function scope for a top-level grouped RETURN).
    void reset();

private:
    NLGroupTable _groupTable;
    NLGroupAggregates _aggregates;
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
    NLGroupAggregateLoopData(NLGroupAggregateState* state, NLStepBuffers* stepBuffers)
        : _state(state),
        _stepBuffers(stepBuffers)
    {
    }

    NLGroupAggregateState* getState() const { return _state; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    NLGroupAggregateState* _state {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
    NLLimitState* _limit {nullptr};
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
                                       const NLGroupDistinctTally* distinct,
                                       NLValueStore& store);

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

// Runtime state of one collect: the group table mapping each row's grouping keys to its
// group, and - the accumulator - a single flat buffer of collected values plus, per group,
// the positions of its elements in that buffer. nl.collect_buffer resets it, nl.collect_update assigns each
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
    // One collected column - Cypher's collect(x). _distinct holds the (group, value) pairs
    // already charged, so collect(DISTINCT x) takes each value once, and is null for a plain
    // collect; _output is filled either as the per-group list cell or as the unwound value,
    // which is why the drain carries an emit handler for each.
    struct ValueColumn {
        const Column* _input {nullptr};
        Column* _buffer {nullptr};
        Column* _output {nullptr};
        NLCollectFoldFunction _fold {nullptr};
        NLCollectListEmitFunction _listEmit {nullptr};
        NLUnwindCollectValueEmitFunction _unwindCollectEmit {nullptr};
        NLGroupDistinctTally* _distinct {nullptr};
        std::vector<std::vector<size_t>> _groupPositions;
    };

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
    NLGroupAggregates& aggregates() { return _aggregates; }

    // The query-scoped list buffer the nl.collect drain materializes per-group runs
    // into; the ListViews it hands out span it and stay valid for the whole run. Every
    // collected column shares it, since each run is contiguous per insert.
    ListBuffer<>& listBuffer() { return _listBuffer; }

    // The collected values the buffers hold, copied in as NLSortState::values' are
    NLValueStore& collectedValues() { return _collectedValues; }

    // Empty the group table, value buffers and per-group positions, so the collect starts
    // fresh. Runs each time nl.collect_buffer's block runs. An ungrouped collect comes back
    // with its single group, which the group table holds before any row arrives.
    void reset();

private:
    NLGroupTable _groupTable;

    std::vector<ValueColumn> _valueColumns;

    ListBuffer<> _listBuffer;
    NLValueStore _collectedValues;

    NLGroupAggregates _aggregates;
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
    NLCollectLoopData(NLCollectState* state, NLStepBuffers* stepBuffers)
        : _state(state),
        _stepBuffers(stepBuffers)
    {
    }

    NLCollectState* getState() const { return _state; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    NLCollectState* _state {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
    NLStmtContainer _stmts;
};

// One property a CREATE writes and the column its values come from. A column of tagged
// cells carries the runtime property that types it as the write runs in place of a type ID.
struct NLCreatedProperty {
    PropertyTypeID _propertyTypeID;
    const Column* _values {nullptr};
    NLRuntimeProperty* _runtimeProperty {nullptr};
};

class NLCreateNodeData : public NLFunctionData {
public:
    using Property = NLCreatedProperty;

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
    using Property = NLCreatedProperty;

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
    const ColumnVector<CustomBool>* getSrcPendingMask() const { return _srcPendingMask; }
    const ColumnVector<CustomBool>* getTgtPendingMask() const { return _tgtPendingMask; }

    void setSrcPendingMask(const ColumnVector<CustomBool>* mask) { _srcPendingMask = mask; }
    void setTgtPendingMask(const ColumnVector<CustomBool>* mask) { _tgtPendingMask = mask; }

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
    const ColumnVector<CustomBool>* _srcPendingMask {nullptr};
    const ColumnVector<CustomBool>* _tgtPendingMask {nullptr};
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

    // Set for a column of tagged cells, whose rows are keyed as the values they are staged
    // as. The type stays unset until the merge's first chunk holding a value types it.
    NLRuntimeProperty* _runtimeProperty {nullptr};
};

// The graph side of that key: the same property, read out of the graph into a scratch
// column. Its appender keys a fetched (and so nullable) value the way the row's own
// appender keys the asked-for one, which is what lets the two keys be compared.
struct NLMergeScanProperty {
    PropertyType _propertyType;
    Column* _values {nullptr};
    NLKeyAppendFunction _keyAppend {nullptr};
};

using NLMergeScanProperties = std::vector<NLMergeScanProperty>;

// The nodes the graph holds under one chain-node spec, indexed by the spec's property
// values. Scanned once, on first use, so a merge driven by many rows pays for one pass
// over its label set rather than a lookup per row. The nodes this query writes, whichever
// clause writes them, are taken in as they are written, keyed by the same values read
// back from the write buffer.
class NLMergeNodeIndex {
public:
    NLMergeNodeIndex(const LabelSet& labels,
                     const LabelSet& writeLabels,
                     bool matchable,
                     ColumnNodeIDs* scanNodes);
    ~NLMergeNodeIndex();

    const LabelSet& getLabels() const { return _labels; }
    const LabelSet& getWriteLabels() const { return _writeLabels; }

    // False when a label or a key property of the spec is absent from the graph's
    // schema: no committed node can carry it, so the graph offers no candidate and only
    // the pending nodes this change wrote can match
    bool isMatchable() const { return _matchable; }

    bool isBuilt() const { return _built; }
    void markBuilt() { _built = true; }

    ColumnNodeIDs* getScanNodes() const { return _scanNodes; }

    const std::vector<NLMergeScanProperty>& scanProperties() const { return _scanProperties; }
    void addScanProperty(const NLMergeScanProperty& property) { _scanProperties.push_back(property); }

    // The spec's key properties in the types they are written with, which is what a
    // pending node's values are read back through
    const NLMergeScanProperties& writtenProperties() const { return _writtenProperties; }
    NLMergeScanProperties& writtenProperties() { return _writtenProperties; }
    void addWrittenProperty(const NLMergeScanProperty& property) { _writtenProperties.push_back(property); }

    size_t getNextPendingNode() const { return _nextPendingNode; }
    void setNextPendingNode(size_t offset) { _nextPendingNode = offset; }

    // How far the index has read the updates to the graph's nodes and to the ones this
    // query wrote. A node an update reached after the index took it in is keyed again
    // under what it holds now, and its entries under earlier values stay behind: a lookup
    // checks such a node against its current values.
    size_t getNextNodeUpdate() const { return _nextNodeUpdate; }
    void setNextNodeUpdate(size_t position) { _nextNodeUpdate = position; }

    size_t getNextPendingNodeUpdate() const { return _nextPendingNodeUpdate; }
    void setNextPendingNodeUpdate(size_t position) { _nextPendingNodeUpdate = position; }

    bool hasChanged(const NLMergeRef& ref) const { return _changedNodes.contains(ref.asKey()); }
    void markChanged(const NLMergeRef& ref) { _changedNodes.insert(ref.asKey()); }

    void add(std::string_view key, const NLMergeRef& ref) { _byKey[std::string(key)].push_back(ref); }

    std::span<const NLMergeRef> find(std::string_view key) const;

private:
    LabelSet _labels;
    LabelSet _writeLabels;
    std::vector<NLMergeScanProperty> _scanProperties;
    NLMergeScanProperties _writtenProperties;
    StringHashMap<std::string, std::vector<NLMergeRef>> _byKey;
    std::unordered_set<uint64_t> _changedNodes;
    ColumnNodeIDs* _scanNodes {nullptr};
    size_t _nextPendingNode {0};
    size_t _nextNodeUpdate {0};
    size_t _nextPendingNodeUpdate {0};
    bool _matchable {false};
    bool _built {false};
};

// Every edge this change wrote, whichever clause or statement wrote it, under each of the
// two nodes it joins: a pending edge is in no graph the match reads, so this is where a
// hop finds it. One log per program, shared by every merge op in it, and taken in from the
// write buffer as the query writes.
class NLMergePendingEdges {
public:
    struct Entry {
        NLMergeRef _other;
        EdgeTypeID _edgeType;
        uint64_t _offset {0};
    };

    NLMergePendingEdges();
    ~NLMergePendingEdges();

    void add(const NLMergeRef& source, const NLMergeRef& target, EdgeTypeID edgeType, uint64_t offset);

    size_t getNextPendingEdge() const { return _nextPendingEdge; }
    void setNextPendingEdge(size_t offset) { _nextPendingEdge = offset; }

    std::span<const Entry> outOf(const NLMergeRef& node) const;
    std::span<const Entry> into(const NLMergeRef& node) const;

private:
    std::unordered_map<uint64_t, std::vector<Entry>> _outgoing;
    std::unordered_map<uint64_t, std::vector<Entry>> _incoming;
    size_t _nextPendingEdge {0};

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

        const ColumnNodeIDs* _boundColumn {nullptr};
        const ColumnVector<CustomBool>* _boundPending {nullptr};
        NLMergeNodeIndex* _index {nullptr};
        LabelSetHandle _labelSetHandle;
        ColumnNodeIDs* _output {nullptr};
        ColumnVector<CustomBool>* _outputPending {nullptr};
    };

    // One hop of the chain, joining the node ahead of it to the one behind. The match
    // edge type is invalid when the graph's schema does not have it, which leaves only
    // the pending edges to match. The scratch chunks below are what a hop constraining
    // properties reads its candidates' values through: the graph's in the types the graph
    // holds them as, the pending ones' in the types they are written with.
    struct Hop {
        std::vector<NLMergeProperty> _properties;
        std::vector<NLMergeScanProperty> _scanProperties;
        NLMergeScanProperties _writtenProperties;

        EdgeTypeID _matchEdgeType;
        EdgeTypeID _writeEdgeType;
        NLMergeDirection _direction {NLMergeDirection::Forward};
        ColumnNodeIDs* _scanSources {nullptr};
        ColumnEdgeIDs* _scanEdges {nullptr};
        ColumnEdgeIDs* _output {nullptr};
        ColumnVector<CustomBool>* _outputPending {nullptr};
    };

    NLMergeData(NLMergePendingEdges* pendingEdges, ColumnVector<CustomBool>* created, LocalMemory* memory);
    ~NLMergeData() override;

    const std::vector<Node>& nodes() const { return _nodes; }
    const std::vector<Hop>& hops() const { return _hops; }
    const std::vector<NLCarriedColumn>& carriedColumns() const { return _carriedColumns; }

    std::vector<Node>& nodes() { return _nodes; }
    std::vector<Hop>& hops() { return _hops; }

    NLMergePendingEdges* getPendingEdges() const { return _pendingEdges; }
    ColumnVector<CustomBool>* getCreated() const { return _created; }

    LocalMemory* getMemory() const { return _memory; }

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
    NLMergePendingEdges* _pendingEdges {nullptr};
    ColumnVector<CustomBool>* _created {nullptr};
    LocalMemory* _memory {nullptr};
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
    const ColumnVector<CustomBool>* getPending() const { return _pending; }
    void setPending(const ColumnVector<CustomBool>* pending) { _pending = pending; }

    bool isAllPending() const { return _allPending; }
    void setAllPending(bool allPending) { _allPending = allPending; }

    // The rows the write touches, or null for a write that touches every one: Cypher's
    // ON CREATE hands over the merge's created mask and ON MATCH its negation.
    const ColumnVector<CustomBool>* getRows() const { return _rows; }
    void setRows(const ColumnVector<CustomBool>* rows) { _rows = rows; }

    // A null or a column of tagged cells reaches a property the program types as it runs:
    // a removal stages a null of the type it has by then, and every cell is checked against
    // that type and staged as it. Null for a column of a type the write was translated with.
    NLRuntimeProperty* getRuntimeProperty() const { return _runtimeProperty; }
    void setRuntimeProperty(NLRuntimeProperty* runtimeProperty) { _runtimeProperty = runtimeProperty; }

    bool writesNull() const { return _writesNull; }
    void setWritesNull(bool writesNull) { _writesNull = writesNull; }

    // The rows of this step the write reaches, which alone type and check its cells
    ColumnVector<CustomBool>* getWrittenRows() const { return _writtenRows; }
    void setWrittenRows(ColumnVector<CustomBool>* writtenRows) { _writtenRows = writtenRows; }

private:
    const ColumnNodeIDs* _input {nullptr};
    const Column* _value {nullptr};
    const ColumnVector<CustomBool>* _pending {nullptr};
    const ColumnVector<CustomBool>* _rows {nullptr};
    NLRuntimeProperty* _runtimeProperty {nullptr};
    ColumnVector<CustomBool>* _writtenRows {nullptr};
    PropertyTypeID _propertyTypeID;
    bool _writesNull {false};
    bool _allPending {false};
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

    const ColumnVector<CustomBool>* getPending() const { return _pending; }
    void setPending(const ColumnVector<CustomBool>* pending) { _pending = pending; }

    bool isAllPending() const { return _allPending; }
    void setAllPending(bool allPending) { _allPending = allPending; }

    const ColumnVector<CustomBool>* getRows() const { return _rows; }
    void setRows(const ColumnVector<CustomBool>* rows) { _rows = rows; }

    // A null or a column of tagged cells reaches a property the program types as it runs:
    // a removal stages a null of the type it has by then, and every cell is checked against
    // that type and staged as it. Null for a column of a type the write was translated with.
    NLRuntimeProperty* getRuntimeProperty() const { return _runtimeProperty; }
    void setRuntimeProperty(NLRuntimeProperty* runtimeProperty) { _runtimeProperty = runtimeProperty; }

    bool writesNull() const { return _writesNull; }
    void setWritesNull(bool writesNull) { _writesNull = writesNull; }

    // The rows of this step the write reaches, which alone type and check its cells
    ColumnVector<CustomBool>* getWrittenRows() const { return _writtenRows; }
    void setWrittenRows(ColumnVector<CustomBool>* writtenRows) { _writtenRows = writtenRows; }

private:
    const ColumnEdgeIDs* _input {nullptr};
    const Column* _value {nullptr};
    const ColumnVector<CustomBool>* _pending {nullptr};
    const ColumnVector<CustomBool>* _rows {nullptr};
    NLRuntimeProperty* _runtimeProperty {nullptr};
    ColumnVector<CustomBool>* _writtenRows {nullptr};
    PropertyTypeID _propertyTypeID;
    bool _writesNull {false};
    bool _allPending {false};
};

template <typename IDColumn>
class NLSetPropertiesData : public NLFunctionData {
public:
    using IDColumnType = IDColumn;

    NLSetPropertiesData(const IDColumn* input, const Column* value, bool replaces)
        : _input(input),
        _value(value),
        _replaces(replaces)
    {
    }

    const IDColumn* getInput() const { return _input; }
    const Column* getValue() const { return _value; }
    bool getReplaces() const { return _replaces; }

    const ColumnVector<CustomBool>* getPending() const { return _pending; }
    void setPending(const ColumnVector<CustomBool>* pending) { _pending = pending; }

    bool isAllPending() const { return _allPending; }
    void setAllPending(bool allPending) { _allPending = allPending; }

    const ColumnVector<CustomBool>* getRows() const { return _rows; }
    void setRows(const ColumnVector<CustomBool>* rows) { _rows = rows; }

    bool writesNull() const { return _writesNull; }
    void setWritesNull(bool writesNull) { _writesNull = writesNull; }

    std::vector<WrittenMap>& getMaps() { return _maps; }
    CommitWriteBuffer::UntypedProperties& getMapProperties() { return _mapProperties; }
    std::vector<StagedRange>& getMapRanges() { return _mapRanges; }
    CommitWriteBuffer::UntypedProperties& getRemovals() { return _removals; }
    std::vector<PropertyTypeID>& getHeld() { return _held; }
    std::vector<ValueType>& getHeldTypes() { return _heldTypes; }
    std::vector<MapKeyPropertyType>& getKeyTypes() { return _keyTypes; }

private:
    const IDColumn* _input {nullptr};
    const Column* _value {nullptr};
    const ColumnVector<CustomBool>* _pending {nullptr};
    const ColumnVector<CustomBool>* _rows {nullptr};
    bool _allPending {false};
    bool _replaces {false};
    bool _writesNull {false};
    std::vector<WrittenMap> _maps;
    CommitWriteBuffer::UntypedProperties _mapProperties;
    std::vector<StagedRange> _mapRanges;
    CommitWriteBuffer::UntypedProperties _removals;
    std::vector<PropertyTypeID> _held;
    std::vector<ValueType> _heldTypes;
    std::vector<MapKeyPropertyType> _keyTypes;
};

using NLSetNodePropertiesData = NLSetPropertiesData<ColumnNodeIDs>;
using NLSetEdgePropertiesData = NLSetPropertiesData<ColumnEdgeIDs>;

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
    const ColumnVector<CustomBool>* getPending() const { return _pending; }
    void setPending(const ColumnVector<CustomBool>* pending) { _pending = pending; }

    bool isAllPending() const { return _allPending; }
    void setAllPending(bool allPending) { _allPending = allPending; }

    // The scratch the committed rows of a mixed column are gathered into, since the
    // deletion of those goes through the graph's own IDs
    ColumnNodeIDs* getCommitted() const { return _committed; }

private:
    const ColumnNodeIDs* _input {nullptr};
    const ColumnVector<CustomBool>* _pending {nullptr};
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
    const ColumnVector<CustomBool>* getPending() const { return _pending; }
    void setPending(const ColumnVector<CustomBool>* pending) { _pending = pending; }

    bool isAllPending() const { return _allPending; }
    void setAllPending(bool allPending) { _allPending = allPending; }

    ColumnEdgeIDs* getCommitted() const { return _committed; }

    const ColumnEdgeIDs* getInput() const { return _input; }

private:
    const ColumnEdgeIDs* _input {nullptr};
    const ColumnVector<CustomBool>* _pending {nullptr};
    ColumnEdgeIDs* _committed {nullptr};
    bool _allPending {false};
};

// Runtime state of one OPTIONAL MATCH over one step of the rows its pattern joins onto:
// that step's own chunks, a matched flag per row of them, and the buffers the matched rows
// are collected into. The sibling of NLSortState, except that it covers one step rather
// than the whole relation, so the memory it holds is one step's matches.
class NLOptionalState {
public:
    // One chunk of the step the pattern joins onto, in the order the pattern yields them
    // back; borrowed, since they are the enclosing loop's own variables.
    void addInputColumn(const Column* input) { _inputColumns.push_back(input); }

    // One buffer per column the pattern contributes, in yield order.
    void addMatchedBuffer(Column* buffer) { _buffers.push_back(buffer); }

    const std::vector<const Column*>& inputColumns() const { return _inputColumns; }
    const std::vector<Column*>& buffers() const { return _buffers; }

    // The rows this step joins onto: the input chunks' row count, and one - the single
    // empty row a query opening on OPTIONAL MATCH starts from - when there are none.
    size_t getRowCount() const;

    // The rows the pattern matched, read from the first buffer as a sort reads its own.
    size_t getMatchedRowCount() const;

    // Empty the buffers and clear every matched flag, so the accumulator covers this step
    // alone. Runs each time nl.optional_buffer's block runs.
    void reset();

    void markMatched(size_t row) {
        bioassert(row < _matched.size(), "Row tag {} is outside the {} rows of the step", row, _matched.size());
        _matched[row] = true;
    }

    // The input rows nothing matched, in order. Computed on the first call of a step, so
    // the drain pays for the sweep once however many chunks it emits.
    const ColumnVector<size_t>& missedRows();

    // The values the buffers hold, copied in as NLSortState::values' are
    NLValueStore& values() { return _values; }

private:
    std::vector<const Column*> _inputColumns;
    std::vector<Column*> _buffers;
    NLValueStore _values;

    // One flag per row of this step's input chunks, cleared by the reset and set by the
    // collect through the row tag.
    std::vector<bool> _matched;

    ColumnVector<size_t> _missedRows;
    bool _swept {false};
};

// nl.optional_buffer data: empties an accumulator and lays the row tag out over this
// step's input rows, each time the block it lives in runs.
class NLOptionalResetData : public NLFunctionData {
public:
    NLOptionalResetData(NLOptionalState* state, ColumnVector<uint64_t>* tag)
        : _state(state),
        _tag(tag)
    {
    }

    NLOptionalState* getState() const { return _state; }
    ColumnVector<uint64_t>* getTag() const { return _tag; }

private:
    NLOptionalState* _state {nullptr};
    ColumnVector<uint64_t>* _tag {nullptr};
};

// nl.optional_collect data: appends the current chunk of every column the pattern
// contributes to the matching buffer, and marks the input rows the tag names. Each entry
// pairs the pattern's chunk with the buffer it grows and the append that copies one into
// the other, as nl.sort_collect's do.
class NLOptionalCollectData : public NLFunctionData {
public:
    NLOptionalCollectData(NLOptionalState* state)
        : _state(state)
    {
    }

    NLOptionalState* getState() const { return _state; }

    // Null when the accumulator has no input column: the step is then the single empty
    // row, which any collected row marks.
    const ColumnVector<uint64_t>* getTag() const { return _tag; }
    void setTag(const ColumnVector<uint64_t>* tag) { _tag = tag; }

    const std::vector<NLSortCollectData::Append>& appends() const { return _appends; }

    void addAppend(const NLSortCollectData::Append& append) { _appends.push_back(append); }

private:
    NLOptionalState* _state {nullptr};
    const ColumnVector<uint64_t>* _tag {nullptr};
    std::vector<NLSortCollectData::Append> _appends;
};

// Runtime state of one pattern comprehension or COLLECT subquery over one step of the rows
// it is read on: how many rows that step has, and what its body's rows contributed to each
// of them. The list-valued sibling of NLOptionalState - both cover one step rather than the
// whole relation - except that it reduces what the body found to one cell per row instead
// of re-emitting it as rows.
class NLRowListsState {
public:
    // Drop what the last step staged and size the accumulator to this one. Runs each time
    // nl.row_lists_buffer's block runs.
    void reset(size_t rowCount);

    void stage(size_t row, const ListBuffer<>::ListItemVariant& value);

    // One list per row of the step, in row order, built into @param lists: the values that
    // row's body rows staged, and the empty list for a row the body produced none for.
    void buildLists(ListBuffer<>& listBuffer, std::vector<ListView>& lists);

private:
    size_t _rowCount {0};

    // What the body's rows contributed and, row-aligned with it, the row each value belongs
    // to: they are staged in the order the body walked them, which is not the rows'.
    std::vector<ListBuffer<>::ListItemVariant> _values;
    std::vector<size_t> _rows;

    // The copies of the staged values, which the body's later steps would otherwise empty
    // the buffers of
    NLValueStore _stagedValues;

    // The values under the row they belong to, where each row's run of them starts and how
    // far that run has been filled: held here so a step pays for the buffers once rather
    // than once per build.
    std::vector<ListBuffer<>::ListItemVariant> _ordered;
    std::vector<size_t> _starts;
    std::vector<size_t> _next;
};

// nl.row_lists_buffer data: empties an accumulator and lays the row tag out over this
// step's rows, each time the block it lives in runs. The cardinality column is null where
// nothing is in flight, which is the single empty row.
class NLRowListsResetData : public NLFunctionData {
public:
    NLRowListsResetData(NLRowListsState* state,
                        const Column* cardinality,
                        ColumnVector<uint64_t>* tag)
        : _state(state),
        _cardinality(cardinality),
        _tag(tag)
    {
    }

    NLRowListsState* getState() const { return _state; }
    const Column* getCardinality() const { return _cardinality; }
    ColumnVector<uint64_t>* getTag() const { return _tag; }

private:
    NLRowListsState* _state {nullptr};
    const Column* _cardinality {nullptr};
    ColumnVector<uint64_t>* _tag {nullptr};
};

// nl.row_lists_collect data: stages what this step of the body's rows contributes, each
// value under the row its tag names, through the read its column shape takes. The tag is
// null when every value belongs to one row: the one the row chunk holds the position of,
// or the single row of the step when that is null too.
class NLRowListsCollectData : public NLFunctionData {
public:
    NLRowListsCollectData(NLRowListsState* state,
                          const ColumnVector<uint64_t>* tag,
                          const ColumnVector<uint64_t>* row,
                          const Column* value,
                          NLListItemReadFunction valueRead,
                          NLStepBuffers* stepBuffers)
        : _state(state),
        _tag(tag),
        _row(row),
        _value(value),
        _valueRead(valueRead),
        _stepBuffers(stepBuffers)
    {
    }

    NLRowListsState* getState() const { return _state; }
    const ColumnVector<uint64_t>* getTag() const { return _tag; }
    const ColumnVector<uint64_t>* getRow() const { return _row; }
    const Column* getValue() const { return _value; }
    NLListItemReadFunction getValueRead() const { return _valueRead; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

private:
    NLRowListsState* _state {nullptr};
    const ColumnVector<uint64_t>* _tag {nullptr};
    const ColumnVector<uint64_t>* _row {nullptr};
    const Column* _value {nullptr};
    NLListItemReadFunction _valueRead {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
};

// nl.row_lists_result data: the build phase, writing one list per row of the step into
// the result column once the body's nest has been walked.
class NLRowListsResultData : public NLFunctionData {
public:
    NLRowListsResultData(NLRowListsState* state,
                         ColumnVector<ListView>* result,
                         NLStepBuffers* stepBuffers)
        : _state(state),
        _result(result),
        _stepBuffers(stepBuffers)
    {
    }

    NLRowListsState* getState() const { return _state; }
    ColumnVector<ListView>* getResult() const { return _result; }
    NLStepBuffers* getStepBuffers() const { return _stepBuffers; }

private:
    NLRowListsState* _state {nullptr};
    ColumnVector<ListView>* _result {nullptr};
    NLStepBuffers* _stepBuffers {nullptr};
};

// nl.for over nl.optional_drain data: the emit phase of an OPTIONAL MATCH. Holds the
// accumulator and, per column, the buffer the matched rows are read from, the input chunk
// a missed row's value is read from - null for a column the pattern binds, which a missed
// row has no value for - the loop variable to fill, and the gather and null fill that
// write it. The indices scratch holds the row slice of the current emit step.
class NLOptionalDrainLoopData : public NLFunctionData {
public:
    struct DrainColumn {
        const Column* _buffer {nullptr};
        const Column* _input {nullptr};
        Column* _output {nullptr};
        NLGatherFunction _gather {nullptr};
        NLFillNullFunction _fillNull {nullptr};
    };

    NLOptionalDrainLoopData(NLOptionalState* state)
        : _state(state)
    {
    }

    NLOptionalState* getState() const { return _state; }

    const std::vector<DrainColumn>& columns() const { return _columns; }

    void addColumn(const DrainColumn& column) { _columns.push_back(column); }

    ColumnVector<size_t>* getIndices() { return &_indices; }

    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    NLOptionalState* _state {nullptr};
    NLLimitState* _limit {nullptr};
    std::vector<DrainColumn> _columns;
    ColumnVector<size_t> _indices;
    NLStmtContainer _stmts;
};

// Runtime state of one EXISTS or COUNT subquery over one step of the rows it answers for
class NLSubqueryExpressionState {
public:
    virtual ~NLSubqueryExpressionState();

    // One chunk of the step the subquery answers for; borrowed, since they are the
    // enclosing loop's own variables.
    void addInputColumn(const Column* input) { _inputColumns.push_back(input); }

    // The rows this step answers for: the input chunks' row count, and one - the single
    // empty row a subquery over nothing in flight joins onto - when there are none.
    size_t getRowCount() const;

    // Clear every answer, so the accumulator covers this step alone. Runs each time the
    // buffer's block runs.
    virtual void reset() = 0;

private:
    std::vector<const Column*> _inputColumns;
};

class NLExistsState : public NLSubqueryExpressionState {
public:
    void reset() override;

    void markMatched(size_t row) {
        bioassert(row < _matched.size(), "Row tag {} is outside the {} rows of the step", row, _matched.size());
        _matched[row] = true;
    }

    const std::vector<bool>& matched() const { return _matched; }

private:
    // One flag per row of this step's input chunks, cleared by the reset and set by the
    // mark through the row tag.
    std::vector<bool> _matched;
};

// nl.exists_buffer and nl.count_subquery_buffer data: empties an accumulator and lays the
// row tag out over this step's input rows, each time the block it lives in runs.
class NLSubqueryExpressionResetData : public NLFunctionData {
public:
    NLSubqueryExpressionResetData(NLSubqueryExpressionState* state, ColumnVector<uint64_t>* tag);

    NLSubqueryExpressionState* getState() const { return _state; }
    ColumnVector<uint64_t>* getTag() const { return _tag; }

private:
    NLSubqueryExpressionState* _state {nullptr};
    ColumnVector<uint64_t>* _tag {nullptr};
};

// nl.exists_mark data: marks the input rows the tag names. With no tag the columns answer
// for one row instead, which this step's rows mark when they hold one at all: the row the
// row chunk holds the position of, or the single empty row when that is null too.
class NLExistsMarkData : public NLFunctionData {
public:
    NLExistsMarkData(NLExistsState* state)
        : _state(state)
    {
    }

    NLExistsState* getState() const { return _state; }

    const ColumnVector<uint64_t>* getTag() const { return _tag; }
    void setTag(const ColumnVector<uint64_t>* tag) { _tag = tag; }

    const ColumnVector<uint64_t>* getRow() const { return _row; }
    void setRow(const ColumnVector<uint64_t>* row) { _row = row; }

    const std::vector<const Column*>& columns() const { return _columns; }

    void addColumn(const Column* column) { _columns.push_back(column); }

private:
    NLExistsState* _state {nullptr};
    const ColumnVector<uint64_t>* _tag {nullptr};
    const ColumnVector<uint64_t>* _row {nullptr};
    std::vector<const Column*> _columns;
};

// nl.exists_result data: lays the accumulator's flags out as the boolean chunk the EXISTS
// stands for, row-aligned with the chunks the accumulator was opened over.
class NLExistsResultData : public NLFunctionData {
public:
    NLExistsResultData(NLExistsState* state, ColumnVector<CustomBool>* result)
        : _state(state),
        _result(result)
    {
    }

    NLExistsState* getState() const { return _state; }
    ColumnVector<CustomBool>* getResult() const { return _result; }

private:
    NLExistsState* _state {nullptr};
    ColumnVector<CustomBool>* _result {nullptr};
};

// A count per row of the step
class NLCountSubqueryState : public NLSubqueryExpressionState {
public:
    void reset() override;

    void addRows(uint64_t row, size_t rowCount);
    void addTaggedRows(std::span<const uint64_t> tag);

    const std::vector<uint64_t>& getCounts() const { return _counts; }

private:
    std::vector<uint64_t> _counts;
};

// nl.count_subquery_tally data: counts the entries of the tag toward the rows they name,
// or, with no tag, the rows of @param rows toward the one row they answer for: the row
// @param row holds the position of, or the single row of the step when it is null
class NLCountSubqueryTallyData : public NLFunctionData {
public:
    NLCountSubqueryTallyData(NLCountSubqueryState* state,
                             const ColumnVector<uint64_t>* tag,
                             const ColumnVector<uint64_t>* row,
                             const Column* rows);

    NLCountSubqueryState* getState() const { return _state; }
    const ColumnVector<uint64_t>* getTag() const { return _tag; }
    const ColumnVector<uint64_t>* getRow() const { return _row; }
    const Column* getRows() const { return _rows; }

private:
    NLCountSubqueryState* _state {nullptr};
    const ColumnVector<uint64_t>* _tag {nullptr};
    const ColumnVector<uint64_t>* _row {nullptr};
    const Column* _rows {nullptr};
};

class NLCountSubqueryResultData : public NLFunctionData {
public:
    NLCountSubqueryResultData(NLCountSubqueryState* state, ColumnVector<uint64_t>* result);

    NLCountSubqueryState* getState() const { return _state; }
    ColumnVector<uint64_t>* getResult() const { return _result; }

private:
    NLCountSubqueryState* _state {nullptr};
    ColumnVector<uint64_t>* _result {nullptr};
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
