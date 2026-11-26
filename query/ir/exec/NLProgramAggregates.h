#pragma once

#include <stddef.h>
#include <stdint.h>
#include <new>
#include <span>
#include <vector>

#include "ID.h"
#include "NLGroupHashTable.h"
#include "NLStepBuffers.h"
#include "NLValueStore.h"
#include "columns/ColumnVector.h"
#include "list/ListBuffer.h"
#include "metadata/LabelSet.h"

#include "NLFunctionData.h"
#include "NLLimitState.h"
#include "NLColumnFunctions.h"

namespace db {

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

}
