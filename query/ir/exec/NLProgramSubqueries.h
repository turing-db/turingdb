#pragma once

#include <stddef.h>
#include <stdint.h>
#include <span>
#include <vector>

#include "NLStepBuffers.h"
#include "NLValueStore.h"
#include "columns/ColumnVector.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"

#include "NLFunctionData.h"
#include "NLLimitState.h"
#include "NLColumnFunctions.h"
#include "NLProgramRows.h"

#include "BioAssert.h"

namespace db {

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

}
