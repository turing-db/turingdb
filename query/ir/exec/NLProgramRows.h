#pragma once

#include <stddef.h>
#include <vector>

#include "ID.h"
#include "NLGroupHashTable.h"
#include "NLValueStore.h"
#include "columns/ColumnVector.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "list/PathTrie.h"

#include "NLFunctionData.h"
#include "NLLimitState.h"
#include "NLColumnFunctions.h"

namespace db {

// nl.each_row loop data: the step's chunks and a one-row output per chunk, each filled
// through the gather an edge hop uses, over an index column holding the one row the step
// is at. A plain source over the step's rows, so a downstream LIMIT can bound it.
class NLEachRowLoopData : public NLFunctionData {
public:
    using Columns = std::vector<NLCarriedColumn>;

    const Columns& columns() const { return _columns; }

    void addColumn(const NLCarriedColumn& column) { _columns.push_back(column); }

    ColumnVector<size_t>* getIndices() { return &_indices; }

    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    Columns _columns;
    ColumnVector<size_t> _indices;
    NLLimitState* _limit {nullptr};
    NLStmtContainer _stmts;
};

// nl.limit data: resets a counter to its budget each time the block it lives in
// runs - once at function scope for a top-level LIMIT, or per enclosing step for
// a nested one. The budget is the count the query spells, or the one held by the
// constant column bound to it, read at each reset.
class NLLimitInitData : public NLFunctionData {
public:
    NLLimitInitData(NLLimitState* state, size_t count, const Column* boundCount)
        : _state(state),
        _count(count),
        _boundCount(boundCount)
    {
    }

    NLLimitState* getState() const { return _state; }
    size_t getCount() const { return _count; }
    const Column* getBoundCount() const { return _boundCount; }

private:
    NLLimitState* _state {nullptr};
    size_t _count {0};
    const Column* _boundCount {nullptr};
};

// nl.limit_remaining data: resets a counter to the budget another one has left each
// time the block it lives in runs.
class NLLimitRemainingData : public NLFunctionData {
public:
    NLLimitRemainingData(NLLimitState* state, const NLLimitState* source)
        : _state(state),
        _source(source)
    {
    }

    NLLimitState* getState() const { return _state; }
    const NLLimitState* getSource() const { return _source; }

private:
    NLLimitState* _state {nullptr};
    const NLLimitState* _source {nullptr};
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
    NLSkipInitData(NLSkipState* state, size_t count, const Column* boundCount)
        : _state(state),
        _count(count),
        _boundCount(boundCount)
    {
    }

    NLSkipState* getState() const { return _state; }
    size_t getCount() const { return _count; }
    const Column* getBoundCount() const { return _boundCount; }

private:
    NLSkipState* _state {nullptr};
    size_t _count {0};
    const Column* _boundCount {nullptr};
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

// Appends a chunk of views onto the tail of a buffer, copying each value into @param values
// so the buffer owns what it holds rather than viewing the buffer the chunk was built in
using NLOwnedAppendFunction = void (*)(const Column* input, Column* buffer, NLValueStore& values);

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

    // The values the buffers hold. A collected view is copied in here, because the views a
    // chunk carries point into the buffers the producing step built them in, and that step
    // comes round again long before the emit loop reads them.
    NLValueStore& values() { return _values; }

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

    NLValueStore _values;

    // The top-K bound (valid only when _bounded); 0 with _bounded means keep none.
    size_t _topK {0};
    bool _bounded {false};

    bool _sorted {false};
};

// nl.sort_buffer data: resets an accumulator to empty each time the block it
// lives in runs - once at function scope for a top-level ORDER BY. A top-K bound
// to a constant column is read at each reset.
class NLSortResetData : public NLFunctionData {
public:
    NLSortResetData(NLSortState* state, const Column* boundTopK)
        : _state(state),
        _boundTopK(boundTopK)
    {
    }

    NLSortState* getState() const { return _state; }
    const Column* getBoundTopK() const { return _boundTopK; }

private:
    NLSortState* _state {nullptr};
    const Column* _boundTopK {nullptr};
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

        // Set in place of _append for a column of views, which the accumulator takes a
        // copy of rather than a view of
        NLOwnedAppendFunction _appendOwned {nullptr};
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

// Runtime state of one DISTINCT: the hash table of the rows already emitted, keyed by
// every column of the row. nl.distinct resets it (once at function scope for a top-level
// or mid-query DISTINCT), and nl.distinct_filter is the sole reader and mutator - a row
// that creates a group in the table is the first with its key, so it survives. The
// streaming sibling of NLSortState: it filters rows as they arrive rather than
// accumulating them, so it holds only the keys, not the row values (a survivor is
// emitted from the incoming chunk it appeared in, never rebuilt).
class NLDistinctState {
public:
    NLGroupHashTable& hashTable() { return _hashTable; }

    // Drop every key, so DISTINCT starts fresh. Runs each time nl.distinct's block
    // runs (once at function scope for a top-level / mid-query DISTINCT).
    void reset() { _hashTable.clear(); }

private:
    NLGroupHashTable _hashTable;
};

// nl.distinct data: resets a DISTINCT's hash table to empty each time the block it
// lives in runs - once at function scope for a top-level DISTINCT. The distinct
// sibling of NLSortResetData.
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

// nl.distinct_filter data: the DISTINCT whose hash table keys each row, and per column
// its input chunk, the fresh output chunk to fill, and the gather that copies the
// surviving rows into the output. All columns together form each row's key; the
// surviving rows are gathered from the incoming chunk (reusing the NLGatherFunction the
// edge loops use), so no row values are stored. The indices scratch holds this step's
// surviving row indices.
class NLDistinctFilterData : public NLFunctionData {
public:
    struct FilterColumn {
        const Column* _input {nullptr};
        Column* _output {nullptr};
        NLGatherFunction _gather {nullptr};
    };

    NLDistinctFilterData(NLDistinctState* state)
        : _state(state)
    {
    }

    NLDistinctState* getState() const { return _state; }

    const std::vector<FilterColumn>& columns() const { return _columns; }
    const std::vector<const Column*>& getKeyColumns() const { return _keyColumns; }

    void addColumn(const FilterColumn& column) {
        _columns.push_back(column);
        _keyColumns.push_back(column._input);
    }

    ColumnVector<size_t>* getIndices() { return &_indices; }

private:
    NLDistinctState* _state {nullptr};
    std::vector<FilterColumn> _columns;
    std::vector<const Column*> _keyColumns;

    // Scratch for this step's surviving row indices, fed to the per-column gather
    ColumnVector<size_t> _indices;
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

// nl.output data
class NLOutputData : public NLFunctionData {
public:
    using OutputColumns = std::vector<const Column*>;

    // A path chunk emitted as the list of edge IDs each handle stands for
    struct PathExpansion {
        const ColumnVector<PathRef>* _paths {nullptr};
        ColumnVector<ListView>* _lists {nullptr};
    };

    const OutputColumns& outputs() const { return _columns; }

    const std::vector<PathExpansion>& pathExpansions() const { return _pathExpansions; }

    // The sink receives the expanded lists; the row count is still read off the handles
    void addExpandedPathColumn(const ColumnVector<PathRef>* paths,
                               ColumnVector<ListView>* lists,
                               bool carriesRows) {
        _columns.push_back(lists);
        _pathExpansions.push_back({paths, lists});

        if (carriesRows) {
            _rowCountColumns.push_back(paths);
        }
    }

    const PathTrie* getTrie() const { return _trie; }
    void setTrie(const PathTrie* trie) { _trie = trie; }

    // Holds the expanded lists of one emission and is emptied before the next
    QueryListBuffer& pathListBuffer() { return _pathListBuffer; }

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
    std::vector<PathExpansion> _pathExpansions;
    const PathTrie* _trie {nullptr};
    QueryListBuffer _pathListBuffer;
    NLLimitState* _limit {nullptr};
    NLSkipState* _skip {nullptr};
    // Column which may define the cardinality of the output
    const Column* _cardinality {nullptr};
};

// Runtime state of one row accumulator: one growing buffer per column, which every
// nl.row_collect appends to and each nl.for over an nl.row_drain reads back in collected
// order. The buffer columns are borrowed: the translator pool-allocates
// them in the same arena as the loop columns.
class NLRowState {
public:
    void addBuffer(Column* buffer) { _buffers.push_back(buffer); }

    const std::vector<Column*>& buffers() const { return _buffers; }
    Column* buffer(size_t index) const { return _buffers[index]; }

    size_t getRowCount() const;

    // Clear every buffer; runs each time nl.row_buffer's block runs
    void reset();

    // The values the buffers hold, copied in as NLSortState::values' are
    NLValueStore& values() { return _values; }

private:
    std::vector<Column*> _buffers;
    NLValueStore _values;
};

// nl.row_buffer data: resets an accumulator to empty each time its block runs
class NLRowResetData : public NLFunctionData {
public:
    NLRowResetData(NLRowState* state)
        : _state(state)
    {
    }

    NLRowState* getState() const { return _state; }

private:
    NLRowState* _state {nullptr};
};

// nl.row_collect data: appends the current chunk of every column to the matching buffer
// of the accumulator, in NLSortCollectData's append shape
class NLRowCollectData : public NLFunctionData {
public:
    NLRowCollectData(NLRowState* state)
        : _state(state)
    {
    }

    NLRowState* getState() const { return _state; }

    const std::vector<NLSortCollectData::Append>& appends() const { return _appends; }

    void addAppend(const NLSortCollectData::Append& append) {
        _appends.push_back(append);
    }

private:
    NLRowState* _state {nullptr};
    std::vector<NLSortCollectData::Append> _appends;
};

// nl.for over nl.row_drain data: the emit phase of a row accumulator, which gathers the
// collected rows chunk by chunk into the loop variables
class NLRowLoopData : public NLFunctionData {
public:
    NLRowLoopData(NLRowState* state)
        : _state(state)
    {
    }

    NLRowState* getState() const { return _state; }

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
    NLRowState* _state {nullptr};
    NLLimitState* _limit {nullptr};
    std::vector<NLCarriedColumn> _columns;
    ColumnVector<size_t> _indices;
    NLStmtContainer _stmts;
};

}
