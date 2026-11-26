#pragma once

#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "NLHashJoinIndex.h"
#include "NLValueStore.h"
#include "columns/ColumnVector.h"

#include "NLFunctionData.h"
#include "NLLimitState.h"
#include "NLColumnFunctions.h"
#include "NLProgramRows.h"

namespace db {

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

}
