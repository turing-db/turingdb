#pragma once

#include "Iterator.h"

#include <span>

#include "ChunkWriter.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "datapart/EdgeRecord.h"
#include "PartIterator.h"
#include "EdgeExclusion.h"
#include "TombstoneFilter.h"

namespace db {

class GetOutEdgesIterator : public Iterator {
public:
    GetOutEdgesIterator() = default;
    GetOutEdgesIterator(const GetOutEdgesIterator&) = default;
    GetOutEdgesIterator(GetOutEdgesIterator&&) noexcept = default;
    GetOutEdgesIterator& operator=(const GetOutEdgesIterator&) = default;
    GetOutEdgesIterator& operator=(GetOutEdgesIterator&&) noexcept = default;

    GetOutEdgesIterator(const GraphView& view, const ColumnNodeIDs* inputNodeIDs);
    ~GetOutEdgesIterator() override;

    void reset();

    void next() override;

    const EdgeRecord& get() const {
        return *_edgeIt;
    }

    /**
     * @brief Sets @ref _partIt to point to the first valid datapart in the range
     * [partIdx, end]. Always leaves @ref _partIt pointing to a valid datapart, or
     * pointing to the end iterator.
     */
    void goToPart(size_t partIdx);

    GetOutEdgesIterator& operator++() {
        next();
        return *this;
    }

    const EdgeRecord& operator*() const {
        return get();
    }

protected:
    const ColumnNodeIDs* _inputNodeIDs {nullptr};
    ColumnNodeIDs::ConstIterator _nodeIt;

    std::span<const EdgeRecord> _edges;
    std::span<const EdgeRecord>::iterator _edgeIt;

    void init();
    void nextValid();

    /**
     * @brief Advances @ref _partIt by at least @param n steps, or precisely
     * d = distance(_partIt._it, _partIt._itEnd) steps if @ref n > d. Always leaves @ref
     * _partIt pointing to a valid datapart, or pointing to the end iterator.
     */
    void advancePartIterator(size_t n);
};

class GetOutEdgesChunkWriter : public GetOutEdgesIterator {
public:
    GetOutEdgesChunkWriter() = delete;
    GetOutEdgesChunkWriter(const GraphView& view, const ColumnNodeIDs* inputNodeIDs);

    void fill(size_t maxCount);

    void setInputNodeIDs(const ColumnNodeIDs* inputNodeIDs) { _inputNodeIDs = inputNodeIDs; }
    void setIndices(ColumnVector<size_t>* indices) { _indices = indices; }
    void setEdgeIDs(ColumnEdgeIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setTgtIDs(ColumnNodeIDs* tgts) { _tgts = tgts; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

    // The edges the clause bound before this hop, row-aligned with the input, which no row
    // of it repeats: edge columns, and paths read through the trie
    void setDistinctFrom(std::span<const ColumnEdgeIDs* const> columns) { _exclusion.setEdgeColumns(columns); }
    void setDistinctFromPaths(std::span<const ColumnVector<PathRef>* const> columns, const PathTrie* trie) { _exclusion.setPathColumns(columns, trie); }

private:
    ColumnVector<size_t>* _indices {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _tgts {nullptr};
    ColumnEdgeTypes* _types {nullptr};

    TombstoneFilter _filter;
    EdgeExclusion _exclusion;

    void filterTombstones();
};

struct GetOutEdgesRange {
    GraphView _view;
    const ColumnNodeIDs* _inputNodeIDs {nullptr};

    GetOutEdgesIterator begin() const { return {_view, _inputNodeIDs}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    GetOutEdgesChunkWriter chunkWriter() const { return {_view, _inputNodeIDs}; }
};

static_assert(NonRootChunkWriter<GetOutEdgesChunkWriter>);
static_assert(EdgeIDsChunkWriter<GetOutEdgesChunkWriter>);
static_assert(TgtIDsChunkWriter<GetOutEdgesChunkWriter>);
static_assert(EdgeTypesChunkWriter<GetOutEdgesChunkWriter>);

}
