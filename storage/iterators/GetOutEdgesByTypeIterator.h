#pragma once

#include "GetOutEdgesIterator.h"
#include "EdgeExclusion.h"

#include <span>

#include "ID.h"

namespace db {

class GetOutEdgesByTypeChunkWriter : public GetOutEdgesIterator {
public:
    GetOutEdgesByTypeChunkWriter() = delete;
    GetOutEdgesByTypeChunkWriter(const GraphView& view,
                                 const ColumnNodeIDs* inputNodeIDs,
                                 std::span<const EdgeTypeID> edgeTypes);

    void fill(size_t maxCount);

    void setInputNodeIDs(const ColumnNodeIDs* inputNodeIDs) { _inputNodeIDs = inputNodeIDs; }
    void setIndices(ColumnVector<size_t>* indices) { _indices = indices; }
    void setEdgeIDs(ColumnEdgeIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setTgtIDs(ColumnNodeIDs* tgts) { _tgts = tgts; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

    void setDistinctFrom(std::span<const ColumnEdgeIDs* const> columns) { _exclusion.setEdgeColumns(columns); }
    void setDistinctFromPaths(std::span<const ColumnVector<PathRef>* const> columns, const PathTrie* trie) { _exclusion.setPathColumns(columns, trie); }

private:
    // Borrowed, not owned: the caller keeps the types alive for the writer's lifetime
    std::span<const EdgeTypeID> _edgeTypes;

    ColumnVector<size_t>* _indices {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _tgts {nullptr};
    ColumnEdgeTypes* _types {nullptr};

    TombstoneFilter _filter;
    EdgeExclusion _exclusion;

    void filterTombstones();
};

static_assert(NonRootChunkWriter<GetOutEdgesByTypeChunkWriter>);
static_assert(EdgeIDsChunkWriter<GetOutEdgesByTypeChunkWriter>);
static_assert(TgtIDsChunkWriter<GetOutEdgesByTypeChunkWriter>);
static_assert(EdgeTypesChunkWriter<GetOutEdgesByTypeChunkWriter>);

}
