#pragma once

#include "GetInEdgesIterator.h"
#include "EdgeExclusion.h"

#include "metadata/LabelSetHandle.h"

namespace db {

class GetInEdgesByLabelChunkWriter : public GetInEdgesIterator {
public:
    GetInEdgesByLabelChunkWriter() = delete;
    GetInEdgesByLabelChunkWriter(const GraphView& view,
                                 const ColumnNodeIDs* inputNodeIDs,
                                 const LabelSetHandle& labelset);

    void fill(size_t maxCount);

    void setInputNodeIDs(const ColumnNodeIDs* inputNodeIDs) { _inputNodeIDs = inputNodeIDs; }
    void setIndices(ColumnVector<size_t>* indices) { _indices = indices; }
    void setEdgeIDs(ColumnEdgeIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setSrcIDs(ColumnNodeIDs* srcs) { _srcs = srcs; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

    void setDistinctFrom(std::span<const ColumnEdgeIDs* const> columns) { _exclusion.setEdgeColumns(columns); }
    void setDistinctFromPaths(std::span<const ColumnVector<PathRef>* const> columns, const PathTrie* trie) { _exclusion.setPathColumns(columns, trie); }

private:
    LabelSetHandle _labelset;

    ColumnVector<size_t>* _indices {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _srcs {nullptr};
    ColumnEdgeTypes* _types {nullptr};

    TombstoneFilter _filter;
    EdgeExclusion _exclusion;

    void filterTombstones();
};

static_assert(NonRootChunkWriter<GetInEdgesByLabelChunkWriter>);
static_assert(EdgeIDsChunkWriter<GetInEdgesByLabelChunkWriter>);
static_assert(SrcIDsChunkWriter<GetInEdgesByLabelChunkWriter>);
static_assert(EdgeTypesChunkWriter<GetInEdgesByLabelChunkWriter>);

}
