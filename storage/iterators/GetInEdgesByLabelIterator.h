#pragma once

#include "GetInEdgesIterator.h"

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

private:
    LabelSetHandle _labelset;

    ColumnVector<size_t>* _indices {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _srcs {nullptr};
    ColumnEdgeTypes* _types {nullptr};

    TombstoneFilter _filter;

    void filterTombstones();
};

static_assert(NonRootChunkWriter<GetInEdgesByLabelChunkWriter>);
static_assert(EdgeIDsChunkWriter<GetInEdgesByLabelChunkWriter>);
static_assert(SrcIDsChunkWriter<GetInEdgesByLabelChunkWriter>);
static_assert(EdgeTypesChunkWriter<GetInEdgesByLabelChunkWriter>);

}
