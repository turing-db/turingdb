#pragma once

#include "GetOutEdgesIterator.h"

#include "metadata/LabelSetHandle.h"

namespace db {

class GetOutEdgesByLabelChunkWriter : public GetOutEdgesIterator {
public:
    GetOutEdgesByLabelChunkWriter() = delete;
    GetOutEdgesByLabelChunkWriter(const GraphView& view,
                                  const ColumnNodeIDs* inputNodeIDs,
                                  const LabelSetHandle& labelset);

    void fill(size_t maxCount);

    void setInputNodeIDs(const ColumnNodeIDs* inputNodeIDs) { _inputNodeIDs = inputNodeIDs; }
    void setIndices(ColumnVector<size_t>* indices) { _indices = indices; }
    void setEdgeIDs(ColumnEdgeIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setTgtIDs(ColumnNodeIDs* tgts) { _tgts = tgts; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

private:
    LabelSetHandle _labelset;

    ColumnVector<size_t>* _indices {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _tgts {nullptr};
    ColumnEdgeTypes* _types {nullptr};

    TombstoneFilter _filter;

    void filterTombstones();
};

static_assert(NonRootChunkWriter<GetOutEdgesByLabelChunkWriter>);
static_assert(EdgeIDsChunkWriter<GetOutEdgesByLabelChunkWriter>);
static_assert(TgtIDsChunkWriter<GetOutEdgesByLabelChunkWriter>);
static_assert(EdgeTypesChunkWriter<GetOutEdgesByLabelChunkWriter>);

}
