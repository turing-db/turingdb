#pragma once

#include "GetOutEdgesIterator.h"

#include "ID.h"

namespace db {

class GetOutEdgesByTypeChunkWriter : public GetOutEdgesIterator {
public:
    GetOutEdgesByTypeChunkWriter() = delete;
    GetOutEdgesByTypeChunkWriter(const GraphView& view,
                                 const ColumnNodeIDs* inputNodeIDs,
                                 EdgeTypeID edgeType);

    void fill(size_t maxCount);

    void setInputNodeIDs(const ColumnNodeIDs* inputNodeIDs) { _inputNodeIDs = inputNodeIDs; }
    void setIndices(ColumnVector<size_t>* indices) { _indices = indices; }
    void setEdgeIDs(ColumnEdgeIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setTgtIDs(ColumnNodeIDs* tgts) { _tgts = tgts; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

private:
    EdgeTypeID _edgeType;

    ColumnVector<size_t>* _indices {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _tgts {nullptr};
    ColumnEdgeTypes* _types {nullptr};

    TombstoneFilter _filter;

    void filterTombstones();
};

static_assert(NonRootChunkWriter<GetOutEdgesByTypeChunkWriter>);
static_assert(EdgeIDsChunkWriter<GetOutEdgesByTypeChunkWriter>);
static_assert(TgtIDsChunkWriter<GetOutEdgesByTypeChunkWriter>);
static_assert(EdgeTypesChunkWriter<GetOutEdgesByTypeChunkWriter>);

}
