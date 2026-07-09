#pragma once

#include "GetInEdgesIterator.h"

#include "ID.h"

namespace db {

class GetInEdgesByTypeChunkWriter : public GetInEdgesIterator {
public:
    GetInEdgesByTypeChunkWriter() = delete;
    GetInEdgesByTypeChunkWriter(const GraphView& view,
                                const ColumnNodeIDs* inputNodeIDs,
                                EdgeTypeID edgeType);

    void fill(size_t maxCount);

    void setInputNodeIDs(const ColumnNodeIDs* inputNodeIDs) { _inputNodeIDs = inputNodeIDs; }
    void setIndices(ColumnVector<size_t>* indices) { _indices = indices; }
    void setEdgeIDs(ColumnEdgeIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setSrcIDs(ColumnNodeIDs* srcs) { _srcs = srcs; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

private:
    EdgeTypeID _edgeType;

    ColumnVector<size_t>* _indices {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _srcs {nullptr};
    ColumnEdgeTypes* _types {nullptr};

    TombstoneFilter _filter;

    void filterTombstones();
};

static_assert(NonRootChunkWriter<GetInEdgesByTypeChunkWriter>);
static_assert(EdgeIDsChunkWriter<GetInEdgesByTypeChunkWriter>);
static_assert(SrcIDsChunkWriter<GetInEdgesByTypeChunkWriter>);
static_assert(EdgeTypesChunkWriter<GetInEdgesByTypeChunkWriter>);

}
