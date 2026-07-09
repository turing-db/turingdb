#pragma once

#include "GetInEdgesIterator.h"

#include "ID.h"

namespace db {

// The predecessor counterpart of GetOutEdgesByTypeChunkWriter: a
// GetInEdgesChunkWriter restricted to the in-edges of one edge type, modelling the
// MATCH (a)<-[:TYPE]-(b) hop. It reuses GetInEdgesIterator's datapart and node-span
// navigation unchanged - only fill() differs: it walks each node's in-edge span
// edge by edge and writes only the records whose _edgeTypeID matches straight into
// the output columns, so the by-type filter costs no extra copy over the plain
// fetch.
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
    // The single edge type kept; every other type is skipped during fill().
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
