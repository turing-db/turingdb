#pragma once

#include "GetOutEdgesIterator.h"

#include "ID.h"

namespace db {

// A GetOutEdgesChunkWriter restricted to the out-edges of one edge type, modelling
// the MATCH (a)-[:TYPE]->(b) hop. It reuses GetOutEdgesIterator's datapart and
// node-span navigation unchanged - only fill() differs: it walks each node's edge
// span edge by edge and writes only the records whose _edgeTypeID matches straight
// into the output columns. No unfiltered chunk is materialized and then filtered,
// so the by-type filter costs no extra copy over the plain fetch.
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
    // The single edge type kept; every other type is skipped during fill().
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
