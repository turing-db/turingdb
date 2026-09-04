#pragma once

#include "ScanEdgesIterator.h"

#include "ID.h"

namespace db {

class ScanEdgesByTypeChunkWriter : public ScanEdgesIterator {
public:
    ScanEdgesByTypeChunkWriter() = delete;
    ScanEdgesByTypeChunkWriter(const GraphView& view, EdgeTypeID edgeType);

    void fill(size_t maxCount);

    void setSrcIDs(ColumnNodeIDs* srcs) { _srcs = srcs; }
    void setEdgeIDs(ColumnEdgeIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setTgtIDs(ColumnNodeIDs* tgts) { _tgts = tgts; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

private:
    EdgeTypeID _edgeType;

    ColumnNodeIDs* _srcs {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _tgts {nullptr};
    ColumnEdgeTypes* _types {nullptr};

    TombstoneFilter _filter;

    void filterTombstones();
};

static_assert(SrcIDsChunkWriter<ScanEdgesByTypeChunkWriter>);
static_assert(EdgeIDsChunkWriter<ScanEdgesByTypeChunkWriter>);
static_assert(TgtIDsChunkWriter<ScanEdgesByTypeChunkWriter>);
static_assert(EdgeTypesChunkWriter<ScanEdgesByTypeChunkWriter>);

}
