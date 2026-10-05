#pragma once

#include "GetOutEdgesIterator.h"

#include <span>

#include "ExcludedEdges.h"
#include "ID.h"
#include "metadata/LabelSetHandle.h"

namespace db {

class GetOutEdgesByTypeAndLabelChunkWriter : public GetOutEdgesIterator {
public:
    GetOutEdgesByTypeAndLabelChunkWriter() = delete;
    GetOutEdgesByTypeAndLabelChunkWriter(const GraphView& view,
                                         const ColumnNodeIDs* inputNodeIDs,
                                         std::span<const EdgeTypeID> edgeTypes,
                                         const LabelSetHandle& labelset);

    void fill(size_t maxCount);

    void setInputNodeIDs(const ColumnNodeIDs* inputNodeIDs) { _inputNodeIDs = inputNodeIDs; }
    void setIndices(ColumnVector<size_t>* indices) { _indices = indices; }
    void setEdgeIDs(ColumnEdgeIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setTgtIDs(ColumnNodeIDs* tgts) { _tgts = tgts; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

    void setExcludedEdges(const ExcludedEdges& excluded) { _excluded = excluded; }

private:
    // Borrowed, not owned: the caller keeps the types alive for the writer's lifetime
    std::span<const EdgeTypeID> _edgeTypes;
    LabelSetHandle _labelset;

    ColumnVector<size_t>* _indices {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _tgts {nullptr};
    ColumnEdgeTypes* _types {nullptr};

    TombstoneFilter _filter;
    ExcludedEdges _excluded;
    size_t _heldInRun {0};

    void filterTombstones();
};

static_assert(NonRootChunkWriter<GetOutEdgesByTypeAndLabelChunkWriter>);
static_assert(EdgeIDsChunkWriter<GetOutEdgesByTypeAndLabelChunkWriter>);
static_assert(TgtIDsChunkWriter<GetOutEdgesByTypeAndLabelChunkWriter>);
static_assert(EdgeTypesChunkWriter<GetOutEdgesByTypeAndLabelChunkWriter>);

}
