#pragma once

#include "GetInEdgesIterator.h"

#include <span>

#include "ExcludedEdges.h"
#include "ID.h"
#include "metadata/LabelSetHandle.h"

namespace db {

class GetInEdgesByTypeAndLabelChunkWriter : public GetInEdgesIterator {
public:
    GetInEdgesByTypeAndLabelChunkWriter() = delete;
    GetInEdgesByTypeAndLabelChunkWriter(const GraphView& view,
                                        const ColumnNodeIDs* inputNodeIDs,
                                        std::span<const EdgeTypeID> edgeTypes,
                                        const LabelSetHandle& labelset);

    void fill(size_t maxCount);

    void setInputNodeIDs(const ColumnNodeIDs* inputNodeIDs) { _inputNodeIDs = inputNodeIDs; }
    void setIndices(ColumnVector<size_t>* indices) { _indices = indices; }
    void setEdgeIDs(ColumnEdgeIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setSrcIDs(ColumnNodeIDs* srcs) { _srcs = srcs; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

    void setExcludedEdges(const ExcludedEdges& excluded) { _excluded = excluded; }

private:
    // Borrowed, not owned: the caller keeps the types alive for the writer's lifetime
    std::span<const EdgeTypeID> _edgeTypes;
    LabelSetHandle _labelset;

    ColumnVector<size_t>* _indices {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _srcs {nullptr};
    ColumnEdgeTypes* _types {nullptr};

    TombstoneFilter _filter;
    ExcludedEdges _excluded;
    size_t _heldInRun {0};

    void filterTombstones();
};

static_assert(NonRootChunkWriter<GetInEdgesByTypeAndLabelChunkWriter>);
static_assert(EdgeIDsChunkWriter<GetInEdgesByTypeAndLabelChunkWriter>);
static_assert(SrcIDsChunkWriter<GetInEdgesByTypeAndLabelChunkWriter>);
static_assert(EdgeTypesChunkWriter<GetInEdgesByTypeAndLabelChunkWriter>);

}
