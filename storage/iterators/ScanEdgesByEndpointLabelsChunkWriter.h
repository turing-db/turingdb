#pragma once

#include "ScanOutEdgesBySourceLabelIterator.h"
#include "ScanInEdgesByTargetLabelIterator.h"
#include "ChunkWriter.h"
#include "TombstoneFilter.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "metadata/LabelSetHandle.h"

namespace db {

// Walks the edges a by-label edge index holds under one end's label set and keeps those
// whose other end carries at least _otherLabels. IndexIterator picks the index, so the
// node column it fills through setSrcIDs is the indexed end and setTgtIDs the other end.
template <typename IndexIterator>
class ScanEdgesByEndpointLabelsChunkWriter : public IndexIterator {
public:
    ScanEdgesByEndpointLabelsChunkWriter(const GraphView& view,
                                         const LabelSetHandle& indexedLabels,
                                         const LabelSetHandle& otherLabels);
    ~ScanEdgesByEndpointLabelsChunkWriter() override;

    void fill(size_t maxCount);

    void setSrcIDs(ColumnNodeIDs* srcIDs) { _srcs = srcIDs; }
    void setEdgeIDs(ColumnEdgeIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setTgtIDs(ColumnNodeIDs* tgtIDs) { _tgts = tgtIDs; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

private:
    LabelSetHandle _otherLabels;

    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _srcs {nullptr};
    ColumnNodeIDs* _tgts {nullptr};
    ColumnEdgeTypes* _types {nullptr};

    TombstoneFilter _filter;

    void filterTombstones();
};

using ScanOutEdgesByBothLabelsChunkWriter = ScanEdgesByEndpointLabelsChunkWriter<ScanOutEdgesBySourceLabelIterator>;
using ScanInEdgesByBothLabelsChunkWriter = ScanEdgesByEndpointLabelsChunkWriter<ScanInEdgesByTargetLabelIterator>;

static_assert(SrcIDsChunkWriter<ScanOutEdgesByBothLabelsChunkWriter>);
static_assert(EdgeIDsChunkWriter<ScanOutEdgesByBothLabelsChunkWriter>);
static_assert(TgtIDsChunkWriter<ScanOutEdgesByBothLabelsChunkWriter>);
static_assert(EdgeTypesChunkWriter<ScanOutEdgesByBothLabelsChunkWriter>);
static_assert(SrcIDsChunkWriter<ScanInEdgesByBothLabelsChunkWriter>);
static_assert(EdgeIDsChunkWriter<ScanInEdgesByBothLabelsChunkWriter>);
static_assert(TgtIDsChunkWriter<ScanInEdgesByBothLabelsChunkWriter>);
static_assert(EdgeTypesChunkWriter<ScanInEdgesByBothLabelsChunkWriter>);

}
