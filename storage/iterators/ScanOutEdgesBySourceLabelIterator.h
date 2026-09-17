#pragma once

#include <span>

#include "Iterator.h"
#include "ChunkWriter.h"
#include "TombstoneFilter.h"
#include "datapart/EdgeRecord.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "indexers/LabelSetIndexer.h"

namespace db {

class ScanOutEdgesBySourceLabelIterator : public Iterator {
public:
    ScanOutEdgesBySourceLabelIterator() = default;
    ScanOutEdgesBySourceLabelIterator(const GraphView& view, const LabelSetHandle& labelset);
    ~ScanOutEdgesBySourceLabelIterator() override;

    void reset() {
        Iterator::reset();
        init();
    }

    void next() override;

    const EdgeRecord& get() const {
        return *_edgeIt;
    }

    ScanOutEdgesBySourceLabelIterator& operator++() {
        next();
        return *this;
    }

    const EdgeRecord& operator*() const {
        return get();
    }

protected:
    LabelSetHandle _labelset;
    using EdgeSpan = std::span<const EdgeRecord>;
    using EdgeSpans = std::vector<EdgeSpan>;
    using LabelSetIterator = LabelSetIndexer<EdgeSpans>::MatchIterator;
    LabelSetIterator _labelsetIt;

    std::span<const EdgeSpan> _spans;
    std::span<const EdgeSpan>::iterator _spanIt;
    std::span<const EdgeRecord> _edges;
    std::span<const EdgeRecord>::iterator _edgeIt;

    void init();
    void nextValid();
};

class ScanOutEdgesBySourceLabelChunkWriter : public ScanOutEdgesBySourceLabelIterator {
public:
    ScanOutEdgesBySourceLabelChunkWriter();
    ScanOutEdgesBySourceLabelChunkWriter(const GraphView& view, const LabelSetHandle& labelset);

    void fill(size_t maxCount);

    void setSrcIDs(ColumnNodeIDs* srcIDs) { _srcs = srcIDs; }
    void setEdgeIDs(ColumnEdgeIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setTgtIDs(ColumnNodeIDs* tgtIDs) { _tgts = tgtIDs; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

private:
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _srcs {nullptr};
    ColumnNodeIDs* _tgts {nullptr};
    ColumnEdgeTypes* _types {nullptr};

    TombstoneFilter _filter;

    void filterTombstones();
};

struct ScanOutEdgesBySourceLabelRange {
    GraphView _view;
    LabelSetHandle _labelset;

    ScanOutEdgesBySourceLabelIterator begin() const { return {_view, _labelset}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    ScanOutEdgesBySourceLabelChunkWriter chunkWriter() const { return ScanOutEdgesBySourceLabelChunkWriter {_view, _labelset}; }
};

static_assert(SrcIDsChunkWriter<ScanOutEdgesBySourceLabelChunkWriter>);
static_assert(EdgeIDsChunkWriter<ScanOutEdgesBySourceLabelChunkWriter>);
static_assert(TgtIDsChunkWriter<ScanOutEdgesBySourceLabelChunkWriter>);
static_assert(EdgeTypesChunkWriter<ScanOutEdgesBySourceLabelChunkWriter>);

}

