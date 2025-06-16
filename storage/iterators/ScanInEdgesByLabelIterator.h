#pragma once

#include <span>

#include "Iterator.h"
#include "ChunkWriter.h"
#include "EdgeRecord.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "indexers/EdgeIndexer.h"

namespace db {

class ScanInEdgesByLabelIterator : public Iterator {
public:
    ScanInEdgesByLabelIterator() = default;
    ScanInEdgesByLabelIterator(const GraphView& view, const LabelSetHandle& labelset);
    ~ScanInEdgesByLabelIterator() override;

    void reset() {
        Iterator::reset();
        init();
    }

    void next() override;

    const EdgeRecord& get() const {
        return *_edgeIt;
    }

    ScanInEdgesByLabelIterator& operator++() {
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

class ScanInEdgesByLabelChunkWriter : public ScanInEdgesByLabelIterator {
public:
    ScanInEdgesByLabelChunkWriter();
    ScanInEdgesByLabelChunkWriter(const GraphView& view, const LabelSetHandle& labelset);

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
};

struct ScanInEdgesByLabelRange {
    GraphView _view;
    LabelSetHandle _labelset;

    ScanInEdgesByLabelIterator begin() const { return {_view, _labelset}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    ScanInEdgesByLabelChunkWriter chunkWriter() const { return {_view, _labelset}; }
};

static_assert(SrcIDsChunkWriter<ScanInEdgesByLabelChunkWriter>);
static_assert(EdgeIDsChunkWriter<ScanInEdgesByLabelChunkWriter>);
static_assert(TgtIDsChunkWriter<ScanInEdgesByLabelChunkWriter>);
static_assert(EdgeTypesChunkWriter<ScanInEdgesByLabelChunkWriter>);

}
