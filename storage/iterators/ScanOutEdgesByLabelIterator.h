#pragma once

#include <span>

#include "Iterator.h"
#include "ChunkWriter.h"
#include "EdgeRecord.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "indexers/LabelSetIndexer.h"

namespace db {

class ScanOutEdgesByLabelIterator : public Iterator {
public:
    ScanOutEdgesByLabelIterator() = default;
    ScanOutEdgesByLabelIterator(const GraphView& view, const LabelSet* labelset);
    ~ScanOutEdgesByLabelIterator() override;

    void reset() {
        Iterator::reset();
        init();
    }

    void next() override;

    const EdgeRecord& get() const {
        return *_edgeIt;
    }

    ScanOutEdgesByLabelIterator& operator++() {
        next();
        return *this;
    }

    const EdgeRecord& operator*() const {
        return get();
    }

protected:
    const LabelSet* _labelset {nullptr};
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

class ScanOutEdgesByLabelChunkWriter : public ScanOutEdgesByLabelIterator {
public:
    ScanOutEdgesByLabelChunkWriter();
    ScanOutEdgesByLabelChunkWriter(const GraphView& view, const LabelSet* labelset);

    void fill(size_t maxCount);

    void setSrcIDs(ColumnIDs* srcIDs) { _srcs = srcIDs; }
    void setEdgeIDs(ColumnIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setTgtIDs(ColumnIDs* tgtIDs) { _tgts = tgtIDs; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

private:
    ColumnIDs* _edgeIDs {nullptr};
    ColumnIDs* _srcs {nullptr};
    ColumnIDs* _tgts {nullptr};
    ColumnEdgeTypes* _types {nullptr};
};

struct ScanOutEdgesByLabelRange {
    GraphView _view;
    const LabelSet* _labelset {nullptr};

    ScanOutEdgesByLabelIterator begin() const { return {_view, _labelset}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    ScanOutEdgesByLabelChunkWriter chunkWriter() const { return ScanOutEdgesByLabelChunkWriter {_view, _labelset}; }
};

static_assert(SrcIDsChunkWriter<ScanOutEdgesByLabelChunkWriter>);
static_assert(EdgeIDsChunkWriter<ScanOutEdgesByLabelChunkWriter>);
static_assert(TgtIDsChunkWriter<ScanOutEdgesByLabelChunkWriter>);
static_assert(EdgeTypesChunkWriter<ScanOutEdgesByLabelChunkWriter>);

}
