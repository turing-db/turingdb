#pragma once

#include "Iterator.h"

#include "ChunkWriter.h"
#include "NodeRange.h"
#include "columns/ColumnIDs.h"
#include "indexers/LabelSetIndexer.h"
#include "metadata/LabelSetHandle.h"

namespace db {

class ScanNodesByLabelIterator : public Iterator {
public:
    ScanNodesByLabelIterator() = default;
    ScanNodesByLabelIterator(const GraphView& view, const LabelSetHandle& labelset);
    ~ScanNodesByLabelIterator() override;

    void reset() {
        Iterator::reset();
        init();
    }

    void next() override;

    NodeID get() const {
        return *_rangeIt;
    }

    ScanNodesByLabelIterator& operator++() {
        next();
        return *this;
    }

    NodeID operator*() const {
        return get();
    }

protected:
    LabelSetHandle _labelset;
    using LabelSetIterator = LabelSetIndexer<NodeRange>::MatchIterator;
    LabelSetIterator _labelsetIt;
    NodeRange _range;
    NodeRange::Iterator _rangeIt;

    void init();
    void nextValid();
};

class ScanNodesByLabelChunkWriter : public ScanNodesByLabelIterator {
public:
    ScanNodesByLabelChunkWriter();
    ScanNodesByLabelChunkWriter(const GraphView& view, const LabelSetHandle& labelset);

    void fill(size_t maxCount);

    void setNodeIDs(ColumnNodeIDs* nodeIDs) { _nodeIDs = nodeIDs; }

private:
    ColumnNodeIDs* _nodeIDs {nullptr};

    void filterTombstones();
};

struct ScanNodesByLabelRange {
    GraphView _view;
    LabelSetHandle _labelset;

    ScanNodesByLabelIterator begin() const { return ScanNodesByLabelIterator(_view, _labelset); }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    ScanNodesByLabelChunkWriter chunkWriter() const { return {_view, _labelset}; }
};

static_assert(NodeIDsChunkWriter<ScanNodesByLabelChunkWriter>);

}

