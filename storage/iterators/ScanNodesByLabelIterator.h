#pragma once

#include "Iterator.h"

#include "ChunkWriter.h"
#include "NodeRange.h"
#include "columns/ColumnIDs.h"
#include "indexers/LabelSetIndexer.h"

namespace db {

class ScanNodesByLabelIterator : public Iterator {
public:
    ScanNodesByLabelIterator() = default;
    ScanNodesByLabelIterator(const GraphView& view, const LabelSet* labelset);
    ~ScanNodesByLabelIterator() override;

    void reset() {
        Iterator::reset();
        init();
    }

    void next() override;

    EntityID get() const {
        return *_rangeIt;
    }

    ScanNodesByLabelIterator& operator++() {
        next();
        return *this;
    }

    EntityID operator*() const {
        return get();
    }

protected:
    const LabelSet* _labelset {nullptr};
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
    ScanNodesByLabelChunkWriter(const GraphView& view, const LabelSet* labelset);

    void fill(size_t maxCount);

    void setNodeIDs(ColumnIDs* nodeIDs) { _nodeIDs = nodeIDs; }

private:
    ColumnIDs* _nodeIDs {nullptr};
};

struct ScanNodesByLabelRange {
    GraphView _view;
    const LabelSet* _labelset {nullptr};

    ScanNodesByLabelIterator begin() const { return ScanNodesByLabelIterator(_view, _labelset); }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    ScanNodesByLabelChunkWriter chunkWriter() const { return {_view, _labelset}; }
};

static_assert(NodeIDsChunkWriter<ScanNodesByLabelChunkWriter>);

}
