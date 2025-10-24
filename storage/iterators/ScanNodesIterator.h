#pragma once

#include "Iterator.h"

#include "PartIterator.h"
#include "ChunkWriter.h"
#include "ID.h"
#include "columns/ColumnIDs.h"

namespace db {

class ScanNodesIterator : public Iterator {
public:
    ScanNodesIterator() = default;
    explicit ScanNodesIterator(const GraphView& view);
    ~ScanNodesIterator() override;

    void reset();

    void next() override;

    NodeID get() const { return _currentNodeID; }

    inline NodeID operator*() const { return get(); }

    inline ScanNodesIterator& operator++() {
        next();
        return *this;
    }

    void fill(ColumnNodeIDs* column, size_t maxNodes);

protected:
    NodeID _currentNodeID;
    NodeID _partEnd;

    void init();
    void nextValid();
};

class ScanNodesChunkWriter : public ScanNodesIterator {
public:
    ScanNodesChunkWriter();
    explicit ScanNodesChunkWriter(const GraphView& view);

    void setNodeIDs(ColumnNodeIDs* nodeIDs) { _nodeIDs = nodeIDs; }

    void fill(size_t maxCount);

private:
    ColumnNodeIDs* _nodeIDs {nullptr};

    void filterTombstones();
};

struct ScanNodesRange {
    GraphView _view;

    ScanNodesIterator begin() const { return ScanNodesIterator {_view}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    ScanNodesChunkWriter chunkWriter() const { return ScanNodesChunkWriter {_view}; }
};

static_assert(NodeIDsChunkWriter<ScanNodesChunkWriter>);

}

