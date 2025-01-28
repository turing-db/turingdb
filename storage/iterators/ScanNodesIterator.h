#pragma once

#include "Iterator.h"

#include "PartIterator.h"
#include "ChunkWriter.h"
#include "EntityID.h"
#include "columns/ColumnIDs.h"

namespace db {

class ScanNodesIterator : public Iterator {
public:
    ScanNodesIterator() = default;
    explicit ScanNodesIterator(const GraphView& view);
    ~ScanNodesIterator() override;

    void reset();

    void next() override;

    EntityID get() const { return _currentNodeID; }

    inline EntityID operator*() const { return get(); }

    inline ScanNodesIterator& operator++() {
        next();
        return *this;
    }

    void fill(ColumnIDs* column, size_t maxNodes);

protected:
    EntityID _currentNodeID;
    EntityID _partEnd;

    void init();
    void nextValid();
};

class ScanNodesChunkWriter : public ScanNodesIterator {
public:
    ScanNodesChunkWriter();
    explicit ScanNodesChunkWriter(const GraphView& view);

    void setNodeIDs(ColumnIDs* nodeIDs) { _nodeIDs = nodeIDs; }

    void fill(size_t maxCount);

private:
    ColumnIDs* _nodeIDs {nullptr};
};

struct ScanNodesRange {
    GraphView _view;

    ScanNodesIterator begin() const { return ScanNodesIterator {_view}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    ScanNodesChunkWriter chunkWriter() const { return ScanNodesChunkWriter {_view}; }
};

static_assert(NodeIDsChunkWriter<ScanNodesChunkWriter>);

}
