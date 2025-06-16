#pragma once

#include "Iterator.h"

#include "ChunkWriter.h"
#include "PartIterator.h"
#include "columns/ColumnIDs.h"
#include "views/NodeView.h"

namespace db {

class GetNodeViewsIterator : public Iterator {
public:
    GetNodeViewsIterator() = default;
    GetNodeViewsIterator(const GetNodeViewsIterator&) = default;
    GetNodeViewsIterator(GetNodeViewsIterator&&) noexcept = default;
    GetNodeViewsIterator& operator=(const GetNodeViewsIterator&) = default;
    GetNodeViewsIterator& operator=(GetNodeViewsIterator&&) noexcept = default;

    GetNodeViewsIterator(const GraphView& view,
                        const ColumnNodeIDs* inputNodeIDs);
    ~GetNodeViewsIterator() override;

    void next() override;

    const NodeView& get() const {
        return _nodeView;
    }

    GetNodeViewsIterator& operator++() {
        next();
        return *this;
    }

    const NodeView& operator*() const {
        return get();
    }

protected:
    const ColumnNodeIDs* _inputNodeIDs {nullptr};
    ColumnNodeIDs::ConstIterator _nodeIt;
    ColumnNodeIDs::ConstIterator _nodeItEnd;
    NodeView _nodeView;
};

class GetNodeViewsChunkWriter : public GetNodeViewsIterator {
public:
    GetNodeViewsChunkWriter() = default;
    GetNodeViewsChunkWriter(const GraphView& view,
                           const ColumnNodeIDs* inputNodeIDs);

    void fill(size_t chunkSize);

    void setIndices(ColumnVector<size_t>* indices) { _indices = indices; }
    void setNodeViewsColumn(ColumnVector<NodeView>* nodeViews) { _nodeViews = nodeViews; }

private:
    ColumnVector<size_t>* _indices {nullptr};
    ColumnVector<NodeView>* _nodeViews {nullptr};
};

struct GetNodeViewsRange {
    GraphView _dbView;
    const ColumnNodeIDs* _inputNodeIDs {nullptr};

    GetNodeViewsIterator begin() const { return {_dbView, _inputNodeIDs}; }
    DataPartIterator end() const { return PartIterator(_dbView).getEndIterator(); }
    GetNodeViewsChunkWriter chunkWriter() const { return {_dbView, _inputNodeIDs}; }
};

static_assert(NonRootChunkWriter<GetNodeViewsChunkWriter>);
static_assert(NodeViewChunkWriter<GetNodeViewsChunkWriter>);

}

