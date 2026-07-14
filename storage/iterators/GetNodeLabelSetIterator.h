#pragma once

#include "Iterator.h"

#include "ChunkWriter.h"
#include "columns/ColumnIDs.h"

namespace db {

class GetNodeLabelSetIterator : public Iterator {
public:
    GetNodeLabelSetIterator() = default;
    GetNodeLabelSetIterator(const GraphView& view, const ColumnNodeIDs* inputNodeIDs);
    ~GetNodeLabelSetIterator() override;

    bool isValid() const override {
        return _inputNodeIDs && _nodeIt != _inputNodeIDs->cend();
    }

    bool operator!=(const DataPartIterator& other) const override {
        return isValid();
    }

    void next() override;

    LabelSetID get() const;

    GetNodeLabelSetIterator& operator++() {
        next();
        return *this;
    }

    LabelSetID operator*() const {
        return get();
    }

protected:
    const ColumnNodeIDs* _inputNodeIDs {nullptr};
    ColumnNodeIDs::ConstIterator _nodeIt;
};

class GetNodeLabelSetChunkWriter : public GetNodeLabelSetIterator {
public:
    GetNodeLabelSetChunkWriter() = delete;
    GetNodeLabelSetChunkWriter(const GraphView& view, const ColumnNodeIDs* inputNodeIDs);

    void fill(size_t maxCount);

    void setLabelSetIDs(ColumnLabelSetIDs* labelSetIDs) { _labelSetIDs = labelSetIDs; }

private:
    ColumnLabelSetIDs* _labelSetIDs {nullptr};
};

struct GetNodeLabelSetRange {
    GraphView _view;
    const ColumnNodeIDs* _inputNodeIDs {nullptr};

    GetNodeLabelSetIterator begin() const { return {_view, _inputNodeIDs}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    GetNodeLabelSetChunkWriter chunkWriter() const { return {_view, _inputNodeIDs}; }
};

static_assert(LabelSetIDsChunkWriter<GetNodeLabelSetChunkWriter>);

}
