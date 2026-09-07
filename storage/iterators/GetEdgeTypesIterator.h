#pragma once

#include "Iterator.h"

#include "ChunkWriter.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"

namespace db {

class GetEdgeTypesIterator : public Iterator {
public:
    GetEdgeTypesIterator() = default;
    GetEdgeTypesIterator(const GraphView& view, const ColumnEdgeIDs* inputEdgeIDs);
    ~GetEdgeTypesIterator() override;

    bool isValid() const override {
        return _inputEdgeIDs && _edgeIt != _inputEdgeIDs->cend();
    }

    bool operator!=(const DataPartIterator& other) const override {
        return isValid();
    }

    void next() override;

    EdgeTypeID get() const;

    GetEdgeTypesIterator& operator++() {
        next();
        return *this;
    }

    EdgeTypeID operator*() const {
        return get();
    }

protected:
    const ColumnEdgeIDs* _inputEdgeIDs {nullptr};
    ColumnEdgeIDs::ConstIterator _edgeIt;
};

class GetEdgeTypesChunkWriter : public GetEdgeTypesIterator {
public:
    GetEdgeTypesChunkWriter() = delete;
    GetEdgeTypesChunkWriter(const GraphView& view, const ColumnEdgeIDs* inputEdgeIDs);

    void fill(size_t maxCount);

    void setEdgeTypes(ColumnEdgeTypes* edgeTypes) { _edgeTypes = edgeTypes; }

private:
    ColumnEdgeTypes* _edgeTypes {nullptr};
};

struct GetEdgeTypesRange {
    GraphView _view;
    const ColumnEdgeIDs* _inputEdgeIDs {nullptr};

    GetEdgeTypesIterator begin() const { return {_view, _inputEdgeIDs}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    GetEdgeTypesChunkWriter chunkWriter() const { return {_view, _inputEdgeIDs}; }
};

static_assert(EdgeTypesChunkWriter<GetEdgeTypesChunkWriter>);

}
