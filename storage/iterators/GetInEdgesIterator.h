#pragma once

#include <span>

#include "Iterator.h"

#include "ChunkWriter.h"
#include "EdgeRecord.h"
#include "PartIterator.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"

namespace db {

class GetInEdgesIterator : public Iterator {
public:
    GetInEdgesIterator() = default;
    GetInEdgesIterator(const GetInEdgesIterator&) = default;
    GetInEdgesIterator(GetInEdgesIterator&&) noexcept = default;
    GetInEdgesIterator& operator=(const GetInEdgesIterator&) = default;
    GetInEdgesIterator& operator=(GetInEdgesIterator&&) noexcept = default;

    GetInEdgesIterator(const GraphView& view, const ColumnIDs* inputNodeIDs);
    ~GetInEdgesIterator() override;

    void reset();

    void next() override;

    const EdgeRecord& get() const {
        return *_edgeIt;
    }

    GetInEdgesIterator& operator++() {
        next();
        return *this;
    }

    const EdgeRecord& operator*() const {
        return get();
    }

protected:
    const ColumnIDs* _inputNodeIDs {nullptr};
    ColumnIDs::ConstIterator _nodeIt;

    std::span<const EdgeRecord> _edges;
    std::span<const EdgeRecord>::iterator _edgeIt;

    void init();
    void nextValid();
};

class GetInEdgesChunkWriter : public GetInEdgesIterator {
public:
    GetInEdgesChunkWriter() = default;
    GetInEdgesChunkWriter(const GraphView& view, const ColumnIDs* inputNodeIDs);

    void fill(size_t maxCount);

    void setInputNodeIDs(const ColumnIDs* inputNodeIDs) { _inputNodeIDs = inputNodeIDs; }
    void setIndices(ColumnVector<size_t>* indices) { _indices = indices; }
    void setEdgeIDs(ColumnIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setTgtIDs(ColumnIDs* tgts) { _tgts = tgts; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

private:
    ColumnVector<size_t>* _indices {nullptr};
    ColumnIDs* _edgeIDs {nullptr};
    ColumnIDs* _tgts {nullptr};
    ColumnEdgeTypes* _types {nullptr};
};


struct GetInEdgesRange {
    GraphView _view;
    const ColumnIDs* _inputNodeIDs {nullptr};

    GetInEdgesIterator begin() const { return {_view, _inputNodeIDs}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    GetInEdgesChunkWriter chunkWriter() const { return {_view, _inputNodeIDs}; }
};

static_assert(NonRootChunkWriter<GetInEdgesChunkWriter>);
static_assert(EdgeIDsChunkWriter<GetInEdgesChunkWriter>);
static_assert(TgtIDsChunkWriter<GetInEdgesChunkWriter>);
static_assert(EdgeTypesChunkWriter<GetInEdgesChunkWriter>);

}
