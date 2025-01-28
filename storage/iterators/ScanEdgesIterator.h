#pragma once

#include "Iterator.h"

#include "ChunkWriter.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "EdgeRecord.h"
#include "PartIterator.h"

#include <span>

namespace db {

class ScanEdgesIterator : public Iterator {
public:
    ScanEdgesIterator() = default;
    ScanEdgesIterator(const ScanEdgesIterator&) = default;
    ScanEdgesIterator(ScanEdgesIterator&&) noexcept = default;
    ScanEdgesIterator& operator=(const ScanEdgesIterator&) = default;
    ScanEdgesIterator& operator=(ScanEdgesIterator&&) noexcept = default;

    explicit ScanEdgesIterator(const GraphView& view);

    ~ScanEdgesIterator() override;

    void reset();

    void next() override;

    const EdgeRecord& get() const {
        return *_edgeIt;
    }

    ScanEdgesIterator& operator++() {
        next();
        return *this;
    }

    const EdgeRecord& operator*() const {
        return get();
    }

protected:
    std::span<const EdgeRecord> _edges;
    std::span<const EdgeRecord>::iterator _edgeIt;

    void init();
    void nextValid();
};

class ScanEdgesChunkWriter : public ScanEdgesIterator {
public:
    ScanEdgesChunkWriter();
    explicit ScanEdgesChunkWriter(const GraphView& view);

    void fill(size_t maxCount);

    void setSrcIDs(ColumnIDs* srcs) { _srcs = srcs; }
    void setEdgeIDs(ColumnIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setTgtIDs(ColumnIDs* tgts) { _tgts = tgts; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

private:
    ColumnIDs* _srcs {nullptr};
    ColumnIDs* _edgeIDs {nullptr};
    ColumnIDs* _tgts {nullptr};
    ColumnEdgeTypes* _types {nullptr};
};

struct ScanEdgesRange {
    GraphView _view;

    ScanEdgesIterator begin() const { return ScanEdgesIterator {_view}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    ScanEdgesChunkWriter chunkWriter() const { return ScanEdgesChunkWriter {_view}; }
};

static_assert(SrcIDsChunkWriter<ScanEdgesChunkWriter>);
static_assert(EdgeIDsChunkWriter<ScanEdgesChunkWriter>);
static_assert(TgtIDsChunkWriter<ScanEdgesChunkWriter>);
static_assert(EdgeTypesChunkWriter<ScanEdgesChunkWriter>);

}
