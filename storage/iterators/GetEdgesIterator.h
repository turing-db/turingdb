#pragma once

#include "Iterator.h"

#include <array>
#include <span>

#include "ChunkWriter.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "datapart/EdgeRecord.h"
#include "PartIterator.h"
#include "ExcludedEdges.h"
#include "TombstoneFilter.h"

namespace db {

class GetEdgesIterator : public Iterator {
public:
    enum class Direction : uint8_t {
        Outgoing = 0,
        Incoming
    };

    GetEdgesIterator() = default;
    GetEdgesIterator(const GetEdgesIterator&) = default;
    GetEdgesIterator(GetEdgesIterator&&) noexcept = default;
    GetEdgesIterator& operator=(const GetEdgesIterator&) = default;
    GetEdgesIterator& operator=(GetEdgesIterator&&) noexcept = default;

    GetEdgesIterator(const GraphView& view, const ColumnNodeIDs* inputNodeIDs);
    ~GetEdgesIterator() override;

    void reset();

    void next() override;

    const EdgeRecord& get() const {
        return *_edgeIt;
    }

    /**
     * @brief Sets @ref _partIt to point to the first valid datapart in the range
     * [partIdx, end]. Always leaves @ref _partIt pointing to a valid datapart, or
     * pointing to the end iterator.
     */
    void goToPart(size_t partIdx);

    GetEdgesIterator& operator++() {
        next();
        return *this;
    }

    const EdgeRecord& operator*() const {
        return get();
    }

protected:
    const ColumnNodeIDs* _inputNodeIDs {nullptr};
    ColumnNodeIDs::ConstIterator _nodeIt;

    std::span<const EdgeRecord> _edges;
    std::span<const EdgeRecord> _nodeOutEdges;
    std::span<const EdgeRecord>::iterator _edgeIt;
    Direction _direction {Direction::Outgoing};

    void init();
    void nextValid();

    /**
     * @brief Advances @ref _partIt by at least @param n steps, or precisely
     * d = distance(_partIt._it, _partIt._itEnd) steps if @ref n > d. Always leaves @ref
     * _partIt pointing to a valid datapart, or pointing to the end iterator.
     */
    void advancePartIterator(size_t n);
};

class GetEdgesChunkWriter : public GetEdgesIterator {
public:
    GetEdgesChunkWriter() = delete;
    GetEdgesChunkWriter(const GraphView& view, const ColumnNodeIDs* inputNodeIDs);

    void fill(size_t maxCount);

    void setInputNodeIDs(const ColumnNodeIDs* inputNodeIDs) { _inputNodeIDs = inputNodeIDs; }
    void setIndices(ColumnVector<size_t>* indices) { _indices = indices; }
    void setEdgeIDs(ColumnEdgeIDs* edgeIDs) { _edgeIDs = edgeIDs; }
    void setOtherIDs(ColumnNodeIDs* others) { _others = others; }
    void setEdgeTypes(ColumnEdgeTypes* types) { _types = types; }

    // The edges each input row of the hop may not repeat
    void setExcludedEdges(const ExcludedEdges& excluded) { _excluded = excluded; }

private:
    ColumnVector<size_t>* _indices {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _others {nullptr};
    ColumnEdgeTypes* _types {nullptr};

    TombstoneFilter _filter;
    ExcludedEdges _excluded;

    const DataPart* _boundPart {nullptr};
    std::span<const EdgeRecord> _partOutEdges;
    EdgeID _partFirstEdgeID {0};
    std::span<const size_t> _partSelfLoopOffsets;
    size_t _heldInOutRun {0};
    size_t _heldInInRun {0};
    size_t _heldInRun {0};

    void filterTombstones();
    void classifyRow(std::span<const EdgeID> excluded);

    template <std::array<bool, 3> conditions>
    size_t copyRunLeavingOut(std::span<const EdgeID> excluded, size_t begin, size_t count);
};

struct GetEdgesRange {
    GraphView _view;
    const ColumnNodeIDs* _inputNodeIDs {nullptr};

    GetEdgesIterator begin() const { return {_view, _inputNodeIDs}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    GetEdgesChunkWriter chunkWriter() const { return {_view, _inputNodeIDs}; }
};

static_assert(NonRootChunkWriter<GetEdgesChunkWriter>);
static_assert(EdgeIDsChunkWriter<GetEdgesChunkWriter>);
static_assert(OtherIDsChunkWriter<GetEdgesChunkWriter>);
static_assert(EdgeTypesChunkWriter<GetEdgesChunkWriter>);

}
