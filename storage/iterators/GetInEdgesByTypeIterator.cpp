#include "GetInEdgesByTypeIterator.h"

#include <iterator>

#include "columns/ColumnIDs.h"
#include "IteratorUtils.h"

using namespace db;

GetInEdgesByTypeChunkWriter::GetInEdgesByTypeChunkWriter(const GraphView& view,
                                                         const ColumnNodeIDs* inputNodeIDs,
                                                         EdgeTypeID edgeType)
    : GetInEdgesIterator(view, inputNodeIDs),
    _edgeType(edgeType),
    _filter(view.tombstones())
{
}

void GetInEdgesByTypeChunkWriter::filterTombstones() {
    // Base column of this ChunkWriter is _edgeIDs
    _filter.populateRanges(_edgeIDs);

    _filter.filter(_indices);
    _filter.filter(_edgeIDs);

    if (_srcs) {
        _filter.filter(_srcs);
    }
    if (_types) {
        _filter.filter(_types);
    }

    _filter.reset();
}

static constexpr size_t NColumns = 3;
static constexpr size_t NCombinations = 1 << NColumns;

void GetInEdgesByTypeChunkWriter::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    static constexpr auto bools = generateArray<NColumns, NCombinations>();
    static constexpr auto masks = generateBitmasks<NColumns, NCombinations>();

    // The filtered chunk is at most maxCount rows, so reserving once keeps the
    // per-edge push_back below from reallocating as it appends.
    _indices->clear();
    _indices->reserve(maxCount);

    if (_edgeIDs) {
        _edgeIDs->clear();
        _edgeIDs->reserve(maxCount);
    }
    if (_srcs) {
        _srcs->clear();
        _srcs->reserve(maxCount);
    }
    if (_types) {
        _types->clear();
        _types->reserve(maxCount);
    }

    // The by-type in-edge fill mirrors GetOutEdgesByTypeChunkWriter::fill: the
    // edges of the requested type are scattered through a node's in-edge span, so
    // we walk it edge by edge and push only the matches straight into the output
    // columns, with no intermediate unfiltered chunk. For an in-edge the neighbour
    // (_otherID) is the source. The bitmask dispatch lifts the "is this column
    // wired up?" test out of the per-edge loop.
    const auto fill = [&]<std::array<bool, NColumns> conditions>() {
        while (isValid() && remainingToMax > 0) {
            const size_t index = std::distance(_inputNodeIDs->cbegin(), _nodeIt);

            // Append this node's remaining in-edges of the requested type until
            // the span is exhausted or the row budget runs out.
            while (_edgeIt != _edges.end() && remainingToMax > 0) {
                if (_edgeIt->_edgeTypeID == _edgeType) {
                    _indices->push_back(index);

                    if constexpr (conditions[0]) {
                        _edgeIDs->push_back(_edgeIt->_edgeID);
                    }
                    if constexpr (conditions[1]) {
                        _srcs->push_back(_edgeIt->_otherID);
                    }
                    if constexpr (conditions[2]) {
                        _types->push_back(_edgeIt->_edgeTypeID);
                    }

                    remainingToMax--;
                }

                _edgeIt++;
            }

            // Span exhausted: advance to the next node (or datapart) that has
            // edges. Otherwise we stopped mid-span on the row budget, so _edgeIt
            // stays put and the next fill() resumes from the same edge.
            if (_edgeIt == _edges.end()) {
                nextValid();
            }
        }
    };

    switch (bitmask::create(_edgeIDs, _srcs, _types)) {
        CASE(0);
        CASE(1);
        CASE(2);
        CASE(3);
        CASE(4);
        CASE(5);
        CASE(6);
        CASE(7);
    }

    if (_view.tombstones().hasEdges()) {
        filterTombstones();
    }
}
