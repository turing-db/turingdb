#include "GetOutEdgesByTypeIterator.h"

#include <iterator>

#include "columns/ColumnIDs.h"
#include "IteratorUtils.h"

using namespace db;

GetOutEdgesByTypeChunkWriter::GetOutEdgesByTypeChunkWriter(const GraphView& view,
                                                           const ColumnNodeIDs* inputNodeIDs,
                                                           EdgeTypeID edgeType)
    : GetOutEdgesIterator(view, inputNodeIDs),
    _edgeType(edgeType),
    _filter(view.tombstones())
{
}

void GetOutEdgesByTypeChunkWriter::filterTombstones() {
    // Base column of this ChunkWriter is _edgeIDs
    _filter.populateRanges(_edgeIDs);

    _filter.filter(_edgeIDs);
    _filter.filter(_indices);

    if (_tgts) {
        _filter.filter(_tgts);
    }
    if (_types) {
        _filter.filter(_types);
    }

    _filter.reset();
}

static constexpr size_t NColumns = 3;
static constexpr size_t NCombinations = 1 << NColumns;

void GetOutEdgesByTypeChunkWriter::fill(size_t maxCount) {
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
    if (_tgts) {
        _tgts->clear();
        _tgts->reserve(maxCount);
    }
    if (_types) {
        _types->clear();
        _types->reserve(maxCount);
    }

    // Filtering by edge type breaks the contiguous-range fill the unfiltered
    // GetOutEdgesChunkWriter uses: a node's edges of the requested type are
    // scattered through its edge span, not a sub-range of it, so there is no run
    // to std::generate over. We walk the span edge by edge and push only the
    // matches - directly into the output columns, with no intermediate unfiltered
    // chunk. The bitmask dispatch (as in GetOutEdgesChunkWriter::fill) lifts the
    // "is this column wired up?" test out of the per-edge loop: one template
    // instantiation per column combination, selected once by the switch below.
    const auto fill = [&]<std::array<bool, NColumns> conditions>() {
        while (isValid() && remainingToMax > 0) {
            const size_t index = std::distance(_inputNodeIDs->cbegin(), _nodeIt);

            // Append this node's remaining edges of the requested type until the
            // span is exhausted or the row budget runs out.
            while (_edgeIt != _edges.end() && remainingToMax > 0) {
                if (_edgeIt->_edgeTypeID == _edgeType) {
                    _indices->push_back(index);

                    if constexpr (conditions[0]) {
                        _edgeIDs->push_back(_edgeIt->_edgeID);
                    }
                    if constexpr (conditions[1]) {
                        _tgts->push_back(_edgeIt->_otherID);
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

    switch (bitmask::create(_edgeIDs, _tgts, _types)) {
        CASE(0);
        CASE(1);
        CASE(2);
        CASE(3);
        CASE(4);
        CASE(5);
        CASE(6);
        CASE(7);
    }

    // Base column is _edgeIDs: only need to check if there are edge tombstones
    if (_view.tombstones().hasEdges()) {
        filterTombstones();
    }
}
