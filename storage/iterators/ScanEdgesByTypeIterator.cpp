#include "ScanEdgesByTypeIterator.h"

#include "Bitmask.h"
#include "datapart/DataPart.h"
#include "datapart/EdgeContainer.h"
#include "IteratorUtils.h"
#include "TombstoneFilter.h"

#include "BioAssert.h"

using namespace db;

namespace {

constexpr size_t NColumns = 4;
constexpr size_t NCombinations = 1 << NColumns;

}

ScanEdgesByTypeChunkWriter::ScanEdgesByTypeChunkWriter(const GraphView& view, EdgeTypeID edgeType)
    : ScanEdgesIterator(view),
    _edgeType(edgeType),
    _filter(view.tombstones())
{
}

void ScanEdgesByTypeChunkWriter::filterTombstones() {
    // Base column of this ChunkWriter is _edgeIDs
    _filter.populateRanges(_edgeIDs);
    _filter.filter(_edgeIDs);

    if (_srcs) {
        _filter.filter(_srcs);
    }
    if (_tgts) {
        _filter.filter(_tgts);
    }
    if (_types) {
        _filter.filter(_types);
    }

    _filter.reset();
}

void ScanEdgesByTypeChunkWriter::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    static constexpr auto bools = generateArray<NColumns, NCombinations>();
    static constexpr auto masks = generateBitmasks<NColumns, NCombinations>();

    bioassert(_srcs || _tgts || _edgeIDs || _types,
              "ScanEdgesByTypeChunkWriter must be initialized with at least one valid column");

    // The filtered chunk is at most maxCount rows, so reserving once keeps the
    // per-edge push_back below from reallocating as it appends.
    if (_srcs) {
        _srcs->clear();
        _srcs->reserve(maxCount);
    }
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

    // Keeping one type breaks the contiguous-range fill ScanEdgesChunkWriter uses:
    // the edges of a type are scattered through the part's edge span, not a
    // sub-range of it, so there is no run to resize over. We walk the span edge by
    // edge and push only the matches, with the bitmask dispatch lifting the "is
    // this column wired up?" test out of the per-edge loop.
    const auto fill = [&]<std::array<bool, NColumns> conditions>() {
        while (isValid() && remainingToMax > 0) {
            const auto partEnd = _edges.end();

            while (_edgeIt != partEnd && remainingToMax > 0) {
                if (_edgeIt->_edgeTypeID == _edgeType) {
                    if constexpr (conditions[0]) {
                        _srcs->push_back(_edgeIt->_nodeID);
                    }
                    if constexpr (conditions[1]) {
                        _edgeIDs->push_back(_edgeIt->_edgeID);
                    }
                    if constexpr (conditions[2]) {
                        _tgts->push_back(_edgeIt->_otherID);
                    }
                    if constexpr (conditions[3]) {
                        _types->push_back(_edgeIt->_edgeTypeID);
                    }

                    remainingToMax--;
                }

                ++_edgeIt;
            }

            nextValid();
        }
    };

    switch (bitmask::create(_srcs, _edgeIDs, _tgts, _types)) {
        CASE(0);
        CASE(1);
        CASE(2);
        CASE(3);
        CASE(4);
        CASE(5);
        CASE(6);
        CASE(7);
        CASE(8);
        CASE(9);
        CASE(10);
        CASE(11);
        CASE(12);
        CASE(13);
        CASE(14);
        CASE(15);
    }

    if (_view.tombstones().hasEdges()) {
        filterTombstones();
    }
}
