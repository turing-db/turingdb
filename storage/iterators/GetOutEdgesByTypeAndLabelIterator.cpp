#include "GetOutEdgesByTypeAndLabelIterator.h"

#include <iterator>

#include "columns/ColumnIDs.h"
#include "reader/GraphReader.h"
#include "EdgeTypeMatch.h"
#include "IteratorUtils.h"

using namespace db;

GetOutEdgesByTypeAndLabelChunkWriter::GetOutEdgesByTypeAndLabelChunkWriter(const GraphView& view,
                                                                           const ColumnNodeIDs* inputNodeIDs,
                                                                           std::span<const EdgeTypeID> edgeTypes,
                                                                           const LabelSetHandle& labelset)
    : GetOutEdgesIterator(view, inputNodeIDs),
    _edgeTypes(edgeTypes),
    _labelset(labelset),
    _filter(view)
{
}

void GetOutEdgesByTypeAndLabelChunkWriter::filterTombstones() {
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

void GetOutEdgesByTypeAndLabelChunkWriter::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    static constexpr auto bools = generateArray<NColumns, NCombinations>();
    static constexpr auto masks = generateBitmasks<NColumns, NCombinations>();

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

    const GraphReader reader(_view);
    const std::span<const EdgeTypeID> edgeTypes = _edgeTypes;

    const auto fill = [&]<std::array<bool, NColumns> conditions>() {
        while (isValid() && remainingToMax > 0) {
            const size_t index = std::distance(_inputNodeIDs->cbegin(), _nodeIt);
            const std::span<const EdgeID> rowExcluded = _excluded.isSet() ? _excluded.rowEdges(index) : std::span<const EdgeID> {};

            while (_edgeIt != _edges.end() && remainingToMax > 0) {
                const bool excluded = ExcludedEdges::holds(rowExcluded, _edgeIt->_edgeID);
                if (edgeTypeMatches(edgeTypes, _edgeIt->_edgeTypeID) && !excluded) {
                    const LabelSetHandle targetLabels = reader.getNodeLabelSet(_edgeIt->_otherID);

                    if (targetLabels.isValid() && targetLabels.hasAtLeastLabels(_labelset)) {
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
                }

                _edgeIt++;
            }

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

    if (_view.hasDeletedEdges()) {
        filterTombstones();
    }
}
