#include "ReversedPathExplorator.h"

#include <algorithm>

#include "PathTargetIndex.h"
#include "datapart/NodeContainer.h"
#include "metadata/LabelSetHandle.h"

using namespace db;

namespace {

bool carriesLabels(const PartDirectory& parts, NodeID node, const LabelSet& labels) {
    const size_t owner = parts.ownerIndex(node);
    if (owner == parts.size()) {
        return false;
    }

    const LabelSetHandle nodeLabels = parts.get(owner)._nodes->getNodeLabelSet(node);

    return nodeLabels.isValid() && nodeLabels.hasAtLeastLabels(LabelSetHandle(labels));
}

}

ReversedPathExplorator::ReversedPathExplorator(const GraphView& view,
                                               const ColumnNodeIDs* inputNodeIDs,
                                               std::span<const NodeID> endNodeSet,
                                               const LabelSet* endLabels,
                                               PathExplorationDir direction,
                                               uint64_t minHops,
                                               uint64_t maxHops)
    : _explorator(view, &_ends, reverseOf(direction), minHops, maxHops)
{
    const PartDirectory parts(view);
    for (const NodeID end : endNodeSet) {
        if (!endLabels || carriesLabels(parts, end, *endLabels)) {
            _ends.push_back(end);
        }
    }

    const std::vector<NodeID>& seeds = inputNodeIDs->getRaw();
    _seedRows.resize(seeds.size());
    for (size_t row = 0; row < seeds.size(); row++) {
        _seedRows[row] = SeedRow {._node = seeds[row], ._row = row};
    }
    std::stable_sort(_seedRows.begin(), _seedRows.end(), isBefore);

    _seedNodes.assign(seeds.begin(), seeds.end());
    std::sort(_seedNodes.begin(), _seedNodes.end());
    _seedNodes.erase(std::unique(_seedNodes.begin(), _seedNodes.end()), _seedNodes.end());

    _explorator.setIndices(&_walkIndices);
    _explorator.setTargets(&_walkTargets);
    _explorator.setEndNodeSet(_seedNodes);
    _explorator.reset();
}

ReversedPathExplorator::~ReversedPathExplorator() {
}

bool ReversedPathExplorator::isCheaper(const PartDirectory& parts,
                                       PathExplorationDir direction,
                                       std::span<const EdgeTypeID> edgeTypes,
                                       std::span<const NodeID> seeds,
                                       std::span<const NodeID> endNodeSet,
                                       uint64_t maxHops,
                                       PathDistanceIndex::SeedExpansion& fromEnds) {
    PathDistanceIndex::SeedExpansion fromSeeds;
    PathDistanceIndex::sampleSeedExpansion(parts, direction, edgeTypes, seeds, fromSeeds);

    PathDistanceIndex::sampleSeedExpansion(parts, reverseOf(direction), edgeTypes, endNodeSet, fromEnds);

    const double seedChecks = PathDistanceIndex::estimatedEnumerationChecks(parts, fromSeeds, seeds.size(), maxHops);
    const double endChecks = PathDistanceIndex::estimatedEnumerationChecks(parts, fromEnds, endNodeSet.size(), maxHops);

    return endChecks < seedChecks;
}

void ReversedPathExplorator::setEdgeTypeFilter(std::span<const EdgeTypeID> edgeTypes) {
    _explorator.setEdgeTypeFilter(edgeTypes);
}

void ReversedPathExplorator::setDistinctEnds(bool distinct) {
    _explorator.setDistinctEnds(distinct);
}

void ReversedPathExplorator::setTargetIndex(const PathTargetIndex* index) {
    _explorator.setTargetIndex(index);
}

bool ReversedPathExplorator::isValid() const {
    return _walkRow < _walkIndices.size() || _explorator.isValid();
}

bool ReversedPathExplorator::isBefore(const SeedRow& lhs, const SeedRow& rhs) {
    return lhs._node < rhs._node;
}

void ReversedPathExplorator::startWalkRow() {
    const SeedRow reached {._node = _walkTargets[_walkRow]};
    const auto rowsOfSeed = std::equal_range(_seedRows.begin(), _seedRows.end(), reached, isBefore);

    _seedRowCursor = rowsOfSeed.first - _seedRows.begin();
    _seedRowEnd = rowsOfSeed.second - _seedRows.begin();
}

void ReversedPathExplorator::fill(size_t maxCount) {
    std::vector<size_t>& indices = _indices->getRaw();
    indices.resize(maxCount);

    std::vector<NodeID>* targets = _targets ? &_targets->getRaw() : nullptr;
    if (targets) {
        targets->resize(maxCount);
    }

    size_t written = 0;
    while (written < maxCount) {
        if (_walkRow == _walkIndices.size()) {
            if (!_explorator.isValid()) {
                break;
            }

            _explorator.fill(maxCount);
            _walkRow = 0;
            if (_walkIndices.empty()) {
                continue;
            }

            startWalkRow();
        }

        const NodeID end = _ends[_walkIndices[_walkRow]];
        for (; _seedRowCursor < _seedRowEnd && written < maxCount; _seedRowCursor++) {
            indices[written] = _seedRows[_seedRowCursor]._row;
            if (targets) {
                (*targets)[written] = end;
            }
            written++;
        }

        if (_seedRowCursor == _seedRowEnd) {
            _walkRow++;
            if (_walkRow < _walkIndices.size()) {
                startWalkRow();
            }
        }
    }

    indices.resize(written);
    if (targets) {
        targets->resize(written);
    }
}
