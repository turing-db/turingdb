#include "EdgeCounts.h"

#include <algorithm>
#include <tuple>
#include <unordered_map>

#include "EdgeRecord.h"
#include "NodeContainer.h"
#include "indexers/EdgeIndexer.h"
#include "Profiler.h"

#include "BioAssert.h"

using namespace db;

namespace {

using EdgeTriple = std::tuple<EdgeTypeID::Type, LabelSetID::Type, LabelSetID::Type>;

struct TargetCount {
    LabelSetHandle _labelSet;
    size_t _count {0};
};

EdgeTriple tripleOf(const EdgeCount& count) {
    return EdgeTriple {count._edgeTypeID.getValue(),
                       count._sourceLabelSet.getID().getValue(),
                       count._targetLabelSet.getID().getValue()};
}

EdgeTypeID::Type typeOf(const EdgeCount& count) {
    return count._edgeTypeID.getValue();
}

void sortAndCombine(std::vector<EdgeCount>& counts) {
    std::ranges::sort(counts, std::less<EdgeTriple> {}, tripleOf);

    size_t kept = 0;
    for (const EdgeCount& count : counts) {
        if (kept != 0 && tripleOf(counts[kept - 1]) == tripleOf(count)) {
            counts[kept - 1]._count += count._count;
        } else {
            counts[kept] = count;
            kept++;
        }
    }

    counts.resize(kept);
}

void countTarget(std::vector<TargetCount>& targets, const LabelSetHandle& labelSet) {
    for (TargetCount& target : targets) {
        if (target._labelSet.getID() == labelSet.getID()) {
            target._count++;
            return;
        }
    }

    targets.push_back(TargetCount {._labelSet = labelSet, ._count = 1});
}

}

EdgeCounts::EdgeCounts()
{
}

EdgeCounts::~EdgeCounts() {
}

std::unique_ptr<EdgeCounts> EdgeCounts::create(const EdgeIndexer& indexer, const NodeContainer& nodes) {
    Profile profile("EdgeCounts::create");

    const NodeID firstNodeID = indexer.getFirstNodeID();

    // A node of an earlier part reached by an edge of this one is a patch node, whose label
    // set only the in-edge spans holding it record
    std::unordered_map<NodeID, LabelSetHandle> earlierNodeLabelSets;
    for (const auto& [labelSet, spans] : indexer.getInsByLabelSet()) {
        for (const EdgeIndexer::EdgeSpan& span : spans) {
            if (span.empty() || span.front()._nodeID >= firstNodeID) {
                continue;
            }

            for (const EdgeRecord& edge : span) {
                earlierNodeLabelSets.emplace(edge._nodeID, labelSet);
            }
        }
    }

    std::vector<EdgeCount> counts;
    std::vector<std::vector<TargetCount>> targetsByType;
    std::vector<EdgeTypeID> sourceTypes;

    for (const auto& [sourceLabelSet, spans] : indexer.getOutsByLabelSet()) {
        for (const EdgeIndexer::EdgeSpan& span : spans) {
            for (const EdgeRecord& edge : span) {
                const NodeID target = edge._otherID;
                const LabelSetHandle targetLabelSet = target >= firstNodeID
                                                    ? nodes.getNodeLabelSet(target)
                                                    : earlierNodeLabelSets.at(target);

                const size_t typeIndex = edge._edgeTypeID.getValue();
                if (typeIndex >= targetsByType.size()) {
                    targetsByType.resize(typeIndex + 1);
                }

                std::vector<TargetCount>& targets = targetsByType[typeIndex];
                if (targets.empty()) {
                    sourceTypes.push_back(edge._edgeTypeID);
                }

                countTarget(targets, targetLabelSet);
            }
        }

        for (const EdgeTypeID edgeType : sourceTypes) {
            std::vector<TargetCount>& targets = targetsByType[edgeType.getValue()];
            for (const TargetCount& target : targets) {
                counts.push_back(EdgeCount {._edgeTypeID = edgeType,
                                            ._sourceLabelSet = sourceLabelSet,
                                            ._targetLabelSet = target._labelSet,
                                            ._count = target._count});
            }

            targets.clear();
        }

        sourceTypes.clear();
    }

    std::unique_ptr<EdgeCounts> edgeCounts = std::make_unique<EdgeCounts>();
    edgeCounts->assign(counts);

    return edgeCounts;
}

void EdgeCounts::assign(std::span<const EdgeCount> counts) {
    _counts.assign(counts.begin(), counts.end());
    sortAndCombine(_counts);
}

void EdgeCounts::subtract(std::span<const EdgeCount> counts) {
    std::vector<EdgeCount> removed(counts.begin(), counts.end());
    sortAndCombine(removed);

    for (const EdgeCount& count : removed) {
        const auto entry = std::ranges::lower_bound(_counts, tripleOf(count), std::less<EdgeTriple> {}, tripleOf);
        const bool holdsTheEdges = entry != _counts.end()
                                && tripleOf(*entry) == tripleOf(count)
                                && entry->_count >= count._count;
        bioassert(holdsTheEdges, "Subtracting edges the counts do not hold");

        entry->_count -= count._count;
    }

    std::erase_if(_counts, [](const EdgeCount& count) { return count._count == 0; });
}

size_t EdgeCounts::countEdges(const LabelSetHandle& source, const LabelSetHandle& target) const {
    size_t edges = 0;
    for (const EdgeCount& count : _counts) {
        if (count._sourceLabelSet.hasAtLeastLabels(source) && count._targetLabelSet.hasAtLeastLabels(target)) {
            edges += count._count;
        }
    }

    return edges;
}

size_t EdgeCounts::countEdges(const LabelSetHandle& source, EdgeTypeID edgeType, const LabelSetHandle& target) const {
    const auto ofType = std::ranges::equal_range(_counts, edgeType.getValue(), std::less<EdgeTypeID::Type> {}, typeOf);

    size_t edges = 0;
    for (const EdgeCount& count : ofType) {
        if (count._sourceLabelSet.hasAtLeastLabels(source) && count._targetLabelSet.hasAtLeastLabels(target)) {
            edges += count._count;
        }
    }

    return edges;
}
