#include "SchemaGraph.h"

#include <algorithm>

#include "datapart/DataPart.h"
#include "datapart/EdgeContainer.h"
#include "datapart/EdgeRecord.h"
#include "datapart/NodeContainer.h"
#include "metadata/EdgeTypeMap.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/LabelSetMap.h"

#include "BioAssert.h"

using namespace db;

namespace {

// The edges of one cell of the build table, a source label set over one type, by target
struct TargetCounts {
    LabelSetID _target {0};
    size_t _count {0};
    size_t _selfLoopCount {0};
};

LabelSetID findNodeLabelSet(DataPartSpan parts, NodeID node) {
    const auto startsAtOrBefore = [node](const WeakArc<DataPart>& arc) {
        return arc.get()->getFirstNodeID() <= node;
    };

    const DataPartIterator next = std::ranges::partition_point(parts, startsAtOrBefore);
    bioassert(next != parts.begin(), "Node {} is in no part", node.getValue());

    const DataPart* part = std::prev(next)->get();
    const LabelSetHandle labelSet = part->nodes().getNodeLabelSet(node);
    bioassert(labelSet.isValid(), "Node {} is in no part", node.getValue());

    return labelSet.getID();
}

constexpr size_t embeddingArcVisitBudget = 200000;

// A depth-first search for the embedding, one pattern edge at a time, taking next the edge
// with the most ends already placed so that each step scans the arcs at a placed node
class SchemaEmbedding {
public:
    SchemaEmbedding(std::span<const SchemaArc> arcs, const SchemaPattern& pattern)
        : _arcs(arcs),
        _pattern(pattern)
    {
    }

    void build(const LabelSetMap& labelSets) {
        const size_t nodeCount = _pattern._nodes.size();
        _candidates.resize(nodeCount);
        _placed.resize(nodeCount, LabelSetID {0});
        _isPlaced.resize(nodeCount, false);
        _done.resize(_pattern._edges.size(), false);

        for (size_t node = 0; node < nodeCount; node++) {
            for (const LabelSetMap::Pair& pair : labelSets) {
                if (pair._value->hasAtLeastLabels(_pattern._nodes[node]._labels)) {
                    _candidates[node].push_back(pair._id);
                }
            }
        }
    }

    bool run() {
        const bool everyNodePlaceable = std::ranges::none_of(_candidates, [](const std::vector<LabelSetID>& candidates) {
            return candidates.empty();
        });
        if (!everyNodePlaceable) {
            return false;
        }

        return extend();
    }

private:
    std::span<const SchemaArc> _arcs;
    const SchemaPattern& _pattern;
    std::vector<std::vector<LabelSetID>> _candidates;
    std::vector<LabelSetID> _placed;
    std::vector<bool> _isPlaced;
    std::vector<bool> _done;
    size_t _visits {0};

    size_t placedEndCount(const SchemaPattern::Edge& edge) const {
        return (_isPlaced[edge._source] ? 1 : 0) + (_isPlaced[edge._target] ? 1 : 0);
    }

    size_t nextEdge() const {
        const size_t edgeCount = _pattern._edges.size();
        size_t best = edgeCount;
        size_t bestPlaced = 0;

        for (size_t index = 0; index < edgeCount; index++) {
            if (_done[index]) {
                continue;
            }

            const size_t placed = placedEndCount(_pattern._edges[index]);
            if (best == edgeCount || placed > bestPlaced) {
                best = index;
                bestPlaced = placed;
            }
        }

        return best;
    }

    bool admits(size_t node, LabelSetID labelSet) const {
        if (_isPlaced[node]) {
            return _placed[node] == labelSet;
        }

        return std::ranges::find(_candidates[node], labelSet) != _candidates[node].end();
    }

    bool fits(const SchemaArc& arc, size_t source, size_t target) const {
        if (source == target) {
            const bool closesOnItself = arc._source == arc._target && arc._selfLoopCount > 0;
            return closesOnItself && admits(source, arc._source);
        }

        return admits(source, arc._source) && admits(target, arc._target);
    }

    bool place(const SchemaArc& arc, size_t source, size_t target) {
        const bool placesSource = !_isPlaced[source];
        if (placesSource) {
            _placed[source] = arc._source;
            _isPlaced[source] = true;
        }

        const bool placesTarget = !_isPlaced[target];
        if (placesTarget) {
            _placed[target] = arc._target;
            _isPlaced[target] = true;
        }

        const bool embedded = extend();

        if (placesTarget) {
            _isPlaced[target] = false;
        }
        if (placesSource) {
            _isPlaced[source] = false;
        }

        return embedded;
    }

    bool matchesType(const SchemaPattern::Edge& edge, const SchemaArc& arc) const {
        return edge._types.empty() || std::ranges::find(edge._types, arc._type) != edge._types.end();
    }

    bool extend() {
        const size_t edgeIndex = nextEdge();
        if (edgeIndex == _pattern._edges.size()) {
            return true;
        }

        const SchemaPattern::Edge& edge = _pattern._edges[edgeIndex];
        _done[edgeIndex] = true;

        bool embedded = false;
        for (const SchemaArc& arc : _arcs) {
            if (++_visits > embeddingArcVisitBudget) {
                embedded = true;
                break;
            }

            if (!matchesType(edge, arc)) {
                continue;
            }

            const bool forward = fits(arc, edge._source, edge._target) && place(arc, edge._source, edge._target);
            const bool backward = !forward && edge._undirected && fits(arc, edge._target, edge._source) && place(arc, edge._target, edge._source);
            if (forward || backward) {
                embedded = true;
                break;
            }
        }

        _done[edgeIndex] = false;

        return embedded;
    }
};

}

SchemaGraph::SchemaGraph() {
}

SchemaGraph::~SchemaGraph() {
}

void SchemaGraph::refresh(DataPartSpan parts, const GraphMetadata& metadata) {
    size_t nodeCount = 0;
    size_t edgeCount = 0;
    for (const WeakArc<DataPart>& arc : parts) {
        const DataPart* part = arc.get();
        nodeCount = part->getFirstNodeID().getValue() + part->getNodeContainerSize();
        edgeCount = part->getFirstEdgeID().getValue() + part->getEdgeContainerSize();
    }

    const std::lock_guard<std::mutex> lock(_mutex);

    const bool upToDate = _built && _nodeCount == nodeCount && _edgeCount == edgeCount;
    if (upToDate) {
        return;
    }

    build(parts, metadata);

    _built = true;
    _nodeCount = nodeCount;
    _edgeCount = edgeCount;
}

// A table dense over (source label set, edge type), the IDs of both running from zero,
// whose cells hold the few target label sets each reaches
void SchemaGraph::build(DataPartSpan parts, const GraphMetadata& metadata) {
    const size_t labelSetCount = metadata.labelsets().getCount();
    const size_t edgeTypeCount = metadata.edgeTypes().getCount();

    // The out-records of a part run node by node, so a node's label set is looked up once
    NodeID sourceNode;
    LabelSetID source;

    std::vector<std::vector<TargetCounts>> cells(labelSetCount * edgeTypeCount);
    for (const WeakArc<DataPart>& arc : parts) {
        const DataPart* part = arc.get();

        for (const EdgeRecord& edge : part->edges().getOuts()) {
            if (edge._nodeID != sourceNode) {
                sourceNode = edge._nodeID;
                source = findNodeLabelSet(parts, sourceNode);
            }

            const LabelSetID target = findNodeLabelSet(parts, edge._otherID);
            const size_t cell = source.getValue() * edgeTypeCount + edge._edgeTypeID.getValue();
            bioassert(cell < cells.size(), "Edge {} runs over a label set or a type the metadata lacks", edge._edgeID.getValue());

            std::vector<TargetCounts>& targets = cells[cell];
            auto counted = std::ranges::find_if(targets, [target](const TargetCounts& counts) {
                return counts._target == target;
            });
            if (counted == targets.end()) {
                targets.push_back(TargetCounts {target, 0, 0});
                counted = targets.end() - 1;
            }

            counted->_count++;
            if (edge._nodeID == edge._otherID) {
                counted->_selfLoopCount++;
            }
        }
    }

    _arcs.clear();
    for (size_t cell = 0; cell < cells.size(); cell++) {
        std::vector<TargetCounts>& targets = cells[cell];
        std::ranges::sort(targets, [](const TargetCounts& left, const TargetCounts& right) {
            return left._target < right._target;
        });

        const LabelSetID source {static_cast<LabelSetID::Type>(cell / edgeTypeCount)};
        const EdgeTypeID type {static_cast<EdgeTypeID::Type>(cell % edgeTypeCount)};
        for (const TargetCounts& counted : targets) {
            _arcs.push_back(SchemaArc {source, type, counted._target, counted._count, counted._selfLoopCount});
        }
    }
}

bool SchemaGraph::embeds(const SchemaPattern& pattern, const LabelSetMap& labelSets) const {
    SchemaEmbedding embedding(_arcs, pattern);
    embedding.build(labelSets);

    return embedding.run();
}
