#include "SchemaGraph.h"

#include <algorithm>
#include <tuple>
#include <unordered_map>

#include "datapart/DataPart.h"
#include "datapart/EdgeContainer.h"
#include "datapart/EdgeRecord.h"
#include "datapart/NodeContainer.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/LabelSetMap.h"

#include "BioAssert.h"

using namespace db;

namespace {

struct ArcKey {
    LabelSetID _source;
    EdgeTypeID _type;
    LabelSetID _target;

    bool operator==(const ArcKey& other) const = default;
};

struct ArcKeyHash {
    size_t operator()(const ArcKey& key) const {
        const size_t source = key._source.getValue();
        const size_t type = key._type.getValue();
        const size_t target = key._target.getValue();

        return (source * 0x9E3779B97F4A7C15ull) ^ (type * 0xC2B2AE3D27D4EB4Full) ^ target;
    }
};

struct ArcCounts {
    size_t _count {0};
    size_t _selfLoopCount {0};
};

// The label set of a node: the parts are ordered by first node ID, so a node's owner is
// the last part starting at or before it
class NodeLabelSets {
public:
    explicit NodeLabelSets(DataPartSpan parts) {
        for (const WeakArc<DataPart>& arc : parts) {
            const DataPart* part = arc.get();
            _firstNodeIDs.push_back(part->getFirstNodeID());
            _nodes.push_back(&part->nodes());
        }
    }

    LabelSetID get(NodeID node) const {
        const auto afterOwner = std::upper_bound(_firstNodeIDs.begin(), _firstNodeIDs.end(), node);
        bioassert(afterOwner != _firstNodeIDs.begin(), "Node {} precedes every part", node.getValue());

        const size_t owner = static_cast<size_t>(afterOwner - _firstNodeIDs.begin()) - 1;
        return _nodes[owner]->getNodeLabelSet(node).getID();
    }

private:
    std::vector<NodeID> _firstNodeIDs;
    std::vector<const NodeContainer*> _nodes;
};

constexpr size_t embeddingArcVisitBudget = 200000;

// A depth-first search for the embedding, one pattern edge at a time, taking next the edge
// with the most ends already placed so that each step scans the arcs at a placed node
class SchemaEmbedding {
public:
    SchemaEmbedding(std::span<const SchemaArc> arcs, const SchemaPattern& pattern, const LabelSetMap& labelSets)
        : _arcs(arcs),
        _pattern(pattern)
    {
        const size_t nodeCount = pattern._nodes.size();
        _candidates.resize(nodeCount);
        _placed.resize(nodeCount, LabelSetID {0});
        _isPlaced.resize(nodeCount, false);
        _done.resize(pattern._edges.size(), false);

        for (size_t node = 0; node < nodeCount; node++) {
            for (const LabelSetMap::Pair& pair : labelSets) {
                if (pair._value->hasAtLeastLabels(pattern._nodes[node]._labels)) {
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

void SchemaGraph::refresh(DataPartSpan parts) {
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

    build(parts);

    _built = true;
    _nodeCount = nodeCount;
    _edgeCount = edgeCount;
}

void SchemaGraph::build(DataPartSpan parts) {
    const NodeLabelSets labelSets(parts);

    std::unordered_map<ArcKey, ArcCounts, ArcKeyHash> counts;
    for (const WeakArc<DataPart>& arc : parts) {
        const DataPart* part = arc.get();

        for (const EdgeRecord& edge : part->edges().getOuts()) {
            const ArcKey key {labelSets.get(edge._nodeID), edge._edgeTypeID, labelSets.get(edge._otherID)};

            ArcCounts& counted = counts[key];
            counted._count++;
            if (edge._nodeID == edge._otherID) {
                counted._selfLoopCount++;
            }
        }
    }

    _arcs.clear();
    for (const auto& [key, counted] : counts) {
        _arcs.push_back(SchemaArc {key._source, key._type, key._target, counted._count, counted._selfLoopCount});
    }

    std::ranges::sort(_arcs, [](const SchemaArc& left, const SchemaArc& right) {
        return std::tie(left._source, left._type, left._target) < std::tie(right._source, right._type, right._target);
    });
}

bool SchemaGraph::embeds(const SchemaPattern& pattern, const LabelSetMap& labelSets) const {
    SchemaEmbedding embedding(_arcs, pattern, labelSets);

    return embedding.run();
}
