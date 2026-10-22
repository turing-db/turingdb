#include "SchemaGraph.h"

#include <algorithm>
#include <stdint.h>
#include <unordered_map>

#include "datapart/DataPart.h"
#include "datapart/EdgeContainer.h"
#include "datapart/EdgeRecord.h"
#include "datapart/NodeContainer.h"
#include "datapart/NodeOwnerPart.h"
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

LabelSetID findNodeLabelSet(std::span<const NodeID> firstNodeIDs, std::span<const NodeContainer* const> nodeContainers, NodeID node) {
    const size_t owner = findNodeOwnerPart(firstNodeIDs, node);
    bioassert(owner < firstNodeIDs.size(), "Node {} is in no part", node.getValue());

    const LabelSetHandle labelSet = nodeContainers[owner]->getNodeLabelSet(node);
    bioassert(labelSet.isValid(), "Node {} is past the end of its owner part", node.getValue());

    return labelSet.getID();
}

// The pattern as a constraint problem: each node takes a label set carrying its labels, each
// edge an arc of one of its types between the label sets its two ends took. Arc consistency
// first strikes every label set no arc supports, which settles a pattern without cycles
// outright; a backtracking search over what is left then places the nodes one at a time.
// Every arc, label set and candidate visited takes a step, and a search left without
// steps answers true.
class SchemaEmbedding {
public:
    SchemaEmbedding(std::span<const SchemaArc> arcs, const SchemaPattern& pattern, size_t& steps)
        : _arcs(arcs),
        _pattern(pattern),
        _steps(steps)
    {
    }

    bool run(const LabelSetMap& labelSets) {
        indexLabelSets(labelSets);
        buildDomains(labelSets);
        buildSupports();

        if (_outOfSteps) {
            return true;
        } else if (!makeArcConsistent()) {
            return _outOfSteps;
        }

        orderNodes();

        return search();
    }

private:
    static constexpr size_t UNPLACED = SIZE_MAX;

    // A pattern edge to a node placed earlier in the search order
    struct EarlierEdge {
        size_t _edge {0};
        size_t _other {0};
        bool _leavesTheNode {false};
    };

    std::span<const SchemaArc> _arcs;
    const SchemaPattern& _pattern;
    size_t& _steps;
    bool _outOfSteps {false};

    std::vector<LabelSetID> _labelSets;
    std::unordered_map<LabelSetID, size_t> _labelSetIndices;

    // A pattern node's label sets left, one flag per indexed label set
    std::vector<std::vector<uint8_t>> _domains;
    std::vector<size_t> _domainSizes;

    // The (source, target) label set index pairs an arc lets a pattern edge take, sorted
    std::vector<std::vector<uint64_t>> _supports;
    std::vector<std::vector<size_t>> _edgesAtNode;

    std::vector<size_t> _order;
    std::vector<std::vector<EarlierEdge>> _earlierEdges;
    std::vector<size_t> _placed;

    bool takeSteps(size_t count) {
        if (_steps < count) {
            _steps = 0;
            _outOfSteps = true;
            return false;
        }

        _steps -= count;

        return true;
    }

    static uint64_t supportKey(size_t source, size_t target) {
        return (static_cast<uint64_t>(source) << 32) | static_cast<uint64_t>(target);
    }

    void indexLabelSets(const LabelSetMap& labelSets) {
        for (const LabelSetMap::Pair& pair : labelSets) {
            _labelSetIndices.emplace(pair._id, _labelSets.size());
            _labelSets.push_back(pair._id);
        }
    }

    void buildDomains(const LabelSetMap& labelSets) {
        const size_t nodeCount = _pattern._nodes.size();
        const size_t labelSetCount = _labelSets.size();

        _domains.resize(nodeCount);
        _domainSizes.resize(nodeCount, 0);
        for (size_t node = 0; node < nodeCount; node++) {
            if (!takeSteps(labelSetCount)) {
                return;
            }

            const LabelSet& wanted = _pattern._nodes[node]._labels;
            std::vector<uint8_t>& domain = _domains[node];
            domain.resize(labelSetCount, 0);

            size_t index = 0;
            for (const LabelSetMap::Pair& pair : labelSets) {
                if (pair._value->hasAtLeastLabels(wanted)) {
                    domain[index] = 1;
                    _domainSizes[node]++;
                }
                index++;
            }
        }
    }

    bool matchesType(const SchemaPattern::Edge& edge, const SchemaArc& arc) const {
        return edge._types.empty() || std::ranges::find(edge._types, arc._type) != edge._types.end();
    }

    void buildSupports() {
        const size_t edgeCount = _pattern._edges.size();
        _supports.resize(edgeCount);
        _edgesAtNode.resize(_pattern._nodes.size());

        for (size_t edgeIndex = 0; edgeIndex < edgeCount; edgeIndex++) {
            if (!takeSteps(_arcs.size())) {
                return;
            }

            const SchemaPattern::Edge& edge = _pattern._edges[edgeIndex];
            _edgesAtNode[edge._source].push_back(edgeIndex);
            if (edge._target != edge._source) {
                _edgesAtNode[edge._target].push_back(edgeIndex);
            }

            std::vector<uint64_t>& supports = _supports[edgeIndex];
            for (const SchemaArc& arc : _arcs) {
                const auto source = _labelSetIndices.find(arc._source);
                const auto target = _labelSetIndices.find(arc._target);
                const bool indexed = source != _labelSetIndices.end() && target != _labelSetIndices.end();
                if (!indexed || !matchesType(edge, arc)) {
                    continue;
                }

                if (edge._source == edge._target) {
                    const bool closesOnItself = arc._source == arc._target && arc._selfLoopCount > 0;
                    if (closesOnItself) {
                        supports.push_back(supportKey(source->second, source->second));
                    }
                    continue;
                }

                supports.push_back(supportKey(source->second, target->second));
                if (edge._undirected) {
                    supports.push_back(supportKey(target->second, source->second));
                }
            }

            std::ranges::sort(supports);
        }
    }

    // Strikes from the node's label sets those @param supported leaves unflagged, false when
    // none is left
    bool keepSupported(size_t node, const std::vector<uint8_t>& supported, bool& changed) {
        std::vector<uint8_t>& domain = _domains[node];
        for (size_t index = 0; index < domain.size(); index++) {
            if (domain[index] && !supported[index]) {
                domain[index] = 0;
                _domainSizes[node]--;
                changed = true;
            }
        }

        return _domainSizes[node] > 0;
    }

    bool makeArcConsistent() {
        const size_t edgeCount = _pattern._edges.size();
        const size_t labelSetCount = _labelSets.size();

        for (size_t node = 0; node < _pattern._nodes.size(); node++) {
            if (_domainSizes[node] == 0) {
                return false;
            }
        }

        std::vector<size_t> worklist(edgeCount);
        std::vector<uint8_t> queued(edgeCount, 1);
        for (size_t edgeIndex = 0; edgeIndex < edgeCount; edgeIndex++) {
            worklist[edgeIndex] = edgeCount - 1 - edgeIndex;
        }

        std::vector<uint8_t> sourceSupported(labelSetCount);
        std::vector<uint8_t> targetSupported(labelSetCount);

        while (!worklist.empty()) {
            const size_t edgeIndex = worklist.back();
            worklist.pop_back();
            queued[edgeIndex] = 0;

            const std::vector<uint64_t>& supports = _supports[edgeIndex];
            if (!takeSteps(supports.size() + labelSetCount)) {
                return false;
            }

            const SchemaPattern::Edge& edge = _pattern._edges[edgeIndex];
            const std::vector<uint8_t>& sourceDomain = _domains[edge._source];
            const std::vector<uint8_t>& targetDomain = _domains[edge._target];

            std::ranges::fill(sourceSupported, 0);
            std::ranges::fill(targetSupported, 0);
            for (const uint64_t key : supports) {
                const size_t source = static_cast<size_t>(key >> 32);
                const size_t target = static_cast<size_t>(key & 0xffffffffu);
                if (sourceDomain[source] && targetDomain[target]) {
                    sourceSupported[source] = 1;
                    targetSupported[target] = 1;
                }
            }

            bool sourceChanged = false;
            bool targetChanged = false;
            if (!keepSupported(edge._source, sourceSupported, sourceChanged)) {
                return false;
            } else if (!keepSupported(edge._target, targetSupported, targetChanged)) {
                return false;
            }

            for (const size_t node : {edge._source, edge._target}) {
                const bool changed = node == edge._source ? sourceChanged : targetChanged;
                if (!changed) {
                    continue;
                }

                for (const size_t incident : _edgesAtNode[node]) {
                    if (!queued[incident]) {
                        queued[incident] = 1;
                        worklist.push_back(incident);
                    }
                }
            }
        }

        return true;
    }

    // Breadth first from each node not yet reached, so that every node after the first of
    // its component is placed beside one already placed
    void orderNodes() {
        const size_t nodeCount = _pattern._nodes.size();
        std::vector<size_t> positions(nodeCount, UNPLACED);

        for (size_t root = 0; root < nodeCount; root++) {
            if (positions[root] != UNPLACED) {
                continue;
            }

            positions[root] = _order.size();
            _order.push_back(root);
            for (size_t next = positions[root]; next < _order.size(); next++) {
                const size_t node = _order[next];
                for (const size_t edgeIndex : _edgesAtNode[node]) {
                    const SchemaPattern::Edge& edge = _pattern._edges[edgeIndex];
                    const size_t other = edge._source == node ? edge._target : edge._source;
                    if (positions[other] == UNPLACED) {
                        positions[other] = _order.size();
                        _order.push_back(other);
                    }
                }
            }
        }

        _earlierEdges.resize(nodeCount);
        for (size_t edgeIndex = 0; edgeIndex < _pattern._edges.size(); edgeIndex++) {
            const SchemaPattern::Edge& edge = _pattern._edges[edgeIndex];
            if (edge._source == edge._target) {
                continue;
            }

            const bool sourceLater = positions[edge._source] > positions[edge._target];
            const size_t later = sourceLater ? edge._source : edge._target;
            const size_t earlier = sourceLater ? edge._target : edge._source;
            _earlierEdges[later].push_back(EarlierEdge {._edge = edgeIndex, ._other = earlier, ._leavesTheNode = sourceLater});
        }
    }

    bool fitsThePlaced(size_t node, size_t labelSet) const {
        for (const EarlierEdge& earlier : _earlierEdges[node]) {
            const size_t other = _placed[earlier._other];
            const uint64_t key = earlier._leavesTheNode ? supportKey(labelSet, other) : supportKey(other, labelSet);
            if (!std::ranges::binary_search(_supports[earlier._edge], key)) {
                return false;
            }
        }

        return true;
    }

    bool search() {
        const size_t depthCount = _order.size();
        _placed.assign(_pattern._nodes.size(), UNPLACED);

        std::vector<size_t> cursors(depthCount, 0);
        size_t depth = 0;
        while (depth < depthCount) {
            const size_t node = _order[depth];
            const std::vector<uint8_t>& domain = _domains[node];
            size_t& cursor = cursors[depth];

            bool placed = false;
            for (; cursor < domain.size() && !placed; cursor++) {
                if (!domain[cursor]) {
                    continue;
                } else if (!takeSteps(1)) {
                    return true;
                }

                if (fitsThePlaced(node, cursor)) {
                    _placed[node] = cursor;
                    placed = true;
                }
            }

            if (placed) {
                depth++;
                if (depth < depthCount) {
                    cursors[depth] = 0;
                }
            } else if (depth == 0) {
                return false;
            } else {
                cursor = 0;
                depth--;
            }
        }

        return true;
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

    std::vector<NodeID> firstNodeIDs;
    std::vector<const NodeContainer*> nodeContainers;
    for (const WeakArc<DataPart>& arc : parts) {
        const DataPart* part = arc.get();
        firstNodeIDs.push_back(part->getFirstNodeID());
        nodeContainers.push_back(&part->nodes());
    }

    std::vector<std::vector<TargetCounts>> cells(labelSetCount * edgeTypeCount);
    for (const WeakArc<DataPart>& arc : parts) {
        const DataPart* part = arc.get();
        const NodeContainer& partNodes = part->nodes();

        for (const EdgeRecord& edge : part->edges().getOuts()) {
            if (edge._nodeID != sourceNode) {
                sourceNode = edge._nodeID;
                source = partNodes.hasEntity(sourceNode) ? partNodes.getNodeLabelSet(sourceNode).getID() : findNodeLabelSet(firstNodeIDs, nodeContainers, sourceNode);
            }

            const LabelSetID target = findNodeLabelSet(firstNodeIDs, nodeContainers, edge._otherID);
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

bool SchemaGraph::embeds(const SchemaPattern& pattern, const LabelSetMap& labelSets, size_t& steps) const {
    SchemaEmbedding embedding(_arcs, pattern, steps);

    return embedding.run(labelSets);
}
