#include "EdgeIndexer.h"

#include <range/v3/view/enumerate.hpp>

#include "EdgeContainer.h"
#include "EdgeRecord.h"
#include "NodeContainer.h"
#include "views/NodeEdgeView.h"

namespace rv = ranges::views;

using namespace db;

EdgeIndexer::EdgeIndexer(const EdgeContainer& edges)
    : _firstNodeID(edges._firstNodeID),
      _firstEdgeID(edges._firstEdgeID),
      _edges(&edges)
{
}

std::unique_ptr<EdgeIndexer> EdgeIndexer::create(const EdgeContainer& edges,
                                                 const NodeContainer& nodeContainer,
                                                 size_t patchNodeCount,
                                                 const std::map<EntityID, LabelSetHandle>& patchNodeLabelSets,
                                                 size_t patchOutEdgeCount,
                                                 size_t patchInEdgeCount) {
    auto* ptr = new EdgeIndexer(edges);
    std::unique_ptr<EdgeIndexer> indexer(ptr);

    indexer->_nodes.resize(patchNodeCount + nodeContainer.size());
    indexer->_patchNodeOffsets.reserve(patchNodeCount);
    indexer->_patchNodes = std::span(indexer->_nodes).subspan(0, patchNodeCount);
    indexer->_coreNodes = std::span(indexer->_nodes).subspan(patchNodeCount, nodeContainer.size());
    auto outs = std::span {edges._outEdges};
    auto ins = std::span {edges._inEdges};
    auto patchOutEdges = outs.subspan(0, patchOutEdgeCount);
    auto patchInEdges = ins.subspan(0, patchInEdgeCount);
    auto coreOutEdges = outs.subspan(patchOutEdgeCount, outs.size() - patchOutEdgeCount);
    auto coreInEdges = ins.subspan(patchInEdgeCount, ins.size() - patchInEdgeCount);

    auto& patchNodeOffsets = indexer->_patchNodeOffsets;
    auto& nodes = indexer->_nodes;

    // Patch out edges
    {
        EntityID currentNodeID;
        LabelSetHandle currentLabelSet;
        NodeEdgeData* currentNode = nullptr;
        size_t rangeCount = 0;
        size_t rangeFirst = 0;

        for (const auto& [i, edge] : patchOutEdges | rv::enumerate) {
            const EntityID nodeID = edge._nodeID;
            if (currentNodeID != nodeID) {
                currentNodeID = nodeID;

                auto it = patchNodeOffsets.find(nodeID);
                if (it == patchNodeOffsets.end()) {
                    it = patchNodeOffsets.emplace(nodeID, patchNodeOffsets.size()).first;
                }

                const LabelSetHandle& newLabelSet = patchNodeLabelSets.at(nodeID);
                if (newLabelSet != currentLabelSet) {
                    if (rangeCount != 0) {
                        auto& spans = indexer->_outLabelSetSpans[currentLabelSet];
                        spans.push_back(patchOutEdges.subspan(rangeFirst, rangeCount));
                    }
                    currentLabelSet = newLabelSet;
                    rangeCount = 0;
                    rangeFirst = i;
                }

                const size_t nodeOffset = it->second;
                currentNode = &nodes[nodeOffset];
                currentNode->_outRange._first = i;
            }
            ++currentNode->_outRange._count;
            ++rangeCount;
        }

        if (rangeCount != 0) {
            auto& spans = indexer->_outLabelSetSpans[currentLabelSet];
            spans.push_back(patchOutEdges.subspan(rangeFirst, rangeCount));
        }
    }

    // Core out edges
    {
        EntityID currentNodeID;
        LabelSetHandle currentLabelSet;
        NodeEdgeData* currentNode = nullptr;
        size_t rangeCount = 0;
        size_t rangeFirst = 0;

        for (const auto& [i, edge] : coreOutEdges | rv::enumerate) {
            const EntityID nodeID = edge._nodeID;
            if (currentNodeID != nodeID) {
                currentNodeID = nodeID;

                const LabelSetHandle prev = currentLabelSet;
                currentLabelSet = nodeContainer.getNodeLabelSet(nodeID);

                if (prev != currentLabelSet) {
                    if (rangeCount != 0) {
                        auto& spans = indexer->_outLabelSetSpans[prev];
                        spans.push_back(coreOutEdges.subspan(rangeFirst, rangeCount));
                        rangeCount = 0;
                        rangeFirst = i;
                    }
                }

                const size_t nodeOffset = (nodeID - edges._firstNodeID).getValue() + patchNodeCount;
                currentNode = &nodes[nodeOffset];
                currentNode->_outRange._first = i + patchOutEdgeCount;
            }
            ++currentNode->_outRange._count;
            ++rangeCount;
        }

        if (rangeCount != 0) {
            auto& spans = indexer->_outLabelSetSpans[currentLabelSet];
            spans.push_back(coreOutEdges.subspan(rangeFirst, rangeCount));
        }
    }


    // Patch in edges
    {
        EntityID currentNodeID;
        LabelSetHandle currentLabelSet;
        NodeEdgeData* currentNode = nullptr;
        size_t rangeCount = 0;
        size_t rangeFirst = 0;

        for (const auto& [i, edge] : patchInEdges | rv::enumerate) {
            const EntityID nodeID = edge._nodeID;
            if (currentNodeID != nodeID) {
                currentNodeID = nodeID;

                auto it = patchNodeOffsets.find(nodeID);
                if (it == patchNodeOffsets.end()) {
                    it = patchNodeOffsets.emplace(nodeID, patchNodeOffsets.size()).first;
                }

                const LabelSetHandle& newLabelSet = patchNodeLabelSets.at(nodeID);
                if (newLabelSet != currentLabelSet) {
                    if (rangeCount != 0) {
                        auto& spans = indexer->_inLabelSetSpans[currentLabelSet];
                        spans.push_back(patchInEdges.subspan(rangeFirst, rangeCount));
                    }
                    rangeCount = 0;
                    rangeFirst = i;
                    currentLabelSet = newLabelSet;
                }

                const size_t nodeOffset = it->second;
                currentNode = &nodes[nodeOffset];
                currentNode->_inRange._first = i;
            }
            ++currentNode->_inRange._count;
            ++rangeCount;
        }

        if (rangeCount != 0) {
            auto& spans = indexer->_inLabelSetSpans[currentLabelSet];
            spans.push_back(patchInEdges.subspan(rangeFirst, rangeCount));
        }
    }

    // Core in edges
    {
        EntityID currentNodeID;
        LabelSetHandle currentLabelSet;
        NodeEdgeData* currentNode = nullptr;
        size_t rangeCount = 0;
        size_t rangeFirst = 0;

        for (const auto& [i, edge] : coreInEdges | rv::enumerate) {
            const EntityID nodeID = edge._nodeID;
            if (currentNodeID != nodeID) {
                currentNodeID = nodeID;

                LabelSetHandle prev = currentLabelSet;
                currentLabelSet = nodeContainer.getNodeLabelSet(nodeID);

                if (prev != currentLabelSet) {
                    if (rangeCount != 0) {
                        auto& spans = indexer->_inLabelSetSpans[prev];
                        spans.push_back(coreInEdges.subspan(rangeFirst, rangeCount));
                        rangeCount = 0;
                        rangeFirst = i;
                    }
                }

                const size_t nodeOffset = (nodeID - edges._firstNodeID).getValue() + patchNodeCount;
                currentNode = &nodes[nodeOffset];
                currentNode->_inRange._first = i + patchInEdgeCount;
            }
            ++currentNode->_inRange._count;
            ++rangeCount;
        }

        if (rangeCount != 0) {
            auto& spans = indexer->_inLabelSetSpans[currentLabelSet];
            spans.push_back(coreInEdges.subspan(rangeFirst, rangeCount));
        }
    }


    return indexer;
}

std::span<const EdgeRecord> EdgeIndexer::getNodeOutEdges(EntityID nodeID) const {
    if (nodeID >= _firstNodeID) {
        // Core node
        const size_t nodeOffset = (nodeID - _firstNodeID).getValue();
        if (nodeOffset >= _coreNodes.size()) {
            // Out of bounds, return empty span
            return {};
        }

        const auto& outRange = _coreNodes[nodeOffset]._outRange;
        return std::span(_edges->_outEdges).subspan(outRange._first, outRange._count);
    }

    // Patch node
    const auto patchIt = _patchNodeOffsets.find(nodeID);
    if (patchIt == _patchNodeOffsets.end()) {
        // Node has no patch here
        return {};
    }

    const auto patchOffset = patchIt->second;
    const auto& outRange = _patchNodes[patchOffset]._outRange;
    return std::span(_edges->_outEdges).subspan(outRange._first, outRange._count);
}

std::span<const EdgeRecord> EdgeIndexer::getNodeInEdges(EntityID nodeID) const {
    if (nodeID >= _firstNodeID) {
        // Core node
        const size_t nodeOffset = (nodeID - _firstNodeID).getValue();
        if (nodeOffset >= _coreNodes.size()) {
            // Out of bounds, return empty span
            return {};
        }

        const auto& inRange = _coreNodes[nodeOffset]._inRange;
        return std::span(_edges->_inEdges).subspan(inRange._first, inRange._count);
    }

    // Patch node
    const auto patchIt = _patchNodeOffsets.find(nodeID);
    if (patchIt == _patchNodeOffsets.end()) {
        // Node has no patch here
        return {};
    }

    const auto patchOffset = patchIt->second;
    const auto& inRange = _patchNodes[patchOffset]._inRange;
    return std::span(_edges->_inEdges).subspan(inRange._first, inRange._count);
}

void EdgeIndexer::fillEntityEdgeView(EntityID nodeID, NodeEdgeView& view) const {
    auto outs = getNodeOutEdges(nodeID);
    auto ins = getNodeInEdges(nodeID);

    if (!outs.empty()) {
        view.addOuts(outs);
    }
    if (!ins.empty()) {
        view.addIns(ins);
    }
}
