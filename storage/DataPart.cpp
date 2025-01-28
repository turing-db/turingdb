#include "DataPart.h"

#include <numeric>
#include <range/v3/action/sort.hpp>
#include <range/v3/view/enumerate.hpp>
#include <range/v3/view/transform.hpp>

#include "NodeContainer.h"
#include "EdgeContainer.h"
#include "GraphMetadata.h"
#include "indexers/EdgeIndexer.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"
#include "writers/DataPartBuilder.h"
#include "JobSystem.h"
#include "JobGroup.h"

using namespace db;

namespace rv = ranges::views;
namespace rg = ranges;

DataPart::DataPart(EntityID firstNodeID,
                   EntityID firstEdgeID)
    : _firstNodeID(firstNodeID),
      _firstEdgeID(firstEdgeID)
{
}

DataPart::~DataPart() = default;

bool DataPart::load(const GraphView& view, JobSystem& jobSystem, DataPartBuilder& builder) {
    JobGroup jobs = jobSystem.newGroup();
    const auto reader = view.read();
    const auto& metadata = view.metadata();

    std::vector<LabelSetID>& coreNodeLabelSets = builder.coreNodeLabelSets();
    std::vector<EdgeRecord>& outEdges = builder.edges();
    std::map<EntityID, LabelSetID>& patchNodeLabelSets = builder.patchNodeLabelSets();
    std::unordered_map<EntityID, const EdgeRecord*>& patchedEdges = builder.patchedEdges();
    std::unique_ptr<PropertyManager>& nodeProperties = builder.nodeProperties();
    std::unique_ptr<PropertyManager>& edgeProperties = builder.edgeProperties();

    const EntityID firstTmpNodeID = builder.firstNodeID();
    const EntityID firstTmpEdgeID = builder.firstEdgeID();

    // Storing unknown node labelsets
    for (auto& [nodeID, labelset] : patchNodeLabelSets) {
        labelset = reader.getNodeLabelSetID(nodeID);
    }

    // Storing temp node IDs to keep track of the sorting mechanism
    std::vector<EntityID> tmpNodeIDs(coreNodeLabelSets.size());
    std::iota(tmpNodeIDs.begin(), tmpNodeIDs.end(), firstTmpNodeID);

    // Sorting based on the labelset
    rg::sort(rv::zip(coreNodeLabelSets, tmpNodeIDs),
             [](const auto& data1, const auto& data2) {
                 const LabelSetID& id1 = std::get<0>(data1);
                 const LabelSetID& id2 = std::get<0>(data2);
                 return id1 < id2;
             });

    _nodes = NodeContainer::create(_firstNodeID,
                                   metadata,
                                   coreNodeLabelSets);

    // nodeID Mapping
    for (const auto& [i, tmpID] : tmpNodeIDs | rv::enumerate) {
        const EntityID id {_firstNodeID + i};
        _tmpToFinalNodeIDs[tmpID] = id;
    }

    // Converting temp to final source/target IDs
    [[maybe_unused]] size_t i = 0;
    for (auto& out : outEdges) {
        i++;
        if (out._nodeID >= firstTmpNodeID) {
            out._nodeID = _tmpToFinalNodeIDs.at(out._nodeID);
        }
        if (out._otherID >= firstTmpNodeID) {
            out._otherID = _tmpToFinalNodeIDs.at(out._otherID);
        }
    }

    // Node properties
    _nodeProperties = std::move(nodeProperties);
    for (const auto& [ptID, props] : *_nodeProperties) {
        _nodeProperties->addIndexer(ptID);
    }

    for (const auto& [ptID, props] : *_nodeProperties) {
        jobs.submit<void>([&, ptID, props = props.get()](Promise*) {
            for (auto& id : props->ids()) {
                if (id >= firstTmpNodeID) {
                    id = _tmpToFinalNodeIDs.at(id);
                }
            }

            props->sort();

            auto& indexer = _nodeProperties->getIndexer(ptID);
            for (const auto& [offset, id] : props->ids() | rv::enumerate) {
                const LabelSetID& labelset = id >= _firstNodeID
                                               ? _nodes->getNodeLabelSet(id)
                                               : patchNodeLabelSets.at(id);

                auto& info = indexer[labelset];
                _nodeProperties->createEntity(info, id, offset);
            }
        });
    }

    jobs.wait();

    // EdgeContainer
    _edges = EdgeContainer::create(_firstNodeID,
                                   _firstEdgeID,
                                   std::move(outEdges));

    // Edge properties
    _edgeProperties = std::move(edgeProperties);
    for (const auto& [ptID, props] : *_edgeProperties) {
        _edgeProperties->addIndexer(ptID);
    }

    const auto& tmpToFinalEdgeIDs = _edges->getTmpToFinalEdgeIDs();

    for (const auto& [ptID, props] : *_edgeProperties) {
        jobs.submit<void>([&, ptID, props = props.get()](Promise*) {
            for (auto& id : props->ids()) {
                if (id >= firstTmpEdgeID) {
                    id = tmpToFinalEdgeIDs.at(id);
                }
            }

            props->sort();

            auto& indexer = _edgeProperties->addIndexer(ptID);
            for (const auto& [offset, edgeID] : props->ids() | rv::enumerate) {
                if (edgeID >= _firstEdgeID) {
                    const EdgeRecord& edge = _edges->get(edgeID);
                    const LabelSetID& labelset = edge._nodeID >= _firstNodeID
                                                   ? _nodes->getNodeLabelSet(edge._nodeID)
                                                   : patchNodeLabelSets.at(edge._nodeID);
                    auto& info = indexer[labelset];
                    _edgeProperties->createEntity(info, edgeID, offset);
                } else {
                    const EdgeRecord* edge = patchedEdges.at(edgeID);
                    const LabelSetID& labelset = patchNodeLabelSets.at(edge->_nodeID);
                    auto& info = indexer[labelset];
                    _edgeProperties->createEntity(info, edgeID, offset);
                }
            }
        });
    }

    // Edge indexer
    _edgeIndexer = EdgeIndexer::create(*_edges, *_nodes,
                                       builder.patchNodeEdgeDataCount(),
                                       patchNodeLabelSets,
                                       metadata.labelsets().getCount(),
                                       builder.getOutPatchEdgeCount(),
                                       builder.getInPatchEdgeCount(),
                                       metadata);

    jobs.wait();

    _initialized = true;
    return true;
}

EntityID DataPart::getFirstNodeID() const {
    return _nodes->getFirstNodeID();
}

EntityID DataPart::getFirstNodeID(const LabelSetID& labelset) const {
    return _nodes->getFirstNodeID(labelset);
}

size_t DataPart::getNodeCount() const {
    return _nodes->size();
}

EntityID DataPart::getFirstEdgeID() const {
    return _firstEdgeID;
}

size_t DataPart::getEdgeCount() const {
    return _edges->size();
}

bool DataPart::hasNode(EntityID nodeID) const {
    return _nodes->hasEntity(nodeID);
}

bool DataPart::hasEdge(EntityID edgeID) const {
    return (edgeID - _firstEdgeID) < _edges->size();
}
