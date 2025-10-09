#include "DataPart.h"

#include <memory>
#include <numeric>
#include <range/v3/action/sort.hpp>
#include <range/v3/action/stable_sort.hpp>
#include <range/v3/view/enumerate.hpp>
#include <range/v3/view/transform.hpp>
#include <string>

#include "ID.h"
#include "NodeContainer.h"
#include "EdgeContainer.h"
#include "TuringException.h"
#include "indexers/EdgeIndexer.h"
#include "indexers/StringPropertyIndexer.h"
#include "metadata/PropertyType.h"
#include "properties/PropertyContainer.h"
#include "range/v3/algorithm/is_sorted.hpp"
#include "views/GraphView.h"
#include "reader/GraphReader.h"
#include "writers/DataPartBuilder.h"
#include "JobSystem.h"
#include "JobGroup.h"
#include "Profiler.h"

using namespace db;

namespace rv = ranges::views;
namespace rg = ranges;

DataPart::DataPart(NodeID firstNodeID,
                   EdgeID firstEdgeID)
    : _firstNodeID(firstNodeID),
    _firstEdgeID(firstEdgeID),
    _nodeStrPropIdx(std::make_unique<StringPropertyIndexer>()),
    _edgeStrPropIdx(std::make_unique<StringPropertyIndexer>())
    
{
}

DataPart::~DataPart() = default;

bool DataPart::load(const GraphView& view, JobSystem& jobSystem, DataPartBuilder& builder) {
    Profile profile {"DataPart::load"};

    JobGroup jobs = jobSystem.newGroup();
    const auto reader = view.read();

    std::vector<LabelSetHandle>& coreNodeLabelSets = builder.coreNodeLabelSets();
    std::vector<EdgeRecord>& outEdges = builder.edges();
    std::map<NodeID, LabelSetHandle>& patchNodeLabelSets = builder.patchNodeLabelSets();
    std::unordered_map<EdgeID, const EdgeRecord*>& patchedEdges = builder.patchedEdges();
    std::unique_ptr<PropertyManager>& nodeProperties = builder.nodeProperties();
    std::unique_ptr<PropertyManager>& edgeProperties = builder.edgeProperties();

    const NodeID firstTmpNodeID = builder.firstNodeID();
    const EdgeID firstTmpEdgeID = builder.firstEdgeID();

    // Storing unknown node labelsets
    for (auto& [nodeID, labelset] : patchNodeLabelSets) {
        labelset = reader.getNodeLabelSet(nodeID);
        if (!labelset.isValid() || !labelset.isStored()) {
            return false;
        }
    }

    // Storing temp node IDs to keep track of the sorting mechanism
    std::vector<NodeID> tmpNodeIDs(coreNodeLabelSets.size());
    std::iota(tmpNodeIDs.begin(), tmpNodeIDs.end(), firstTmpNodeID);

    // NOTE: With the below change from @ref rg::sort to @ref rg::stable_sort, the
    // underlying algorithm is changed from a variant of quicksort (in rg::sort) to a
    // variant of merge sort (rg::stable_sort). rg::stable_sort allocates a large amount
    // of memory, even in the case of an already-sorted array. In the case of deletions,
    // it is always the case that the nodes are already sorted. To avoid this guaranteed
    // extra memory allocation, we add this is_sorted check to reduce the overhead for
    // deletions.
    bool sorted = rg::is_sorted(rv::zip(coreNodeLabelSets, tmpNodeIDs),
                                [](const auto& data1, const auto& data2) {
                                    const LabelSetHandle& lset1 = std::get<0>(data1);
                                    const LabelSetHandle& lset2 = std::get<0>(data2);
                                    return lset1.getID() < lset2.getID();
                                });

    // NOTE: stable_sort ensures that nodes are mapped to expected of @ref
    // DataPartModifier::nodeIDMapping, which provides a much more consistent view of
    // NodeIDs to the user upon deletion.
    if (!sorted) {
        // Sorting based on the labelset
        rg::stable_sort(rv::zip(coreNodeLabelSets, tmpNodeIDs),
                        [](const auto& data1, const auto& data2) {
                            const LabelSetHandle& lset1 = std::get<0>(data1);
                            const LabelSetHandle& lset2 = std::get<0>(data2);
                            return lset1.getID() < lset2.getID();
                        });
    }

    _nodes = NodeContainer::create(_firstNodeID, coreNodeLabelSets);
    if (!_nodes) {
        return false;
    }

    // nodeID Mapping
    for (const auto& [i, tmpID] : tmpNodeIDs | rv::enumerate) {
        const NodeID id {_firstNodeID + i};
        _tmpToFinalNodeIDs[tmpID] = id;
    }

    // Converting temp to final source/target IDs
    // NOTE: This *may* be unneeded in the case @ref _coreNodeLabelSet is already sorted
    for (auto& out : outEdges) {
        if (out._nodeID >= firstTmpNodeID) {
            out._nodeID = _tmpToFinalNodeIDs.at(out._nodeID);
        }
        if (out._otherID >= firstTmpNodeID) {
            out._otherID = _tmpToFinalNodeIDs.at(out._otherID);
        }
    }

    // Node properties: Add index*ers* and note properties to *index*
    _nodeProperties = std::move(nodeProperties);
    std::vector<std::pair<PropertyTypeID, PropertyContainer*>> nodesToIndex {};
    for (const auto& [ptID, props] : *_nodeProperties) {
        _nodeProperties->addIndexer(ptID);

        // If the property is string type, we want to index it
        if (props->getValueType() == ValueType::String) {
            nodesToIndex.emplace_back(ptID, props.get());
        }
    }

    // Build indexes for noted node properties.
    // TODO: Async with jobs
    _nodeStrPropIdx->buildIndex(nodesToIndex, _tmpToFinalNodeIDs); 
    _nodeStrPropIdx->setInitialised();

    for (const auto& [ptID, props] : *_nodeProperties) {
        jobs.submit<void>([&, ptID, props = props.get()](Promise*) {
            for (auto& id : props->ids()) {
                if (id >= firstTmpNodeID.getValue()) {
                    id = _tmpToFinalNodeIDs.at(id.getValue()).getValue();
                }
            }

            props->sort();

            auto& indexer = _nodeProperties->getIndexer(ptID);
            LabelSetHandle prevLabelset;

            for (const auto& [offset, id] : props->ids() | rv::enumerate) {
                LabelSetHandle labelset = id >= _firstNodeID.getValue()
                                         ? _nodes->getNodeLabelSet(id.getValue())
                                         : patchNodeLabelSets.at(id.getValue());

                auto& info = indexer[labelset];

                if (labelset != prevLabelset) {
                    info.emplace_back(PropertyRange {
                        ._offset = offset,
                        ._count = 0,
                    });
                    prevLabelset = labelset;
                }

                auto& range = info.back();
                range._count++;
            }
        });
    }

    jobs.wait();

    // EdgeContainer
    _edges = EdgeContainer::create(_firstNodeID,
                                   _firstEdgeID,
                                   std::move(outEdges));

    // Edge properties: Add index*ers* and note properties to *index*
    _edgeProperties = std::move(edgeProperties);
    std::vector<std::pair<PropertyTypeID, PropertyContainer*>> edgesToIndex;
    for (const auto& [ptID, props] : *_edgeProperties) {
        _edgeProperties->addIndexer(ptID);

        // If the property is string type, we want to index it
        if (props->getValueType() == ValueType::String) {
            edgesToIndex.emplace_back(ptID, props.get());
        }
    }

    const auto& tmpToFinalEdgeIDs = _edges->getTmpToFinalEdgeIDs();

    // Build indexes for noted edge properties.
    // TODO: Async with jobs
    _edgeStrPropIdx->buildIndex(edgesToIndex, tmpToFinalEdgeIDs);
    _edgeStrPropIdx->setInitialised();

    for (const auto& [ptID, props] : *_edgeProperties) {
        jobs.submit<void>([&, ptID, props = props.get()](Promise*) {
            for (auto& id : props->ids()) {
                if (id >= firstTmpEdgeID.getValue()) {
                    id = tmpToFinalEdgeIDs.at(id.getValue()).getValue();
                }
            }

            props->sort();

            LabelSetHandle prevLabelset;
            auto& indexer = _edgeProperties->addIndexer(ptID);
            for (const auto& [offset, edgeID] : props->ids() | rv::enumerate) {
                LabelSetHandle labelset;
                if (edgeID >= _firstEdgeID.getValue()) {
                    const EdgeRecord& edge = _edges->get(edgeID.getValue());
                    labelset = edge._nodeID >= _firstNodeID
                                 ? _nodes->getNodeLabelSet(edge._nodeID)
                                 : patchNodeLabelSets.at(edge._nodeID);
                } else {
                    const EdgeRecord* edge = patchedEdges.at(edgeID.getValue());
                    labelset = patchNodeLabelSets.at(edge->_nodeID);
                }

                auto& info = indexer[labelset];

                if (labelset != prevLabelset) {
                    info.emplace_back(PropertyRange {
                        ._offset = offset,
                        ._count = 0,
                    });
                    prevLabelset = labelset;
                }

                auto& range = info.back();
                range._count++;
            }
        });
    }

    // Edge indexer
    _edgeIndexer = EdgeIndexer::create(*_edges, *_nodes,
                                       builder.patchNodeEdgeDataCount(),
                                       patchNodeLabelSets,
                                       builder.getOutPatchEdgeCount(),
                                       builder.getInPatchEdgeCount());

    jobs.wait();

    _initialized = true;

    return true;
}

NodeID DataPart::getFirstNodeID() const {
    return _nodes->getFirstNodeID();
}

NodeID DataPart::getFirstNodeID(const LabelSetHandle& labelset) const {
    return _nodes->getFirstNodeID(labelset);
}

size_t DataPart::getNodeCount() const {
    return _nodes->size();
}

EdgeID DataPart::getFirstEdgeID() const {
    return _firstEdgeID;
}

size_t DataPart::getEdgeCount() const {
    return _edges->size();
}

bool DataPart::hasNode(NodeID nodeID) const {
    return _nodes->hasEntity(nodeID);
}

bool DataPart::hasEdge(EdgeID edgeID) const {
    return (edgeID - _firstEdgeID) < _edges->size();
}

const StringPropertyIndexer& DataPart::getNodeStrPropIndexer() const {
    if (!_nodeStrPropIdx) {
        throw TuringException("Node String Index was not initialised");
    }
    return *_nodeStrPropIdx;
}

const StringPropertyIndexer& DataPart::getEdgeStrPropIndexer() const {
    if (!_edgeStrPropIdx) {
        throw TuringException("Edge String Index was not initialised");
    }
    return *_edgeStrPropIdx;
}
