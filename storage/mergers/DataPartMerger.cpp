#include "DataPartMerger.h"

#include <range/v3/view/enumerate.hpp>

#include "DataPart.h"
#include "writers/DataPartBuilder.h"
#include "versioning/CommitBuilder.h"
#include "EdgeContainer.h"
#include "NodeContainer.h"
#include "versioning/TombstoneRanges.h"
#include "properties/PropertyManager.h"
#include "reader/GraphReader.h"

#include "ID.h"

using namespace db;
namespace rg = ranges;
namespace rv = rg::views;

struct PropertyTypeDispatcher {
    ValueType _valueType;

    auto execute(const auto& executor) const {
        switch (_valueType) {
            case ValueType::Int64:
                return executor.template operator()<types::Int64>();
            case ValueType::UInt64:
                return executor.template operator()<types::UInt64>();
            case ValueType::Double:
                return executor.template operator()<types::Double>();
            case ValueType::Bool:
                return executor.template operator()<types::Bool>();
            case ValueType::String:
                return executor.template operator()<types::String>();
            case ValueType::_SIZE:
            case ValueType::Invalid: {
                throw TuringException("Unsupported property type");
            }
        }
    }
};

DataPartMerger::DataPartMerger(const CommitData* commitData,
                               MetadataBuilder& metadataBuilder)
    : _graphView(*commitData),
    _metadataBuilder(metadataBuilder)
{
}

std::unique_ptr<DataPartBuilder> DataPartMerger::merge(DataPartSpan dataParts) const {
    const auto& graphReader = _graphView.read();
    const auto& tombstones = _graphView.tombstones();

    const size_t nodeCount = graphReader.getNodeCount();
    const size_t edgeCount = graphReader.getEdgeCount();

    std::unique_ptr<DataPartBuilder> datapartBuilder = DataPartBuilder::prepare(_metadataBuilder, 0, 0, 0);

    std::vector<LabelSetHandle>& labelSets = datapartBuilder->coreNodeLabelSets();
    std::vector<EdgeRecord>& edges = datapartBuilder->edges();
    PropertyManager& nodePropertyManager = *datapartBuilder->nodeProperties();
    PropertyManager& edgePropertyManager = *datapartBuilder->edgeProperties();

    // nodeIDVal will let us keep track of the current NodeID we are on
    // we can use this to check the tombstones as well as build the custom
    // tmpNodeIDs vector we will pass to datapart::load()
    std::vector<NodeID>& nodeVector = datapartBuilder->getTmpNodeIDs();
    NodeID nodeIDVal {0};

    nodeVector.reserve(nodeCount);
    labelSets.reserve(nodeCount);
    edges.resize(edgeCount);

    // Map to store the number of entries for each property
    std::unordered_map<PropertyTypeID, size_t> _nodePropertiesCount;
    std::unordered_map<PropertyTypeID, size_t> _edgePropertiesCount;

    // Maps to store the ranges of the property container to keep in every datapart
    std::vector<std::unordered_map<PropertyTypeID, TombstoneRanges>> nodeRangesPerPart {dataParts.size()};
    std::vector<std::unordered_map<PropertyTypeID, TombstoneRanges>> edgeRangesPerPart {dataParts.size()};

    // We keep a set emptyProperties to skip if they do not have values left
    // after deletion
    std::unordered_set<PropertyTypeID> emptyNodeProperties;
    std::unordered_set<PropertyTypeID> emptyEdgeProperties;
    // Loop over all the dataparts and count the number of values of each property (skippping tombstones)
    // We also caculate and store the ranges of the container to keep in this loop
    //
    for (const auto& [idx, dataPart] : dataParts | rv::enumerate) {
        for (const auto& [propertyID, propertyContainer] : dataPart->nodeProperties()) {
            const auto [it, inserted] = nodeRangesPerPart[idx].emplace(propertyID, tombstones);
            auto& tombstoneRanges = it->second;

            tombstoneRanges.populateRanges<NodeID>(propertyContainer->ids());
            _nodePropertiesCount[propertyID] += tombstoneRanges.getNumRemainingEntities();
        }

        for (const auto& [propertyID, propertyContainer] : dataPart->edgeProperties()) {
            const auto [it, inserted] = edgeRangesPerPart[idx].emplace(propertyID, tombstones);
            auto& tombstoneRanges = it->second;

            tombstoneRanges.populateRanges<EdgeID>(propertyContainer->ids());
            _edgePropertiesCount[propertyID] += tombstoneRanges.getNumRemainingEntities();
        }
    }

    const auto& prevMetadata = _graphView.metadata();
    size_t edgeContainerOffset = 0;

    // Loop Over The Dataparts and Start Merging Containers
    for (const auto& [idx, dataPart] : dataParts | rv::enumerate) {
        // Create labelSets pointing to the current commits metatdata.
        // As the nodeIDs of the original dataPart span are sorted in order, when we insert the
        // labelSets into the new dataPart they follow an ascending insertion nodeID order -
        // this follows the pattern expected by dataPart::load() and will allow us to directly copy
        // other datapart containers without needing to map the nodeIDs.
        for (const auto& nodeRecord : dataPart->nodes().records()) {
            if (!tombstones.containsNode(nodeIDVal)) {
                labelSets.emplace_back(*prevMetadata.labelsets().getValue(nodeRecord._labelset.getID()));
                nodeVector.push_back(nodeIDVal);
            }
            ++nodeIDVal;
        }

        // Copy Edges as-is (using the tombstone ranges), (as mentioned above)the nodeIDs referred to by the edges
        // follow the insertion order of the labelSets above - so these can be sorted by dataPart::load().
        TombstoneRanges deletedEdges {tombstones};
        deletedEdges.populateRanges(dataPart->edges()._outEdges);
        for (const auto& [start, size] : deletedEdges) {
            std::memcpy(edges.data() + edgeContainerOffset, dataPart->edges()._outEdges.data() + start, size * sizeof(EdgeRecord));
            edgeContainerOffset += size;
        }

        // Go over all the existing node properties and copy them into the single datapart's node container. Similar to the edges the nodeIDs here
        // follow the order of insertion of the labelsets so they will be sorted appropriately in the load() function.
        for (const auto& [propertyID, propertyContainer] : dataPart->nodeProperties()) {
            if (emptyNodeProperties.contains(propertyID)) {
                continue;
            }
            const auto process = [&]<SupportedType Type> {
                if constexpr (std::is_same_v<Type, types::String>) {
                    if (!nodePropertyManager.hasPropertyType(propertyID)) {
                        if (_nodePropertiesCount[propertyID] == 0) {
                            emptyNodeProperties.insert(propertyID);
                            return;
                        }
                        nodePropertyManager.registerPropertyType<types::String>(propertyID);

                        auto& propertyContainer = nodePropertyManager.getMutableContainer<types::String>(propertyID);
                        propertyContainer._values._views.resize(_nodePropertiesCount[propertyID]);
                        propertyContainer.ids().resize(_nodePropertiesCount[propertyID]);

                        // Re-use for offsets;
                        _nodePropertiesCount[propertyID] = 0;
                    }

                    auto& oldContainer = propertyContainer->cast<types::String>();
                    auto& newContainer = nodePropertyManager.getMutableContainer<types::String>(propertyID);

                    // since the string_view values are just copied into another string_view on the sort
                    // we can just copy the id's and the corresponding string_view that still point to
                    // the string buckets in the unmerged datapart commit. This data is guaranteed to
                    // exist until our merged datapart is fully created. This allows us to avoid unecessary
                    // copies of all the strings, and avoid having to do some not-so-nice pointer transalations

                    const TombstoneRanges& deletedNodeIDs = nodeRangesPerPart.at(idx).at(propertyID);
                    for (const auto& [start, size] : deletedNodeIDs) {
                        std::memcpy(newContainer._values._views.data() + _nodePropertiesCount[propertyID],
                                    oldContainer._values._views.data() + start,
                                    (size * sizeof(std::string_view)));
                        std::memcpy(newContainer.ids().data() + _nodePropertiesCount[propertyID],
                                    oldContainer.ids().data() + start,
                                    (size * sizeof(EntityID)));
                        _nodePropertiesCount[propertyID] += size;
                    }

                } else {
                    if (!nodePropertyManager.hasPropertyType(propertyID)) {
                        if (_nodePropertiesCount[propertyID] == 0) {
                            emptyNodeProperties.insert(propertyID);
                            return;
                        }
                        nodePropertyManager.registerPropertyType<Type>(propertyID);

                        auto& propertyContainer = nodePropertyManager.getMutableContainer<Type>(propertyID);
                        propertyContainer.values().resize(_nodePropertiesCount[propertyID]);
                        propertyContainer.ids().resize(_nodePropertiesCount[propertyID]);

                        // Re-use for offsets;
                        _nodePropertiesCount[propertyID] = 0;
                    }

                    auto& oldContainer = propertyContainer->cast<Type>();
                    auto& newContainer = nodePropertyManager.getMutableContainer<Type>(propertyID);

                    const TombstoneRanges& deletedNodeIDs = nodeRangesPerPart.at(idx).at(propertyID);
                    for (const auto& [start, size] : deletedNodeIDs) {
                        std::memcpy(newContainer.values().data() + _nodePropertiesCount[propertyID],
                                    oldContainer.values().data() + start,
                                    (size * sizeof(typename Type::Primitive)));
                        std::memcpy(newContainer.ids().data() + _nodePropertiesCount[propertyID],
                                    oldContainer.ids().data() + start,
                                    (size * sizeof(EntityID)));
                        _nodePropertiesCount[propertyID] += size;
                    }
                }
            };
            PropertyTypeDispatcher {propertyContainer->getValueType()}.execute(process);
        }

        // copy over the edge properties
        for (const auto& [propertyID, propertyContainer] : dataPart->edgeProperties()) {
            if (emptyEdgeProperties.contains(propertyID)) {
                continue;
            }

            const auto process = [&]<SupportedType Type> {
                if constexpr (std::is_same_v<Type, types::String>) {
                    if (!edgePropertyManager.hasPropertyType(propertyID)) {
                        if (_edgePropertiesCount[propertyID] == 0) {
                            emptyEdgeProperties.insert(propertyID);
                            return;
                        }

                        edgePropertyManager.registerPropertyType<types::String>(propertyID);

                        auto& propertyContainer = edgePropertyManager.getMutableContainer<types::String>(propertyID);
                        propertyContainer._values._views.resize(_edgePropertiesCount[propertyID]);
                        propertyContainer.ids().resize(_edgePropertiesCount[propertyID]);

                        // Re-use for offsets;
                        _edgePropertiesCount[propertyID] = 0;
                    }

                    auto& oldContainer = propertyContainer->cast<types::String>();
                    auto& newContainer = edgePropertyManager.getMutableContainer<types::String>(propertyID);

                    const TombstoneRanges& deletedEdgeIDs = edgeRangesPerPart.at(idx).at(propertyID);
                    for (const auto& [start, size] : deletedEdgeIDs) {
                        std::memcpy(newContainer._values._views.data() + _edgePropertiesCount[propertyID],
                                    oldContainer._values._views.data() + start,
                                    (size * sizeof(std::string_view)));
                        std::memcpy(newContainer.ids().data() + _edgePropertiesCount[propertyID],
                                    oldContainer.ids().data() + start,
                                    (size * sizeof(EntityID)));
                        _edgePropertiesCount[propertyID] += size;
                    }

                } else {
                    if (!edgePropertyManager.hasPropertyType(propertyID)) {
                        if (_edgePropertiesCount[propertyID] == 0) {
                            emptyEdgeProperties.insert(propertyID);
                            return;
                        }
                        edgePropertyManager.registerPropertyType<Type>(propertyID);

                        auto& propertyContainer = edgePropertyManager.getMutableContainer<Type>(propertyID);
                        propertyContainer.values().resize(_edgePropertiesCount[propertyID]);
                        propertyContainer.ids().resize(_edgePropertiesCount[propertyID]);

                        // Re-use for offsets;
                        _edgePropertiesCount[propertyID] = 0;
                    }
                    auto& oldContainer = propertyContainer->cast<Type>();
                    auto& newContainer = edgePropertyManager.getMutableContainer<Type>(propertyID);

                    const TombstoneRanges& deletedEdgeIDs = edgeRangesPerPart.at(idx).at(propertyID);
                    for (const auto& [start, size] : deletedEdgeIDs) {
                        std::memcpy(newContainer.values().data() + _edgePropertiesCount[propertyID],
                                    oldContainer.values().data() + start,
                                    (size * sizeof(typename Type::Primitive)));
                        std::memcpy(newContainer.ids().data() + _edgePropertiesCount[propertyID],
                                    oldContainer.ids().data() + start,
                                    (size * sizeof(EntityID)));
                        _edgePropertiesCount[propertyID] += size;
                    }
                }
            };
            PropertyTypeDispatcher {propertyContainer->getValueType()}.execute(process);
        }
    }

    return datapartBuilder;
}
