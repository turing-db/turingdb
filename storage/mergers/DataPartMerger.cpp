#include "DataPartMerger.h"

#include "DataPart.h"
#include "writers/DataPartBuilder.h"
#include "versioning/CommitBuilder.h"
#include "EdgeContainer.h"
#include "NodeContainer.h"
#include "properties/PropertyManager.h"
#include "ID.h"
#include "reader/GraphReader.h"
#include "versioning/VersionController.h"

using namespace db;

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

DataPartMerger::DataPartMerger(CommitData* commitData,
                               MetadataBuilder& metadataBuilder)
    : _graphView(*commitData),
    _metadataBuilder(metadataBuilder)
{
}

std::unique_ptr<DataPartBuilder> DataPartMerger::merge(DataPartSpan dataParts) const {
    const auto& graphReader = _graphView.read();
    const size_t nodeCount = graphReader.getNodeCount();
    const size_t edgeCount = graphReader.getEdgeCount();

    std::unique_ptr<DataPartBuilder> datapartBuilder = DataPartBuilder::prepare(_metadataBuilder, 0, 0, 0);

    std::vector<LabelSetHandle>& labelSets = datapartBuilder->coreNodeLabelSets();
    labelSets.reserve(nodeCount);

    size_t edgeOffset = 0;
    std::vector<EdgeRecord>& edges = datapartBuilder->edges();
    edges.resize(edgeCount);

    PropertyManager& nodePropertyManager = *datapartBuilder->nodeProperties();
    PropertyManager& edgePropertyManager = *datapartBuilder->edgeProperties();

    const auto& graphMetadata = _graphView.metadata();

    //Count The Number Of Entries For Each Property
    std::unordered_map<PropertyTypeID, size_t> _nodePropertiesCount;
    std::unordered_map<PropertyTypeID, size_t> _edgePropertiesCount;

    for (const auto& dataPart : dataParts) {
        for (const auto& [propertyID, propertyContainer] : dataPart->nodeProperties()) {
            _nodePropertiesCount[propertyID] += propertyContainer->size();
        }

        for (const auto& [propertyID, propertyContainer] : dataPart->edgeProperties()) {
            _edgePropertiesCount[propertyID] += propertyContainer->size();
        }
    }

    //Loop Over The Dataparts and Start Merging Containers
    for (const auto& dataPart : dataParts) {
        //Create labelSets pointing to the current commits metatdata.
        //As the nodeIDs of the original dataPart span are sorted in order, when we insert the
        //labelSets into the new dataPart they follow an ascending insertion nodeID order - 
        //this follows the pattern expected by dataPart::load() and will allow us to directly copy
        //other datapart containers without needing to map the nodeIDs.
        for (const auto& nodeRecord : dataPart->nodes().records()) {
            labelSets.emplace_back(*graphMetadata.labelsets().getValue(nodeRecord._labelset.getID()));
        }

        // Copy Edges as-is, (as mentioned above)the nodeIDs referred to by the edges
        // follow the insertion order of the labelSets above - so these can be sorted by dataPart::load().
        std::memcpy(edges.data() + edgeOffset, dataPart->edges()._outEdges.data(), (dataPart->edges().size() * sizeof(EdgeRecord)));
        edgeOffset += dataPart->edges().size();

        //Go over all the existing nodeproperties and copy them into the single datapart's node container. Similar to the edges the nodeIDs here
        //follow the order of insertion of the labelsets so they will be sorted appropriately in the load() function.
        for (const auto& [propertyID, propertyContainer]: dataPart->nodeProperties()) {
            const auto process = [&]<SupportedType Type> {
                if constexpr (std::is_same_v<Type, types::String>) {
                    if (!nodePropertyManager.hasPropertyType(propertyID)) {
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

                    std::memcpy(newContainer._values._views.data() + _nodePropertiesCount[propertyID],
                                oldContainer._values._views.data(),
                                (oldContainer._values._views.size() * sizeof(std::string_view)));

                    std::memcpy(newContainer.ids().data() + _nodePropertiesCount[propertyID],
                                oldContainer.ids().data(),
                                (oldContainer.ids().size() * sizeof(EntityID)));

                    _nodePropertiesCount[propertyID] += oldContainer.ids().size();
                } else {
                    if (!nodePropertyManager.hasPropertyType(propertyID)) {
                        nodePropertyManager.registerPropertyType<Type>(propertyID);

                        auto& propertyContainer = nodePropertyManager.getMutableContainer<Type>(propertyID);
                        propertyContainer.values().resize(_nodePropertiesCount[propertyID]);
                        propertyContainer.ids().resize(_nodePropertiesCount[propertyID]);

                        // Re-use for offsets;
                        _nodePropertiesCount[propertyID] = 0;
                    }

                    auto& oldContainer = propertyContainer->cast<Type>();
                    auto& newContainer = nodePropertyManager.getMutableContainer<Type>(propertyID);

                    std::memcpy(newContainer.values().data() + _nodePropertiesCount[propertyID],
                                oldContainer.values().data(),
                                (oldContainer.values().size() * sizeof(typename Type::Primitive)));

                    std::memcpy(newContainer.ids().data() + _nodePropertiesCount[propertyID],
                                oldContainer.ids().data(),
                                (oldContainer.ids().size() * sizeof(EntityID)));

                    _nodePropertiesCount[propertyID] += oldContainer.ids().size();
                }
            };
            PropertyTypeDispatcher {propertyContainer->getValueType()}.execute(process);
        }

        //copy over the edge properties
        for (const auto& [propertyID,propertyContainer]: dataPart->edgeProperties()) {
            const auto process = [&]<SupportedType Type> {
                if constexpr (std::is_same_v<Type, types::String>) {
                    if (!edgePropertyManager.hasPropertyType(propertyID)) {
                        edgePropertyManager.registerPropertyType<types::String>(propertyID);

                        auto& propertyContainer = edgePropertyManager.getMutableContainer<types::String>(propertyID);
                        propertyContainer._values._views.resize(_edgePropertiesCount[propertyID]);
                        propertyContainer.ids().resize(_edgePropertiesCount[propertyID]);

                        // Re-use for offsets;
                        _edgePropertiesCount[propertyID] = 0;
                    }

                    auto& oldContainer = propertyContainer->cast<types::String>();
                    auto& newContainer = edgePropertyManager.getMutableContainer<types::String>(propertyID);

                    std::memcpy(newContainer._values._views.data() + _edgePropertiesCount[propertyID],
                                oldContainer._values._views.data(),
                                (oldContainer._values._views.size() * sizeof(std::string_view)));

                    std::memcpy(newContainer.ids().data() + _edgePropertiesCount[propertyID],
                                oldContainer.ids().data(),
                                (oldContainer.ids().size() * sizeof(EntityID)));

                    _edgePropertiesCount[propertyID] += oldContainer.ids().size();
                } else {
                    if (!edgePropertyManager.hasPropertyType(propertyID)) {
                        edgePropertyManager.registerPropertyType<Type>(propertyID);

                        auto& propertyContainer = edgePropertyManager.getMutableContainer<Type>(propertyID);
                        propertyContainer.values().resize(_edgePropertiesCount[propertyID]);
                        propertyContainer.ids().resize(_edgePropertiesCount[propertyID]);

                        // Re-use for offsets;
                        _edgePropertiesCount[propertyID] = 0;
                    }
                    auto& oldContainer = propertyContainer->cast<Type>();
                    auto& newContainer = edgePropertyManager.getMutableContainer<Type>(propertyID);

                    std::memcpy(newContainer.values().data() + _edgePropertiesCount[propertyID],
                                oldContainer.values().data(),
                                (oldContainer.values().size() * sizeof(typename Type::Primitive)));

                    std::memcpy(newContainer.ids().data() + _edgePropertiesCount[propertyID],
                                oldContainer.ids().data(),
                                (oldContainer.ids().size() * sizeof(EntityID)));

                    _edgePropertiesCount[propertyID] += oldContainer.ids().size();
                }
            };
            PropertyTypeDispatcher {propertyContainer->getValueType()}.execute(process);
        }
    }

    return datapartBuilder;
}
