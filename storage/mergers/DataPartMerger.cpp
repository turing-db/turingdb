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

DataPartMerger::DataPartMerger(CommitBuilder* commitBuilder)
    :_commitBuilder(commitBuilder)
{
}

std::unique_ptr<DataPartBuilder> DataPartMerger::merge(DataPartSpan dataParts, JobSystem& jobSystem) const {
    const auto& graphReader = _commitBuilder->viewGraph().read();
    const size_t nodeCount = graphReader.getNodeCount();
    const size_t edgeCount = graphReader.getEdgeCount();

    std::unique_ptr<DataPartBuilder> datapartBuilder = DataPartBuilder::prepare(_commitBuilder->metadata(), 0, 0, 0);

    std::vector<LabelSetHandle>& labelSets = datapartBuilder->coreNodeLabelSets();
    labelSets.reserve(nodeCount);

    size_t edgeOffset = 0;
    std::vector<EdgeRecord>& edges = datapartBuilder->edges();
    edges.resize(edgeCount);

    PropertyManager& nodePropertyManager = *datapartBuilder->nodeProperties();
    PropertyManager& edgePropertyManager = *datapartBuilder->edgeProperties();

    const auto& graphMetadata = _commitBuilder->commitData().metadata();

    std::unordered_map<PropertyTypeID, size_t> _nodePropertiesCount;
    std::unordered_map<PropertyTypeID, size_t> _edgePropertiesCount;

    for (const auto& dataPart : dataParts) {
        for (const auto& it : dataPart->nodeProperties()) {
            _nodePropertiesCount[it.first] += it.second->size();
        }

        for (const auto& it : dataPart->edgeProperties()) {
            _edgePropertiesCount[it.first] += it.second->size();
        }
    }

    for (const auto& dataPart : dataParts) {
        for (const auto& record : dataPart->nodes().records()) {
            labelSets.emplace_back(*graphMetadata.labelsets().getValue(record._labelset.getID()));
        }

        std::memcpy(edges.data() + edgeOffset, dataPart->edges()._outEdges.data(), (dataPart->edges().size() * sizeof(EdgeRecord)));
        edgeOffset += dataPart->edges().size();

        for (const auto& it : dataPart->nodeProperties()) {
            const auto process = [&]<SupportedType Type> {
                if constexpr (std::is_same_v<Type, types::String>) {
                    if (!nodePropertyManager.hasPropertyType(it.first)) {
                        nodePropertyManager.registerPropertyType<types::String>(it.first);

                        auto& propertyContainer = nodePropertyManager.getMutableContainer<types::String>(it.first);
                        propertyContainer._values._views.resize(_nodePropertiesCount[it.first]);
                        propertyContainer.ids().resize(_nodePropertiesCount[it.first]);

                        // Re-use for offsets;
                        _nodePropertiesCount[it.first] = 0;
                    }

                    auto& oldContainer = it.second->cast<types::String>();
                    auto& newContainer = nodePropertyManager.getMutableContainer<types::String>(it.first);

                    // since the string_view values are just copied into another string_view on the sort
                    // we can just copy the id's and the corresponding string_view that still point to
                    // the string buckets in the unmerged datapart commit. This data is guaranteed to
                    // exist until our merged datapart is fully created. This allows us to avoid unecessary
                    // copies of all the strings, and avoid having to do some not-so-nice pointer transalations

                    std::memcpy(newContainer._values._views.data() + _nodePropertiesCount[it.first],
                                oldContainer._values._views.data(),
                                (oldContainer._values._views.size() * sizeof(std::string_view)));

                    std::memcpy(newContainer.ids().data() + _nodePropertiesCount[it.first],
                                oldContainer.ids().data(),
                                (oldContainer.ids().size() * sizeof(EntityID)));

                    _nodePropertiesCount[it.first] += oldContainer.ids().size();
                } else {
                    if (!nodePropertyManager.hasPropertyType(it.first)) {
                        nodePropertyManager.registerPropertyType<Type>(it.first);

                        auto& propertyContainer = nodePropertyManager.getMutableContainer<Type>(it.first);
                        propertyContainer.values().resize(_nodePropertiesCount[it.first]);
                        propertyContainer.ids().resize(_nodePropertiesCount[it.first]);

                        // Re-use for offsets;
                        _nodePropertiesCount[it.first] = 0;
                    }

                    auto& oldContainer = it.second->cast<Type>();
                    auto& newContainer = nodePropertyManager.getMutableContainer<Type>(it.first);

                    std::memcpy(newContainer.values().data() + _nodePropertiesCount[it.first],
                                oldContainer.values().data(),
                                (oldContainer.values().size() * sizeof(typename Type::Primitive)));

                    std::memcpy(newContainer.ids().data() + _nodePropertiesCount[it.first],
                                oldContainer.ids().data(),
                                (oldContainer.ids().size() * sizeof(EntityID)));

                    _nodePropertiesCount[it.first] += oldContainer.ids().size();
                }
            };
            PropertyTypeDispatcher {it.second->getValueType()}.execute(process);
        }

        for (const auto& it : dataPart->edgeProperties()) {
            const auto process = [&]<SupportedType Type> {
                if constexpr (std::is_same_v<Type, types::String>) {
                    if (!edgePropertyManager.hasPropertyType(it.first)) {
                        edgePropertyManager.registerPropertyType<types::String>(it.first);

                        auto& propertyContainer = edgePropertyManager.getMutableContainer<types::String>(it.first);
                        propertyContainer._values._views.resize(_edgePropertiesCount[it.first]);
                        propertyContainer.ids().resize(_edgePropertiesCount[it.first]);

                        // Re-use for offsets;
                        _edgePropertiesCount[it.first] = 0;
                    }

                    auto& oldContainer = it.second->cast<types::String>();
                    auto& newContainer = edgePropertyManager.getMutableContainer<types::String>(it.first);

                    std::memcpy(newContainer._values._views.data() + _edgePropertiesCount[it.first],
                                oldContainer._values._views.data(),
                                (oldContainer._values._views.size() * sizeof(std::string_view)));

                    std::memcpy(newContainer.ids().data() + _edgePropertiesCount[it.first],
                                oldContainer.ids().data(),
                                (oldContainer.ids().size() * sizeof(EntityID)));

                    _edgePropertiesCount[it.first] += oldContainer.ids().size();
                } else {
                    if (!edgePropertyManager.hasPropertyType(it.first)) {
                        edgePropertyManager.registerPropertyType<Type>(it.first);

                        auto& propertyContainer = edgePropertyManager.getMutableContainer<Type>(it.first);
                        propertyContainer.values().resize(_edgePropertiesCount[it.first]);
                        propertyContainer.ids().resize(_edgePropertiesCount[it.first]);

                        // Re-use for offsets;
                        _edgePropertiesCount[it.first] = 0;
                    }
                    auto& oldContainer = it.second->cast<Type>();
                    auto& newContainer = edgePropertyManager.getMutableContainer<Type>(it.first);

                    std::memcpy(newContainer.values().data() + _edgePropertiesCount[it.first],
                                oldContainer.values().data(),
                                (oldContainer.values().size() * sizeof(typename Type::Primitive)));

                    std::memcpy(newContainer.ids().data() + _edgePropertiesCount[it.first],
                                oldContainer.ids().data(),
                                (oldContainer.ids().size() * sizeof(EntityID)));

                    _edgePropertiesCount[it.first] += oldContainer.ids().size();
                }
            };
            PropertyTypeDispatcher {it.second->getValueType()}.execute(process);
        }
    }

    return datapartBuilder;
}
