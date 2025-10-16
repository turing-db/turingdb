#include "DataPartMerger.h"

#include "DataPart.h"
#include "DataPartBuilder.h"
#include "EdgeContainer.h"
#include "NodeContainer.h"
#include "versioning/CommitBuilder.h"
#include "versioning/VersionController.h"
#include "properties/PropertyManager.h"
#include "reader/GraphReader.h"
#include "ID.h"

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
                // throw PlannerException("Unsupported property type");
            }
        }
    }
};

DataPartMerger::DataPartMerger(VersionController* versionController,
               CommitBuilder* commitBuilder,
               GraphMetadata* graphMetadata)
    :_versionController(versionController),
    _commitBuilder(commitBuilder),
    _graphMetadata(graphMetadata)
{
}

void DataPartMerger::merge(DataPartSpan dataParts, JobSystem& jobSystem) {
    WeakArc<DataPart> dataPart = _versionController->createDataPart(0, 0);

    const auto& graphReader = _commitBuilder->viewGraph().read();

    const size_t nodeCount = graphReader.getNodeCount();
    const size_t edgeCount = graphReader.getEdgeCount();

    // It is not nice that we are having to pass metadata builder just for the purpose of DataPart Builder here.
    std::unique_ptr<DataPartBuilder> datapartBuilder = DataPartBuilder::prepare(_commitBuilder->metadata(), 0, 0, 0);

    std::unordered_map<NodeID, NodeID> _tmpToFinalNodeIDs;

    std::vector<LabelSetHandle>& labelSets = datapartBuilder->coreNodeLabelSets();
    labelSets.reserve(nodeCount);
    std::vector<EdgeRecord>& edges = datapartBuilder->edges();
    edges.reserve(edgeCount);
    PropertyManager& nodePropertyManager = *datapartBuilder->nodeProperties();

    size_t edgeOffset = 0;
    for (const auto& dataPart : dataParts) {
        for (const auto& record : dataPart->nodes().records()) {
            labelSets.emplace_back(_graphMetadata->labelsets().getValue(record._labelset.getID()));
        }

        std::memcpy(edges.data() + edgeOffset, dataPart->edges()._outEdges.data(), dataPart->edges().size());
        edgeOffset += dataPart->edges().size();

        for (const auto& it : dataPart->nodeProperties()) {

            const auto process = [&]<SupportedType Type> {
                if constexpr (std::is_same_v<Type, types::String>) {
                    if (!nodePropertyManager.hasPropertyType(it.first)) {
                        nodePropertyManager.registerPropertyType<types::String>(it.first);
                    }
                    auto& oldContainer = it.second->cast<types::String>();
                    auto& newContainer = nodePropertyManager.getMutableContainer<types::String>(it.first);

                    // since the string_view values are just copied into another string_view on the sort
                    // we can just copy the id's and the corresponding string_view that still point to
                    // the string buckets in the unmerged datapart commit. This data is guaranteed to
                    // exist until our merged datapart is fully created. This allows us to avoid unecessary
                    // copies of all the strings, and avoid having to do some not-so-nice pointer transalations

                    newContainer._values._views.insert(newContainer._values._views.end(),
                                                       oldContainer._values._views.begin(),
                                                       oldContainer._values._views.end());

                    newContainer.ids().insert(newContainer.ids().end(),
                                              oldContainer.ids().begin(),
                                              oldContainer.ids().end());
                } else {
                    if (!nodePropertyManager.hasPropertyType(it.first)) {
                        nodePropertyManager.registerPropertyType<types::Int64>(it.first);
                    }
                    auto& oldContainer = it.second->cast<types::Int64>();
                    auto& newContainer = nodePropertyManager.getMutableContainer<types::Int64>(it.first);

                    newContainer.values().insert(newContainer.values().end(),
                                                 oldContainer.values().begin(),
                                                 oldContainer.values().end());

                    newContainer.ids().insert(newContainer.ids().end(),
                                              oldContainer.ids().begin(),
                                              oldContainer.ids().end());
                }
            };
            PropertyTypeDispatcher {it.second->getValueType()}.execute(process);
        }

        for (const auto& it : dataPart->edgeProperties()) {

            const auto process = [&]<SupportedType Type> {
                if constexpr (std::is_same_v<Type, types::String>) {
                    if (!nodePropertyManager.hasPropertyType(it.first)) {
                        nodePropertyManager.registerPropertyType<types::String>(it.first);
                    }
                    auto& oldContainer = it.second->cast<types::String>();
                    auto& newContainer = nodePropertyManager.getMutableContainer<types::String>(it.first);

                    newContainer._values._views.insert(newContainer._values._views.end(),
                                                       oldContainer._values._views.begin(),
                                                       oldContainer._values._views.end());

                    newContainer.ids().insert(newContainer.ids().end(),
                                              oldContainer.ids().begin(),
                                              oldContainer.ids().end());
                } else {
                    if (!nodePropertyManager.hasPropertyType(it.first)) {
                        nodePropertyManager.registerPropertyType<types::Int64>(it.first);
                    }
                    auto& oldContainer = it.second->cast<types::Int64>();
                    auto& newContainer = nodePropertyManager.getMutableContainer<types::Int64>(it.first);

                    newContainer.values().insert(newContainer.values().end(),
                                                 oldContainer.values().begin(),
                                                 oldContainer.values().end());

                    newContainer.ids().insert(newContainer.ids().end(),
                                              oldContainer.ids().begin(),
                                              oldContainer.ids().end());
                }
            };
            PropertyTypeDispatcher {it.second->getValueType()}.execute(process);
        }
    }

    dataPart->load(_commitBuilder->viewGraph(), jobSystem, *datapartBuilder);
}
