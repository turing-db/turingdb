#include "GetPropertiesWithNullProcessor.h"

#include "columns/ColumnIDs.h"
#include "dataframe/NamedColumn.h"

#include <spdlog/fmt/fmt.h>

namespace db::v2 {

template <EntityType Entity, SupportedType T>
std::string GetPropertiesWithNullProcessor<Entity, T>::describe() const {
    return fmt::format("GetPropertiesWithNullProcessor<{}, {}>",
                       Entity == EntityType::Node ? "Node" : "Edge",
                       ValueTypeName::value(T::_valueType));
}

template <EntityType Entity, SupportedType T>
GetPropertiesWithNullProcessor<Entity, T>* GetPropertiesWithNullProcessor<Entity, T>::create(PipelineV2* pipeline, PropertyType propType) {
    auto* getProps = new GetPropertiesWithNullProcessor(propType);

    PipelineInputPort* inIDs = PipelineInputPort::create(pipeline, getProps);
    PipelineOutputPort* outValues = PipelineOutputPort::create(pipeline, getProps);

    getProps->_inIDs.setPort(inIDs);
    getProps->_outValues.setPort(outValues);

    getProps->addInput(inIDs);
    getProps->addOutput(outValues);

    getProps->postCreate(pipeline);
    return getProps;
}

template <EntityType Entity, SupportedType T>
void GetPropertiesWithNullProcessor<Entity, T>::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    const ColumnIDs* ids = nullptr;

    if constexpr (Entity == EntityType::Node) {
        ids = dynamic_cast<const ColumnNodeIDs*>(_inIDs.getNodeIDs()->getColumn());
    } else {
        ids = dynamic_cast<const ColumnEdgeIDs*>(_inIDs.getEdgeIDs()->getColumn());
    }

    _propWriter = std::make_unique<ChunkWriter>(ctxt->getGraphView(), _propType._id, ids);

    ColumnValues* values = dynamic_cast<ColumnValues*>(_outValues.getValues()->getColumn());
    _propWriter->setOutput(values);
    markAsPrepared();
}

template <EntityType Entity, SupportedType T>
void GetPropertiesWithNullProcessor<Entity, T>::reset() {
    _propWriter->reset();
    markAsReset();
}

template <EntityType Entity, SupportedType T>
void GetPropertiesWithNullProcessor<Entity, T>::execute() {
    _propWriter->fill(_ctxt->getChunkSize());

    // The GetPropertiesWithNullProcessor always finishes in one step
    _inIDs.getPort()->consume();
    _outValues.getPort()->writeData();

    finish();
}

template <EntityType Entity, SupportedType T>
GetPropertiesWithNullProcessor<Entity, T>::GetPropertiesWithNullProcessor(PropertyType propType)
    : _propType(propType)
{
}

template class GetPropertiesWithNullProcessor<EntityType::Node, types::Int64>;
template class GetPropertiesWithNullProcessor<EntityType::Node, types::UInt64>;
template class GetPropertiesWithNullProcessor<EntityType::Node, types::Double>;
template class GetPropertiesWithNullProcessor<EntityType::Node, types::String>;
template class GetPropertiesWithNullProcessor<EntityType::Node, types::Bool>;

template class GetPropertiesWithNullProcessor<EntityType::Edge, types::Int64>;
template class GetPropertiesWithNullProcessor<EntityType::Edge, types::UInt64>;
template class GetPropertiesWithNullProcessor<EntityType::Edge, types::Double>;
template class GetPropertiesWithNullProcessor<EntityType::Edge, types::String>;
template class GetPropertiesWithNullProcessor<EntityType::Edge, types::Bool>;

}
