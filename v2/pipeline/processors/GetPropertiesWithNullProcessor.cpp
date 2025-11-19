#include "GetPropertiesWithNullProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "PipelineException.h"
#include "columns/ColumnIDs.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

namespace db::v2 {

template <EntityType Entity, SupportedType T>
std::string GetPropertiesWithNullProcessor<Entity, T>::describe() const {
    return fmt::format("GetPropertiesWithNullProcessor<{}, {}>",
                       Entity == EntityType::Node ? "Node" : "Edge",
                       ValueTypeName::value(T::_valueType));
}

template <EntityType Entity, SupportedType T>
GetPropertiesWithNullProcessor<Entity, T>* GetPropertiesWithNullProcessor<Entity, T>::create(PipelineV2* pipeline,
                                                                                             ColumnTag entityTag,
                                                                                             PropertyType propType) {
    auto* getProps = new GetPropertiesWithNullProcessor(entityTag, propType);

    PipelineInputPort* inIDs = PipelineInputPort::create(pipeline, getProps);
    PipelineOutputPort* outValues = PipelineOutputPort::create(pipeline, getProps);

    getProps->_input.setPort(inIDs);
    getProps->_output.setPort(outValues);

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
        if (!_entityTag.isValid()) {
            throw PipelineException("GetPropertiesWithNullProcessor: nodeIDs column is not defined");
        }

        const Column* idsCol = _input.getDataframe()->getColumn(_entityTag)->getColumn();
        ids = dynamic_cast<const ColumnNodeIDs*>(idsCol);
    } else {
        if (!_entityTag.isValid()) {
            throw PipelineException("GetPropertiesWithNullProcessor: edgeIDs column is not defined");
        }

        const Column* idsCol = _input.getDataframe()->getColumn(_entityTag)->getColumn();
        ids = dynamic_cast<const ColumnEdgeIDs*>(idsCol);
    }

    _propWriter = std::make_unique<ChunkWriter>(ctxt->getGraphView(), _propType._id, ids);

    ColumnValues* values = dynamic_cast<ColumnValues*>(_output.getValues()->getColumn());
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
    _input.getPort()->consume();
    _output.getPort()->writeData();

    finish();
}

template <EntityType Entity, SupportedType T>
GetPropertiesWithNullProcessor<Entity, T>::GetPropertiesWithNullProcessor(ColumnTag entityTag,
                                                                          PropertyType propType)
    : _propType(propType),
    _entityTag(entityTag)
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
