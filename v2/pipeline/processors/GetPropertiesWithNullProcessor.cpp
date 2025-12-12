#include "GetPropertiesWithNullProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnIDs.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

#include "FatalException.h"

namespace db::v2 {

template <EntityType Entity, SupportedType T>
std::string GetPropertiesWithNullProcessor<Entity, T>::describe() const {
    return fmt::format("GetPropertiesWithNullProcessor<{}, {}>",
                       Entity == EntityType::Node ? "Node" : "Edge",
                       ValueTypeName::value(T::_valueType));
}

template <EntityType Entity, SupportedType T>
GetPropertiesWithNullProcessor<Entity, T>* GetPropertiesWithNullProcessor<Entity, T>::create(PipelineV2* pipeline,
                                                                                             PropertyType propType) {
    auto* getProps = new GetPropertiesWithNullProcessor(propType);

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

    const auto& stream = _input.getStream();

    const Dataframe* inDf = _input.getDataframe();


    if constexpr (Entity == EntityType::Node) {
        const ColumnTag idsTag = stream.asNodeStream()._nodeIDsTag;
        if (!idsTag.isValid()) {
            throw FatalException(fmt::format(
                "GetPropertiesWithNullProcessor<Node, {}> must act on a Node stream",
                ValueTypeName::value(T::_valueType)));
        }
        ids = dynamic_cast<const ColumnNodeIDs*>(inDf->getColumn(idsTag)->getColumn());
    } else {
        const ColumnTag idsTag = stream.asEdgeStream()._edgeIDsTag;
        if (!idsTag.isValid()) {
            throw FatalException(fmt::format(
                "GetPropertiesWithNullProcessor<Edge, {}> must act on an Edge stream",
                ValueTypeName::value(T::_valueType)));
        }
        ids = dynamic_cast<const ColumnEdgeIDs*>(inDf->getColumn(idsTag)->getColumn());
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
