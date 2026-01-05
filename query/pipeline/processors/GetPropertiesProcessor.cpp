#include "GetPropertiesProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

#include "PipelineException.h"

namespace db::v2 {

template <EntityType Entity, SupportedType T>
std::string GetPropertiesProcessor<Entity, T>::describe() const {
    return fmt::format("GetPropertiesProcessor<{}, {}>",
                       Entity == EntityType::Node ? "Node" : "Edge",
                       ValueTypeName::value(T::_valueType));
}

template <EntityType Entity, SupportedType T>
GetPropertiesProcessor<Entity, T>* GetPropertiesProcessor<Entity, T>::create(PipelineV2* pipeline, PropertyType propType) {
    auto* getProps = new GetPropertiesProcessor(propType);

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
void GetPropertiesProcessor<Entity, T>::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    const ColumnIDs* ids = nullptr;
    const auto& stream = _input.getStream();

    const Dataframe* inDf = _input.getDataframe();

    if constexpr (Entity == EntityType::Node) {
        const ColumnTag idsTag = stream.asNodeStream()._nodeIDsTag;
        if (!idsTag.isValid()) {
            throw PipelineException(fmt::format("GetPropertiesProcessor<Node, {}> must act on a Node stream",
                                                ValueTypeName::value(T::_valueType)));
        }
        ids = dynamic_cast<const ColumnNodeIDs*>(inDf->getColumn(idsTag)->getColumn());
    } else {
        const ColumnTag idsTag = stream.asEdgeStream()._edgeIDsTag;
        if (!idsTag.isValid()) {
            throw PipelineException(fmt::format("GetPropertiesProcessor<Edge, {}> must act on an Edge stream",
                                                ValueTypeName::value(T::_valueType)));
        }
        ids = dynamic_cast<const ColumnEdgeIDs*>(inDf->getColumn(idsTag)->getColumn());
    }

    _propWriter = std::make_unique<ChunkWriter>(ctxt->getGraphView(), _propType._id, ids);

    ColumnIndices* indices = dynamic_cast<ColumnIndices*>(_output.getIndices()->getColumn());
    _propWriter->setIndices(indices);

    ColumnValues* values = dynamic_cast<ColumnValues*>(_output.getValues()->getColumn());
    _propWriter->setOutput(values);
    markAsPrepared();
}

template <EntityType Entity, SupportedType T>
void GetPropertiesProcessor<Entity, T>::reset() {
    _propWriter->reset();
    markAsReset();
}

template <EntityType Entity, SupportedType T>
void GetPropertiesProcessor<Entity, T>::execute() {
    _propWriter->fill(_ctxt->getChunkSize());

    // The GetPropertiesProcessor always finishes in one step
    _input.getPort()->consume();
    _output.getPort()->writeData();

    finish();
}

template <EntityType Entity, SupportedType T>
GetPropertiesProcessor<Entity, T>::GetPropertiesProcessor(PropertyType propType)
    : _propType(propType) {
}

template class GetPropertiesProcessor<EntityType::Node, types::Int64>;
template class GetPropertiesProcessor<EntityType::Node, types::UInt64>;
template class GetPropertiesProcessor<EntityType::Node, types::Double>;
template class GetPropertiesProcessor<EntityType::Node, types::String>;
template class GetPropertiesProcessor<EntityType::Node, types::Bool>;

template class GetPropertiesProcessor<EntityType::Edge, types::Int64>;
template class GetPropertiesProcessor<EntityType::Edge, types::UInt64>;
template class GetPropertiesProcessor<EntityType::Edge, types::Double>;
template class GetPropertiesProcessor<EntityType::Edge, types::String>;
template class GetPropertiesProcessor<EntityType::Edge, types::Bool>;

}
