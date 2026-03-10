#include "GetEmbeddingPropertiesProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnEmbeddingMany.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

#include "PipelineException.h"

namespace db {

template <EntityType Entity>
std::string GetEmbeddingPropertiesProcessor<Entity>::describe() const {
    return fmt::format("GetEmbeddingPropertiesProcessor<{}>",
                       Entity == EntityType::Node ? "Node" : "Edge");
}

template <EntityType Entity>
GetEmbeddingPropertiesProcessor<Entity>* GetEmbeddingPropertiesProcessor<Entity>::create(PipelineV2* pipeline,
                                                                                         PropertyType propType,
                                                                                         uint32_t dimension) {
    auto* proc = new GetEmbeddingPropertiesProcessor(propType, dimension);

    PipelineInputPort* inPort = PipelineInputPort::create(pipeline, proc);
    PipelineOutputPort* outPort = PipelineOutputPort::create(pipeline, proc);

    proc->_input.setPort(inPort);
    proc->_output.setPort(outPort);

    proc->addInput(inPort);
    proc->addOutput(outPort);

    proc->postCreate(pipeline);
    return proc;
}

template <EntityType Entity>
void GetEmbeddingPropertiesProcessor<Entity>::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    const auto& stream = _input.getStream();
    const Dataframe* inDf = _input.getDataframe();

    const ColumnIDs* ids = nullptr;
    if constexpr (Entity == EntityType::Node) {
        const ColumnTag idsTag = stream.asNodeStream()._nodeIDsTag;
        ids = dynamic_cast<const ColumnIDs*>(inDf->getColumn(idsTag)->getColumn());
    } else {
        const ColumnTag idsTag = stream.asEdgeStream()._edgeIDsTag;
        ids = dynamic_cast<const ColumnIDs*>(inDf->getColumn(idsTag)->getColumn());
    }

    bioassert(ids, "GetEmbeddingPropertiesProcessor: could not get entity IDs column");

    _propWriter = std::make_unique<ChunkWriter>(ctxt->getGraphView(), _propType._id, ids);

    ColumnIndices* indices = dynamic_cast<ColumnIndices*>(_output.getIndices()->getColumn());
    _propWriter->setIndices(indices);

    ColumnValues* values = dynamic_cast<ColumnValues*>(_output.getValues()->getColumn());
    _propWriter->setOutput(values);

    markAsPrepared();
}

template <EntityType Entity>
void GetEmbeddingPropertiesProcessor<Entity>::reset() {
    _propWriter->reset();
    markAsReset();
}

template <EntityType Entity>
void GetEmbeddingPropertiesProcessor<Entity>::execute() {
    _propWriter->fill(_ctxt->getChunkSize());

    _input.getPort()->consume();
    _output.getPort()->writeData();

    finish();
}

template <EntityType Entity>
GetEmbeddingPropertiesProcessor<Entity>::GetEmbeddingPropertiesProcessor(PropertyType propType,
                                                                         uint32_t dimension)
    : _propType(propType),
    _dimension(dimension)
{
}

template class GetEmbeddingPropertiesProcessor<EntityType::Node>;
template class GetEmbeddingPropertiesProcessor<EntityType::Edge>;

}
