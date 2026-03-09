#include "GetEmbeddingPropertiesProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnEmbeddingMany.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"
#include "properties/PropertyManager.h"

#include "DataPart.h"
#include "iterators/PartIterator.h"

#include "PipelineException.h"

namespace db {

template <EntityType Entity>
std::string GetEmbeddingPropertiesProcessor<Entity>::describe() const {
    return fmt::format("GetEmbeddingPropertiesProcessor<{}>",
                       Entity == EntityType::Node ? "Node" : "Edge");
}

template <EntityType Entity>
GetEmbeddingPropertiesProcessor<Entity>* GetEmbeddingPropertiesProcessor<Entity>::create(
    PipelineV2* pipeline, PropertyType propType, uint32_t dimension) {
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
    markAsPrepared();
}

template <EntityType Entity>
void GetEmbeddingPropertiesProcessor<Entity>::reset() {
    markAsReset();
}

template <EntityType Entity>
void GetEmbeddingPropertiesProcessor<Entity>::execute() {
    using IDType = std::conditional_t<Entity == EntityType::Node, NodeID, EdgeID>;
    using ColumnIDs = ColumnVector<IDType>;

    const ColumnIDs* ids = nullptr;
    const auto& stream = _input.getStream();

    const Dataframe* inDf = _input.getDataframe();

    if constexpr (Entity == EntityType::Node) {
        const ColumnTag idsTag = stream.asNodeStream()._nodeIDsTag;
        ids = dynamic_cast<const ColumnIDs*>(inDf->getColumn(idsTag)->getColumn());
    } else {
        const ColumnTag idsTag = stream.asEdgeStream()._edgeIDsTag;
        ids = dynamic_cast<const ColumnIDs*>(inDf->getColumn(idsTag)->getColumn());
    }

    bioassert(ids, "GetEmbeddingPropertiesProcessor: could not get entity IDs column");

    auto* outCol = dynamic_cast<ColumnEmbeddingMany*>(_output.getValues()->getColumn());
    bioassert(outCol, "GetEmbeddingPropertiesProcessor: output column is not ColumnEmbeddingMany");

    auto* indices = dynamic_cast<ColumnVector<size_t>*>(_output.getIndices()->getColumn());
    bioassert(indices, "GetEmbeddingPropertiesProcessor: indices column is not valid");

    outCol->clear();
    indices->clear();
    outCol->reserve(ids->size());
    indices->reserve(ids->size());

    const GraphView& view = _ctxt->getGraphView();
    PartIterator partIt(view);

    for (; partIt.isNotEnd(); partIt.next()) {
        const DataPart* part = partIt.get();
        const PropertyManager& props = (Entity == EntityType::Node)
                                        ? part->nodeProperties()
                                        : part->edgeProperties();

        if (!props.hasPropertyType(_propType._id)) {
            continue;
        }

        for (size_t i = 0; i < ids->size(); i++) {
            const IDType id = (*ids)[i];
            const auto* val = props.tryGet<types::Embedding>(_propType._id, id.getValue());
            if (val) {
                outCol->push_back(*val);
                indices->push_back(i);
            }
        }
    }

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
