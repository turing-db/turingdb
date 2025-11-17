#pragma once

#include <memory>

#include "EntityType.h"
#include "Processor.h"

#include "PipelineInterface.h"

#include "ExecutionContext.h"

#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"
#include "iterators/GetPropertiesIterator.h"
#include "dataframe/NamedColumn.h"

namespace db::v2 {

class PipelineV2;

template <EntityType Entity, SupportedType T>
class GetPropertiesWithNullProcessor : public Processor {
public:
    using ChunkWriter = std::conditional_t<Entity == EntityType::Node,
          GetNodePropertiesWithNullChunkWriter<T>,
          GetEdgePropertiesWithNullChunkWriter<T>>;

    using InputInterface = std::conditional_t<Entity == EntityType::Node,
          PipelineNodeInputInterface,
          PipelineEdgeInputInterface>;

    using ColumnValues = typename ChunkWriter::ColumnValues;
    using ColumnIDs = typename ChunkWriter::ColumnIDs;

    InputInterface& inIDs() { return _inIDs; }
    PipelineValuesOutputInterface& outValues() { return _outValues; }

    static GetPropertiesWithNullProcessor* create(PipelineV2* pipeline, PropertyType propType) {
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

    void prepare(ExecutionContext* ctxt) override {
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

    void reset() override {
        _propWriter->reset();
        markAsReset();
    }

    void execute() override {
        _inIDs.getPort()->consume();
        _propWriter->fill(_ctxt->getChunkSize());
        _outValues.getPort()->writeData();

        if (!_propWriter->isValid()) {
            finish();
        }
    }

protected:
    PropertyType _propType;
    std::unique_ptr<ChunkWriter> _propWriter;
    InputInterface _inIDs;
    PipelineValuesOutputInterface _outValues;

    GetPropertiesWithNullProcessor(PropertyType propType)
        : _propType(propType)
    {
    }

    ~GetPropertiesWithNullProcessor() = default;
};

}
