#pragma once

#include <memory>

#include "Processor.h"

#include "PipelineInterface.h"

#include "ExecutionContext.h"

#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/GetPropertiesIterator.h"

namespace db::v2 {

class PipelineV2;

template <typename PropertyChunkWriter>
class GetPropertiesProcessor : public Processor {
public:
    PipelineInputInterface& inIDs() { return _inIDs; }
    PipelineOutputInterface& outIndices() { return _outIndices; }
    PipelineOutputInterface& outValues() { return _outValues; }

    static GetPropertiesProcessor* create(PipelineV2* pipeline, PropertyType propType) {
        auto* getProps = new GetPropertiesProcessor<PropertyChunkWriter>(propType);

        PipelineInputPort* inIDs = PipelineInputPort::create(pipeline, getProps);
        PipelineOutputPort* outIndices = PipelineOutputPort::create(pipeline, getProps);
        PipelineOutputPort* outValues = PipelineOutputPort::create(pipeline, getProps);

        getProps->_inIDs.setPort(inIDs);
        getProps->_outIndices.setPort(outIndices);
        getProps->_outValues.setPort(outValues);

        getProps->addInput(inIDs);
        getProps->addOutput(outIndices);
        getProps->addOutput(outValues);

        getProps->postCreate(pipeline);
        return getProps;
    }

    void prepare(ExecutionContext* ctxt) override {
        ColumnNodeIDs* ids = dynamic_cast<ColumnNodeIDs*>(_inIDs.getRawColumn());
        _propWriter = std::make_unique<PropertyChunkWriter>(ctxt->getGraphView(), _propType._id, ids);

        ColumnVector<size_t>* indices = dynamic_cast<ColumnVector<size_t>*>(_outIndices.getRawColumn());
        auto* values = dynamic_cast<PropertyChunkWriter::ColumnValues*>(_outValues.getRawColumn());

        _propWriter->setIndices(indices);
        _propWriter->setOutput(values);
        markAsPrepared();
    }

    void reset() override {
        _propWriter->reset();
        markAsReset();
    }

    void execute() override {
        _inIDs.getPort()->consume();
        _propWriter->fill(ChunkConfig::CHUNK_SIZE);
        _outIndices.getPort()->writeData();
        _outValues.getPort()->writeData();

        if (!_propWriter->isValid()) {
            finish();
        }
    }

protected:
    PropertyType _propType;
    std::unique_ptr<PropertyChunkWriter> _propWriter;
    PipelineInputInterface _inIDs;
    PipelineOutputInterface _outIndices;
    PipelineOutputInterface _outValues;

    GetPropertiesProcessor(PropertyType propType)
        : _propType(propType)
    {
    }

    ~GetPropertiesProcessor() = default;
};

template <SupportedType T>
using GetNodePropertiesProcessor = GetPropertiesProcessor<GetNodePropertiesChunkWriter<T>>;

template <SupportedType T>
using GetEdgePropertiesProcessor = GetPropertiesProcessor<GetEdgePropertiesChunkWriter<T>>;

using GetNodePropertiesInt64Processor = GetNodePropertiesProcessor<types::Int64>;
using GetNodePropertiesUInt64Processor = GetNodePropertiesProcessor<types::UInt64>;
using GetNodePropertiesDoubleProcessor = GetNodePropertiesProcessor<types::Double>;
using GetNodePropertiesStringProcessor = GetNodePropertiesProcessor<types::String>;
using GetNodePropertiesBoolProcessor = GetNodePropertiesProcessor<types::Bool>;

using GetEdgePropertiesInt64Processor = GetEdgePropertiesProcessor<types::Int64>;
using GetEdgePropertiesUInt64Processor = GetEdgePropertiesProcessor<types::UInt64>;
using GetEdgePropertiesDoubleProcessor = GetEdgePropertiesProcessor<types::Double>;
using GetEdgePropertiesStringProcessor = GetEdgePropertiesProcessor<types::String>;
using GetEdgePropertiesBoolProcessor = GetEdgePropertiesProcessor<types::Bool>;

}
