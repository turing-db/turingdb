#pragma once

#include <memory>

#include "Processor.h"

#include "PipelinePort.h"
#include "ExecutionContext.h"

#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"
#include "columns/ColumnIDs.h"
#include "columns/Block.h"
#include "iterators/ChunkConfig.h"
#include "iterators/GetPropertiesIterator.h"

#include "PipelineException.h"

namespace db::v2 {

class PipelineV2;

template <typename PropertyChunkWriter>
class GetPropertiesProcessor : public Processor {
public:
    PipelineInputPort* inIDs() { return _inIDs; }
    PipelineOutputPort* outIndices() { return _outIndices; }
    PipelineOutputPort* outValues() { return _outValues; }

    static GetPropertiesProcessor* create(PipelineV2* pipeline, PropertyType propType) {
        auto* getProps = new GetPropertiesProcessor<PropertyChunkWriter>(propType);

        PipelineInputPort* inIDs = PipelineInputPort::create(pipeline, getProps);
        PipelineOutputPort* outIndices = PipelineOutputPort::create(pipeline, getProps);
        PipelineOutputPort* outValues = PipelineOutputPort::create(pipeline, getProps);

        getProps->_inIDs = inIDs;
        getProps->_outIndices = outIndices;
        getProps->_outValues = outValues;

        getProps->addInput(inIDs);
        getProps->addOutput(outIndices);
        getProps->addOutput(outValues);

        getProps->postCreate(pipeline);
        return getProps;
    }

    void prepare(ExecutionContext* ctxt) override {
        PipelineBuffer* idsBuffer = _inIDs->getBuffer();
        if (!idsBuffer) {
            throw PipelineException("GetPropertiesProcessor: Input IDs port not connected");
        }

        ColumnNodeIDs* ids = dynamic_cast<ColumnNodeIDs*>(idsBuffer->getBlock()[0]);
        _propWriter = std::make_unique<PropertyChunkWriter>(ctxt->getGraphView(), _propType._id, ids);

        ColumnVector<size_t>* indices = dynamic_cast<ColumnVector<size_t>*>(_outIndices->getBuffer()->getBlock()[0]);
        auto* values = dynamic_cast<PropertyChunkWriter::ColumnValues*>(_outValues->getBuffer()->getBlock()[0]);

        _propWriter->setIndices(indices);
        _propWriter->setOutput(values);
        markAsPrepared();
    }

    void reset() override {
        _propWriter->reset();
        markAsReset();
    }

    void execute() override {
        _inIDs->consume();
        _propWriter->fill(ChunkConfig::CHUNK_SIZE);
        _outIndices->writeData();
        _outValues->writeData();

        if (!_propWriter->isValid()) {
            finish();
        }
    }

protected:
    PropertyType _propType;
    std::unique_ptr<PropertyChunkWriter> _propWriter;
    PipelineInputPort* _inIDs {nullptr};
    PipelineOutputPort* _outIndices {nullptr};
    PipelineOutputPort* _outValues {nullptr};

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
