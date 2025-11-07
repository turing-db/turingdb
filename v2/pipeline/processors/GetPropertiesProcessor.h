#pragma once

#include <memory>

#include "Processor.h"

#include "PipelineInterface.h"

#include "ExecutionContext.h"

#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"
#include "iterators/ChunkConfig.h"
#include "iterators/GetPropertiesIterator.h"
#include "dataframe/NamedColumn.h"

namespace db::v2 {

class PipelineV2;

template <typename PropertyChunkWriter>
class GetPropertiesProcessor : public Processor {
public:
    using Values = typename PropertyChunkWriter::Values;
    using ColumnValues = typename PropertyChunkWriter::ColumnValues;

    PipelineNodeInputInterface& inIDs() { return _inIDs; }
    PipelineValuesOutputInterface& outValues() { return _outValues; }

    static GetPropertiesProcessor* create(PipelineV2* pipeline, PropertyType propType) {
        auto* getProps = new GetPropertiesProcessor<PropertyChunkWriter>(propType);

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
        const ColumnNodeIDs* ids = dynamic_cast<const ColumnNodeIDs*>(_inIDs.getNodeIDs()->getColumn());
        _propWriter = std::make_unique<PropertyChunkWriter>(ctxt->getGraphView(), _propType._id, ids);

        ColumnIndices* indices = dynamic_cast<ColumnIndices*>(_outValues.getIndices()->getColumn());
        _propWriter->setIndices(indices);

        _chunkSize = ctxt->getChunkSize();

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
        _propWriter->fill(_chunkSize);
        _outValues.getPort()->writeData();

        if (!_propWriter->isValid()) {
            finish();
        }
    }

protected:
    PropertyType _propType;
    std::unique_ptr<PropertyChunkWriter> _propWriter;
    PipelineNodeInputInterface _inIDs;
    PipelineValuesOutputInterface _outValues;
    size_t _chunkSize = 0;

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
