#pragma once

#include <memory>

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

template <typename PropertyChunkWriter>
class GetPropertiesProcessor : public Processor {
public:
    using ColumnValues = typename PropertyChunkWriter::ColumnValues;

    std::string describe() const override;

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
        _ctxt = ctxt;

        const ColumnNodeIDs* ids = dynamic_cast<const ColumnNodeIDs*>(_inIDs.getNodeIDs()->getColumn());
        _propWriter = std::make_unique<PropertyChunkWriter>(ctxt->getGraphView(), _propType._id, ids);

        ColumnIndices* indices = dynamic_cast<ColumnIndices*>(_outValues.getIndices()->getColumn());
        _propWriter->setIndices(indices);

        ColumnValues* values = dynamic_cast<ColumnValues*>(_outValues.getValues()->getColumn());
        _propWriter->setOutput(values);
        markAsPrepared();
    }

    void reset() override {
        _propWriter->reset();
        markAsReset();
    }

    void execute() override {
        if (_propWriter->isValid()) {
            _propWriter->fill(_ctxt->getChunkSize());
            _outValues.getPort()->writeData();

            return;
        }

        // Iterator is invalid, just check if the output was consumed
        // if so, we can finish the processor and consume the input
        if (!_outValues.getPort()->hasData()) {
            _inIDs.getPort()->consume();
            finish();
        }
    }

protected:
    PropertyType _propType;
    std::unique_ptr<PropertyChunkWriter> _propWriter;
    PipelineNodeInputInterface _inIDs;
    PipelineValuesOutputInterface _outValues;

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
