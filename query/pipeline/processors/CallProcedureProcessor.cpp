#include "CallProcedureProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"
#include "Procedure.h"
#include "ExecutionContext.h"
#include "SystemManager.h"
#include "LocalMemory.h"

#include "PipelineException.h"

using namespace db;

CallProcedureProcessor* CallProcedureProcessor::create(PipelineV2* pipeline,
                                                       const Procedure* procedure,
                                                       bool hasInput) {
    CallProcedureProcessor* processor = new CallProcedureProcessor();
    processor->_procedure = procedure;

    if (hasInput) {
        processor->_input = std::make_optional<PipelineBlockInputInterface>();

        auto* inputPort = PipelineInputPort::create(pipeline, processor);
        processor->_input->setPort(inputPort);
        processor->addInput(inputPort);
        inputPort->setNeedsData(true);
    }

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, processor);
    processor->_output.setPort(output);
    processor->addOutput(output);

    const Procedure::AllocCallback alloc = procedure->getAllocCallback();
    if (alloc) {
        processor->_procedureState._data = alloc();
    }

    processor->_procedureState._procedure = procedure;

    processor->postCreate(pipeline);
    return processor;
}

std::string CallProcedureProcessor::describe() const {
    return fmt::format("CallProcedureProcessor @={}", fmt::ptr(this));
}

void CallProcedureProcessor::prepare(ExecutionContext* ctxt) {
    SystemManager* sysMan = ctxt->getSystemManager();
    Graph* graph = sysMan->getGraph(ctxt->getGraphName());

    _procedureContext.setGraph(graph);
    _procedureContext.setGraphView(&ctxt->getGraphView());
    _procedureContext.setTransaction(ctxt->getTransaction());
    _procedureContext.setProcedures(ctxt->getProcedures());
    _procedureContext.setChunkSize(ctxt->getChunkSize());

    _procedureState._ctxt = &_procedureContext;
    _procedureState._step = ProcedureState::Step::PREPARE;
    _procedure->getExecCallback()(&_procedureState);

    if (_procedureState.isFinished()) [[unlikely]] {
        throw PipelineException("Cannot finish a procedure in the prepare phase");
    }

    markAsPrepared();
}

void CallProcedureProcessor::reset() {
    _procedureState._step = ProcedureState::Step::RESET;
    _procedure->getExecCallback()(&_procedureState);

    if (_procedureState.isFinished()) [[unlikely]] {
        throw PipelineException("Cannot finish a procedure in the reset phase");
    }

    markAsReset();
}

void CallProcedureProcessor::execute() {
    _procedureState._step = ProcedureState::Step::EXECUTE;
    _procedure->getExecCallback()(&_procedureState);
    _output.getPort()->writeData();

    if (_procedureState.isFinished()) {
        finish();
    }
}

void CallProcedureProcessor::setInputValues(std::span<const Procedure::Argument> args) {
    ProcedureData& data = *_procedureState._data;

    data.resizeInputColumns(_procedure->argumentTypes().size());

    for (const auto& item : args) {
        data.setInputColumn(item._index, item._col);
    }
}

void CallProcedureProcessor::allocReturnValues(LocalMemory* mem,
                                               DataframeManager* dfMan,
                                               std::span<Procedure::YieldItem> yieldItems) {
    ProcedureData& data = *_procedureState._data;

    PipelineBlockOutputInterface& output = _output;
    Dataframe* outDf = output.getDataframe();

    data.resizeReturnColumns(_procedure->returnValues().size());

    for (auto& item : yieldItems) {
        const std::string_view& colName = item._baseName;
        const std::string_view& asName = item._asName;

        const size_t colIndex = _procedure->getReturnValueIndex(colName);
        const ProcedureType colType = _procedure->getReturnValueType(colIndex);

        Column* col = nullptr;

        switch (colType) {
            case ProcedureType::INVALID: {
                throw PipelineException("Invalid procedure return type");
            } break;
            case ProcedureType::NODE: {
                col = mem->alloc<ColumnVector<NodeID>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::EDGE: {
                col = mem->alloc<ColumnVector<EdgeID>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::LABEL_ID: {
                col = mem->alloc<ColumnVector<LabelID>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::EDGE_TYPE_ID: {
                col = mem->alloc<ColumnVector<EdgeTypeID>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::PROPERTY_TYPE_ID: {
                col = mem->alloc<ColumnVector<PropertyTypeID>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::VALUE_TYPE: {
                col = mem->alloc<ColumnVector<ValueType>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::UINT_64: {
                col = mem->alloc<ColumnVector<types::UInt64::Primitive>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::INT64: {
                col = mem->alloc<ColumnVector<types::Int64::Primitive>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::DOUBLE: {
                col = mem->alloc<ColumnVector<types::Double::Primitive>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::BOOL: {
                col = mem->alloc<ColumnVector<types::Bool::Primitive>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::STRING_VIEW: {
                col = mem->alloc<ColumnVector<types::String::Primitive>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::STRING: {
                col = mem->alloc<ColumnVector<std::string>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::_SIZE: {
                throw PipelineException("Invalid procedure return type: _SIZE");
            } break;
        }

        NamedColumn* namedCol = NamedColumn::create(dfMan, col, dfMan->allocTag());

        if (asName.empty()) {
            namedCol->rename(colName);
        } else {
            namedCol->rename(asName);
        }

        outDf->addColumn(namedCol);
        item._col = namedCol;
    }
}

PipelineBlockInputInterface& CallProcedureProcessor::input() {
    if (!_input.has_value()) {
        throw PipelineException("No input port");
    }

    return *_input;
}

CallProcedureProcessor::CallProcedureProcessor()
{
}

CallProcedureProcessor::~CallProcedureProcessor() {
    if (_procedureState._data && _procedure) {
        const Procedure::DeallocCallback dealloc = _procedure->getDeallocCallback();
        if (dealloc) {
            dealloc(_procedureState._data);
        }
    }
}
