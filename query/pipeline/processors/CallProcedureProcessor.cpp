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
#include "processors/MaterializeProcessor.h"
#include "spdlog/spdlog.h"

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
    Graph* graph = sysMan->getGraph(std::string(ctxt->getGraphName()));

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
                                               std::span<Procedure::YieldItem> yieldItems,
                                               MaterializeData* matData) {
    ProcedureData& data = *_procedureState._data;

    PipelineBlockOutputInterface& output = _output;
    Dataframe* outDf = output.getDataframe();

    data.resizeReturnColumns(_procedure->returnValues().size());

    for (auto& item : yieldItems) {
        const std::string_view& colName = item._baseName;
        const std::string_view& asName = item._asName;

        spdlog::info("col name is {}, as name is {}", colName, asName);

        const size_t colIndex = _procedure->getReturnValueIndex(colName);
        const ProcedureType colType = _procedure->getReturnValueType(colIndex);

        Column* col = nullptr;

        auto allocColumn = [&]<typename T>() {
            col = mem->alloc<T>();
            data.setReturnColumn(colIndex, col);

            NamedColumn* namedCol = NamedColumn::create(dfMan, col, dfMan->allocTag());
            spdlog::info("column tag is {}", namedCol->getTag().getValue());

            if (asName.empty()) {
                namedCol->rename(colName);
            } else {
                namedCol->rename(asName);
            }

            outDf->addColumn(namedCol);
            matData->addToStep<T>(namedCol);

            item._col = namedCol;
        };

        switch (colType) {
            case ProcedureType::INVALID: {
                throw PipelineException("Invalid procedure return type");
            } break;
            case ProcedureType::NODE: {
                allocColumn.operator()<ColumnVector<NodeID>>();
            } break;
            case ProcedureType::EDGE: {
                allocColumn.operator()<ColumnVector<EdgeID>>();
            } break;
            case ProcedureType::LABEL_ID: {
                allocColumn.operator()<ColumnVector<LabelID>>();
            } break;
            case ProcedureType::EDGE_TYPE_ID: {
                allocColumn.operator()<ColumnVector<EdgeTypeID>>();
            } break;
            case ProcedureType::PROPERTY_TYPE_ID: {
                allocColumn.operator()<ColumnVector<PropertyTypeID>>();
            } break;
            case ProcedureType::VALUE_TYPE: {
                allocColumn.operator()<ColumnVector<ValueType>>();
            } break;
            case ProcedureType::UINT_64: {
                allocColumn.operator()<ColumnVector<types::UInt64::Primitive>>();
            } break;
            case ProcedureType::INT64: {
                allocColumn.operator()<ColumnVector<types::Int64::Primitive>>();
            } break;
            case ProcedureType::DOUBLE: {
                allocColumn.operator()<ColumnVector<types::Double::Primitive>>();
            } break;
            case ProcedureType::BOOL: {
                allocColumn.operator()<ColumnVector<types::Bool::Primitive>>();
            } break;
            case ProcedureType::STRING_VIEW: {
                allocColumn.operator()<ColumnVector<types::String::Primitive>>();
            } break;
            case ProcedureType::STRING: {
                allocColumn.operator()<ColumnVector<std::string>>();
            } break;
            case ProcedureType::_SIZE: {
                throw PipelineException("Invalid procedure return type: _SIZE");
            } break;
        }
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
