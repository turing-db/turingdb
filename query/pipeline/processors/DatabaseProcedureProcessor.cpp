#include "DatabaseProcedureProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"
#include "procedures/ProcedureBlueprint.h"
#include "LocalMemory.h"

#include "PipelineException.h"

using namespace db;

DatabaseProcedureProcessor* DatabaseProcedureProcessor::create(PipelineV2* pipeline,
                                                               const ProcedureBlueprint& blueprint,
                                                               bool hasInput) {

    DatabaseProcedureProcessor* processor = new DatabaseProcedureProcessor();

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

    Procedure& procedure = processor->_procedure;
    procedure._blueprint = &blueprint;

    if (blueprint._allocCallback) {
        procedure._data = blueprint._allocCallback();
    }

    processor->postCreate(pipeline);
    return processor;
}

std::string DatabaseProcedureProcessor::describe() const {
    return fmt::format("DatabaseProcedureProcessor @={}", fmt::ptr(this));
}

void DatabaseProcedureProcessor::prepare(ExecutionContext* ctxt) {
    _procedure._ctxt = ctxt;
    _procedure._step = Procedure::Step::PREPARE;
    _procedure._blueprint->_execCallback(_procedure);

    if (_procedure.isFinished()) [[unlikely]] {
        throw PipelineException("Cannot finish a procedure in the prepare phase");
    }

    markAsPrepared();
}

void DatabaseProcedureProcessor::reset() {
    _procedure._step = Procedure::Step::RESET;
    _procedure._blueprint->_execCallback(_procedure);

    if (_procedure.isFinished()) [[unlikely]] {
        throw PipelineException("Cannot finish a procedure in the reset phase");
    }

    markAsReset();
}

void DatabaseProcedureProcessor::execute() {
    _procedure._step = Procedure::Step::EXECUTE;
    _procedure._blueprint->_execCallback(_procedure);
    _output.getPort()->writeData();

    if (_procedure.isFinished()) {
        finish();
    }
}

void DatabaseProcedureProcessor::setInputValues(std::span<const ProcedureBlueprint::InputItem> args) {
    ProcedureData& data = *_procedure._data;

    data.resizeInputColumns(_procedure._blueprint->_argumentTypes.size());

    for (const auto& item : args) {
        data.setInputColumn(item._index, item._col);
    }
}

void DatabaseProcedureProcessor::allocReturnValues(LocalMemory& mem,
                                                   DataframeManager& dfMan,
                                                   std::span<ProcedureBlueprint::YieldItem> yieldItems) {
    ProcedureData& data = *_procedure._data;

    PipelineBlockOutputInterface& output = _output;
    Dataframe* outDf = output.getDataframe();

    data.resizeReturnColumns(_procedure._blueprint->_returnValues.size());

    for (auto& item : yieldItems) {
        const std::string_view& colName = item._baseName;
        const std::string_view& asName = item._asName;

        const size_t colIndex = _procedure._blueprint->getReturnValueIndex(colName);
        const ProcedureType colType = _procedure._blueprint->getReturnValueType(colIndex);

        Column* col = nullptr;

        switch (colType) {
            case ProcedureType::INVALID: {
                throw PipelineException("Invalid procedure return type");
            } break;
            case ProcedureType::NODE: {
                col = mem.alloc<ColumnVector<NodeID>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::EDGE: {
                col = mem.alloc<ColumnVector<EdgeID>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::LABEL_ID: {
                col = mem.alloc<ColumnVector<LabelID>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::EDGE_TYPE_ID: {
                col = mem.alloc<ColumnVector<EdgeTypeID>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::PROPERTY_TYPE_ID: {
                col = mem.alloc<ColumnVector<PropertyTypeID>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::VALUE_TYPE: {
                col = mem.alloc<ColumnVector<ValueType>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::UINT_64: {
                col = mem.alloc<ColumnVector<types::UInt64::Primitive>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::INT64: {
                col = mem.alloc<ColumnVector<types::Int64::Primitive>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::DOUBLE: {
                col = mem.alloc<ColumnVector<types::Double::Primitive>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::BOOL: {
                col = mem.alloc<ColumnVector<types::Bool::Primitive>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::STRING_VIEW: {
                col = mem.alloc<ColumnVector<types::String::Primitive>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::STRING: {
                col = mem.alloc<ColumnVector<std::string>>();
                data.setReturnColumn(colIndex, col);
            } break;
            case ProcedureType::_SIZE: {
                throw PipelineException("Invalid procedure return type: _SIZE");
            } break;
        }

        NamedColumn* namedCol = NamedColumn::create(&dfMan, col, dfMan.allocTag());

        if (asName.empty()) {
            namedCol->rename(colName);
        } else {
            namedCol->rename(asName);
        }

        outDf->addColumn(namedCol);
        item._col = namedCol;
    }
}

PipelineBlockInputInterface& DatabaseProcedureProcessor::input() {
    if (!_input.has_value()) {
        throw PipelineException("No input port");
    }

    return *_input;
}

DatabaseProcedureProcessor::DatabaseProcedureProcessor()
{
}

DatabaseProcedureProcessor::~DatabaseProcedureProcessor() {
}

