#include "ShowProceduresProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnVector.h"
#include "dataframe/NamedColumn.h"
#include "metadata/PropertyType.h"
#include "procedures/ProcedureBlueprintMap.h"
#include "procedures/ProcedureBlueprint.h"
#include "procedures/ProcedureReturnValues.h"

using namespace db;

namespace {

void buildSignature(std::string& result, const ProcedureBlueprint& blueprint) {
    result.clear();
    result += blueprint._name;
    result += "() :: (";

    bool first = true;
    for (const auto& rv : blueprint._returnValues) {
        if (!first) {
            result += ", ";
        }
        result += rv._name;
        result += " :: ";
        result += ProcedureReturnTypeName::value(rv._type);
        first = false;
    }
    result += ")";
}

}

ShowProceduresProcessor::ShowProceduresProcessor(const ProcedureBlueprintMap* blueprints)
    : _blueprints(blueprints)
{
}

ShowProceduresProcessor::~ShowProceduresProcessor() {
}

std::string ShowProceduresProcessor::describe() const {
    return fmt::format("ShowProceduresProcessor @={}", fmt::ptr(this));
}

ShowProceduresProcessor* ShowProceduresProcessor::create(PipelineV2* pipeline,
                                                         const ProcedureBlueprintMap* blueprints) {
    ShowProceduresProcessor* proc = new ShowProceduresProcessor(blueprints);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, proc);
    proc->_output.setPort(output);
    proc->addOutput(output);

    proc->postCreate(pipeline);
    return proc;
}

void ShowProceduresProcessor::prepare(ExecutionContext* ctxt) {
    markAsPrepared();
}

void ShowProceduresProcessor::reset() {
    markAsReset();
}

void ShowProceduresProcessor::execute() {
    auto* colName = _nameCol->as<ColumnVector<types::String::Primitive>>();
    auto* colSignature = _signatureCol->as<ColumnVector<std::string>>();

    std::string signature;
    for (const auto& blueprint : _blueprints->getAll()) {
        colName->push_back(blueprint._name);
        buildSignature(signature, blueprint);
        colSignature->push_back(signature);
    }

    _output.getPort()->writeData();
    finish();
}
