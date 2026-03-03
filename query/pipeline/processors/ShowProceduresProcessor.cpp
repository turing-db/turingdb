#include "ShowProceduresProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnVector.h"
#include "dataframe/NamedColumn.h"
#include "ExecutionContext.h"
#include "metadata/PropertyType.h"
#include "procedures/ProcedureManager.h"
#include "procedures/ProcedureNamespace.h"
#include "procedures/Procedure.h"
#include "procedures/ProcedureTypeVector.h"

using namespace db;

namespace {

void buildSignature(std::string& result, const Procedure* proc) {
    result.clear();
    result += proc->getFullName();
    result += "(";

    bool first = true;
    for (const auto& arg : proc->argumentTypes()) {
        if (!first) {
            result += ", ";
        }
        result += arg._name;
        result += " :: ";
        result += ProcedureTypeName::value(arg._type);
        first = false;
    }

    result += ") :: (";

    first = true;
    for (const auto& rv : proc->returnValues()) {
        if (!first) {
            result += ", ";
        }
        result += rv._name;
        result += " :: ";
        result += ProcedureTypeName::value(rv._type);
        first = false;
    }

    result += ")";
}

}

ShowProceduresProcessor::ShowProceduresProcessor()
{
}

ShowProceduresProcessor::~ShowProceduresProcessor() {
}

std::string ShowProceduresProcessor::describe() const {
    return fmt::format("ShowProceduresProcessor @={}", fmt::ptr(this));
}

ShowProceduresProcessor* ShowProceduresProcessor::create(PipelineV2* pipeline) {
    ShowProceduresProcessor* proc = new ShowProceduresProcessor();

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, proc);
    proc->_output.setPort(output);
    proc->addOutput(output);

    proc->postCreate(pipeline);
    return proc;
}

void ShowProceduresProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void ShowProceduresProcessor::reset() {
    markAsReset();
}

void ShowProceduresProcessor::execute() {
    auto* colName = _nameCol->as<ColumnVector<types::String::Primitive>>();
    auto* colSignature = _signatureCol->as<ColumnVector<std::string>>();

    const ProcedureManager* manager = _ctxt->getProcedures();

    ProcedureManager::Namespaces namespaces;
    manager->getNamespaces(namespaces);

    std::string signature;
    ProcedureNamespace::Procedures procedures;
    for (const auto* ns : namespaces) {
        ns->getProcedures(procedures);

        for (const auto* proc : procedures) {
            colName->push_back(proc->getFullName());
            buildSignature(signature, proc);
            colSignature->push_back(signature);
        }
    }

    _output.getPort()->writeData();
    finish();
}
