#include "ShowProceduresProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnVector.h"
#include "dataframe/NamedColumn.h"
#include "ExecutionContext.h"
#include "metadata/PropertyType.h"
#include "ProcedureManager.h"
#include "ProcedureNamespace.h"
#include "Procedure.h"
#include "ProcedureTypeVector.h"

using namespace db;

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
            proc->buildSignature(signature);
            colSignature->push_back(signature);
        }
    }

    _output.getPort()->writeData();
    finish();
}
