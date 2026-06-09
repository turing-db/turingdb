#include "ShowExtensionsProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnVector.h"
#include "dataframe/NamedColumn.h"
#include "ExecutionContext.h"
#include "ExtensionManager.h"
#include "ExtensionDescriptor.h"

using namespace db;

ShowExtensionsProcessor::ShowExtensionsProcessor()
{
}

ShowExtensionsProcessor::~ShowExtensionsProcessor() {
}

std::string ShowExtensionsProcessor::describe() const {
    return fmt::format("ShowExtensionsProcessor @={}", fmt::ptr(this));
}

ShowExtensionsProcessor* ShowExtensionsProcessor::create(PipelineV2* pipeline) {
    ShowExtensionsProcessor* proc = new ShowExtensionsProcessor();

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, proc);
    proc->_output.setPort(output);
    proc->addOutput(output);

    proc->postCreate(pipeline);
    return proc;
}

void ShowExtensionsProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void ShowExtensionsProcessor::reset() {
    markAsReset();
}

void ShowExtensionsProcessor::execute() {
    auto* colName = _nameCol->as<ColumnVector<types::String::Primitive>>();

    SystemAccessor* system = _ctxt->getSystemAccessor();

    std::vector<ExtensionDescriptor*> extensions;
    system->getInstalledExtensions(extensions);

    for (const ExtensionDescriptor* ext : extensions) {
        colName->push_back(ext->getName());
    }

    _output.getPort()->writeData();
    finish();
}
