#include "InstallExtensionProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "dataframe/NamedColumn.h"
#include "columns/ColumnConst.h"

#include "ExecutionContext.h"
#include "ExtensionManager.h"
#include "PipelineException.h"

using namespace db;

InstallExtensionProcessor::InstallExtensionProcessor(std::string_view extensionName)
    : _extensionName(extensionName)
{
}

InstallExtensionProcessor::~InstallExtensionProcessor() {
}

InstallExtensionProcessor* InstallExtensionProcessor::create(PipelineV2* pipeline,
                                                             std::string_view extensionName) {
    InstallExtensionProcessor* proc = new InstallExtensionProcessor(extensionName);

    PipelineOutputPort* outName = PipelineOutputPort::create(pipeline, proc);
    proc->_outName.setPort(outName);
    proc->addOutput(outName);

    proc->postCreate(pipeline);

    return proc;
}

std::string InstallExtensionProcessor::describe() const {
    return fmt::format("InstallExtensionProcessor @={}", fmt::ptr(this));
}

void InstallExtensionProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    markAsPrepared();
}

void InstallExtensionProcessor::reset() {
    markAsReset();
}

void InstallExtensionProcessor::execute() {
    ExtensionManager* ext = _ctxt->getExtensions();
    if (!ext) {
        throw PipelineException("Extension manager is not available");
    }

    ext->installExtension(_extensionName);

    using ColumnString = ColumnConst<types::String::Primitive>;
    ColumnString* colName = _outName.getValue()->as<ColumnString>();
    colName->set(_extensionName);

    _outName.getPort()->writeData();
    finish();
}
