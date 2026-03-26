#include "CreateNodePropertyIndexProcessor.h"

#include <string_view>

#include "ExecutionContext.h"
#include "PipelineException.h"

#include "PipelineExecutor.h"
#include "PipelinePort.h"

#include "versioning/Transaction.h"

#include "FatalException.h"

using namespace db;

CreateNodePropertyIndexProcessor::CreateNodePropertyIndexProcessor(std::string_view indexName,
                                                                   std::string_view propName)
    : _indexName(indexName),
    _propertyName(propName)
{
}

std::string CreateNodePropertyIndexProcessor::describe() const {
    return fmt::format("CreatePropertyIndexProcessor @={}", fmt::ptr(this));
}

CreateNodePropertyIndexProcessor* CreateNodePropertyIndexProcessor::create(PipelineV2* pipeline,
                                                                           std::string_view indexName,
                                                                           std::string_view propName) {
    CreateNodePropertyIndexProcessor* proc = new CreateNodePropertyIndexProcessor(indexName, propName);

    {
        PipelineOutputPort* out = PipelineOutputPort::create(pipeline, proc);

        proc->_output.setPort(out);
        proc->addOutput(out);
    }

    proc->postCreate(pipeline);

    return proc;
}

void CreateNodePropertyIndexProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    const Transaction* rawTx = ctxt->getTransaction();
    if (!rawTx) {
        throw FatalException(
            "Attempted to create node property index without transaction.");
    }

    if (!rawTx->writingPendingCommit()) {
        throw PipelineException(
            "Create index: Cannot perform writes outside of a write transaction");
    }
    markAsPrepared();
}

void CreateNodePropertyIndexProcessor::reset() {
    markAsReset();
}

void CreateNodePropertyIndexProcessor::execute() {
    throw PipelineException("Property indexes are not yet supported.");
}
