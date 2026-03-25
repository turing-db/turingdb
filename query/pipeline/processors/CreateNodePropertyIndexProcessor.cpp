#include "CreateNodePropertyIndexProcessor.h"

#include <string_view>

#include "ExecutionContext.h"
#include "PipelineException.h"

#include "PipelineExecutor.h"
#include "PipelinePort.h"

#include "versioning/Transaction.h"

#include "FatalException.h"

using namespace db;

CreateNodePropertyIndexProcessor::CreateNodePropertyIndexProcessor(std::string_view indexName)
    : _indexName(indexName)
{
}

std::string CreateNodePropertyIndexProcessor::describe() const {
    return fmt::format("CreatePropertyIndexProcessor @={}", fmt::ptr(this));
}

CreateNodePropertyIndexProcessor* CreateNodePropertyIndexProcessor::create(PipelineV2* pipeline,
                                                                           std::string_view indexName) {
    CreateNodePropertyIndexProcessor* proc = new CreateNodePropertyIndexProcessor(indexName);

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
        throw FatalException("Attempted to prepare WriteProcessor in execution context "
                             "without transaction.");
    }

    if (!rawTx->writingPendingCommit()) {
        throw PipelineException("WriteProcessor: Cannot perform writes outside of a write transaction");
    }
    markAsPrepared();
}

void CreateNodePropertyIndexProcessor::reset() {
    markAsReset();
}

void CreateNodePropertyIndexProcessor::execute() {
    throw PipelineException("Property indexes are not yet supported.");
}
