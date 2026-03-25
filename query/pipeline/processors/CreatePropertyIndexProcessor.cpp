#include "CreatePropertyIndexProcessor.h"

#include <string_view>

#include "ExecutionContext.h"
#include "PipelineException.h"

#include "PipelineExecutor.h"
#include "PipelinePort.h"

#include "versioning/Transaction.h"

#include "FatalException.h"

using namespace db;

CreatePropertyIndexProcessor::CreatePropertyIndexProcessor(std::string_view propertyName)
    : _propertyName(propertyName)
{
}

std::string CreatePropertyIndexProcessor::describe() const {
    return fmt::format("CreatePropertyIndexProcessor @={}", fmt::ptr(this));
}

CreatePropertyIndexProcessor* CreatePropertyIndexProcessor::create(PipelineV2* pipeline,
                                                                   std::string_view propertyName) {
    CreatePropertyIndexProcessor* proc = new CreatePropertyIndexProcessor(propertyName);

    {
        PipelineOutputPort* out = PipelineOutputPort::create(pipeline, proc);

        proc->_output.setPort(out);
        proc->addOutput(out);
    }

    proc->postCreate(pipeline);

    return proc;
}

void CreatePropertyIndexProcessor::prepare(ExecutionContext* ctxt) {
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

void CreatePropertyIndexProcessor::reset() {
    markAsReset();
}

void CreatePropertyIndexProcessor::execute() {
    throw PipelineException("Property indexes are not yet supported.");
}
