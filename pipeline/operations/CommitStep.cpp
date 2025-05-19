#include "CommitStep.h"

#include <sstream>

#include "ExecutionContext.h"
#include "PipelineException.h"
#include "versioning/Transaction.h"

using namespace db;

void CommitStep::prepare(ExecutionContext* ctxt) {
    _tx = ctxt->getWriteTransaction();
    if (!_tx) {
        throw PipelineException("CommitStep: No write transaction");
    }

    _jobSystem = ctxt->getJobSystem();
}

void CommitStep::execute() {
    auto& access = _tx->changeAccessor();
    if (auto res = access.commit(*_jobSystem); !res) {
        throw PipelineException(fmt::format("CommitStep: Failed to commit: {}", res.error().fmtMessage()));
    }
}

void CommitStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "CommitStep";
    descr.assign(ss.str());
}
