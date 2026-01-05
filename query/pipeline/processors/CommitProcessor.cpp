#include "CommitProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "ExecutionContext.h"
#include "versioning/Transaction.h"
#include "versioning/ChangeAccessor.h"

#include "Profiler.h"
#include "BioAssert.h"

using namespace db::v2;

CommitProcessor::CommitProcessor()
{
}

CommitProcessor::~CommitProcessor() {
}

std::string CommitProcessor::describe() const {
    return fmt::format("CommitProcessor @={}", fmt::ptr(this));
}

CommitProcessor* CommitProcessor::create(PipelineV2* pipeline) {
    CommitProcessor* proc = new CommitProcessor();

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, proc);
    proc->_output.setPort(output);
    proc->addOutput(output);

    proc->postCreate(pipeline);

    return proc;
}

void CommitProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void CommitProcessor::reset() {
}

void CommitProcessor::execute() {
    Profile profile {"CommitProcessor::execute"};

    Transaction* tx = _ctxt->getTransaction();
    bioassert(tx, "CommitProcessor: Transaction must be set");

    // Validate we're in a write transaction that's ready to commit
    if (!tx->writingPendingCommit()) {
        throw PipelineException("CommitProcessor: Cannot commit outside of a write transaction");
    }

    auto& writeTx = tx->get<PendingCommitWriteTx>();
    auto& access = writeTx.changeAccessor();

    JobSystem* jobSystem = _ctxt->getJobSystem();
    bioassert(jobSystem, "CommitProcessor: Job system must be set");

    // Perform the commit (core logic from old CommitStep)
    if (auto res = access.commit(*jobSystem); !res) {
        throw PipelineException(fmt::format("CommitProcessor: Failed to commit: {}",
                                           res.error().fmtMessage()));
    }

    // Write empty output (commit succeeded)
    _output.getPort()->writeData();

    finish();
}
