#include "CommitProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "ExecutionContext.h"
#include "versioning/Transaction.h"
#include "versioning/ChangeAccessor.h"

#include "DataPartLimit.h"

#include "Profiler.h"
#include "BioAssert.h"

using namespace db;

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
    Profile profile("CommitProcessor::execute");

    Transaction* tx = _ctxt->getTransaction();
    bioassert(tx, "CommitProcessor: Transaction must be set");

    // Validate we're in a write transaction that's ready to commit
    if (!tx->writingPendingCommit()) {
        throw PipelineException("CommitProcessor: Cannot commit outside of a write transaction");
    }

    auto& writeTx = tx->get<PendingCommitWriteTx>();
    auto& access = writeTx.changeAccessor();

    SystemAccessor* system = _ctxt->getSystemAccessor();

    // Reject the commit if the graph already holds too many data parts, forcing the user
    // to run MERGE_DATAPARTS before accumulating more.
    throwIfTooManyDataParts(access, system->getConfig());

    // Perform the commit (core logic from old CommitStep)
    if (auto res = system->commitChange(access); !res) {
        throw PipelineException(fmt::format("CommitProcessor: Failed to commit: {}",
                                           res.error().fmtMessage()));
    }

    // Write empty output (commit succeeded)
    _output.getPort()->writeData();

    finish();
}
