#include "ChangeProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "ChangeManager.h"
#include "ExecutionContext.h"
#include "SystemManager.h"
#include "versioning/Change.h"
#include "versioning/Transaction.h"

#include "Profiler.h"
#include "BioAssert.h"

using namespace db;

ChangeProcessor::ChangeProcessor(ChangeOp op)
    : _op(op)
{
}

ChangeProcessor::~ChangeProcessor() {
}

std::string ChangeProcessor::describe() const {
    return fmt::format("ChangeProcessor @={}", fmt::ptr(this));
}

ChangeProcessor* ChangeProcessor::create(PipelineV2* pipeline, ChangeOp op) {
    ChangeProcessor* proj = new ChangeProcessor(op);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, proj);
    proj->_output.setPort(output);
    proj->addOutput(output);

    proj->postCreate(pipeline);

    return proj;
}

void ChangeProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void ChangeProcessor::reset() {
    markAsReset();
}

void ChangeProcessor::execute() {
    bioassert(_changeIDCol, "ChangeProcessor: ChangeID column must be set");
    bioassert(!_ctxt->getGraphName().empty(), "ChangeProcessor: Graph name must be set");
    bioassert(_ctxt->getSystemManager(), "ChangeProcessor: System manager must be set");
    bioassert(_ctxt->getJobSystem(), "ChangeProcessor: Job system must be set");

    switch (_op) {
        case ChangeOp::NEW: {
            createChange();
        } break;
        case ChangeOp::SUBMIT: {
            submitChange();
        } break;
        case ChangeOp::DELETE: {
            deleteChange();
        } break;
        case ChangeOp::LIST: {
            listChanges();
        } break;
    }

    PipelineOutputPort* outputPort = _output.getPort();
    outputPort->writeData();

    finish();
}

void ChangeProcessor::createChange() const {
    Profile profile {"ChangeProcessor::createChange"};

    std::string graphName {_ctxt->getGraphName()};
    SystemManager* sysMan = _ctxt->getSystemManager();

    auto res = sysMan->newChange(graphName);
    if (!res) {
        throw PipelineException(fmt::format("Failed to create change: {}", res.error().fmtMessage()));
    }

    _changeIDCol->resize(1);
    _changeIDCol->set(0, res.value()->id());
}

void ChangeProcessor::submitChange() const {
    Profile profile {"ChangeProcessor::submitChange"};

    Transaction* abstractTx = _ctxt->getTransaction();
    bioassert(abstractTx, "ChangeProcessor: Transaction must be set");

    if (!abstractTx->writingPendingCommit()) {
        throw PipelineException("ChangeProcessor: Transaction must be writing a pending commit");
    }

    auto& tx = abstractTx->get<PendingCommitWriteTx>();

    ChangeAccessor& accessor = tx.changeAccessor();
    bioassert(accessor.isValid(), "ChangeProcessor: Change accessor must be valid");

    SystemManager* sysMan = _ctxt->getSystemManager();
    JobSystem* jobSystem = _ctxt->getJobSystem();
    ChangeManager& changeMan = sysMan->getChangeManager();

    const ChangeID changeID = accessor.getID();

    // Step 1: Submit the change
    if (const auto res = changeMan.submitChange(accessor, *jobSystem); !res) {
        throw PipelineException(fmt::format("Failed to submit change: {}", res.error().fmtMessage()));
    }

    const std::string graphName {_ctxt->getGraphName()};

    // Step 2: Dump newly created commits
    if (const auto res = sysMan->dumpGraph(graphName); !res) {
        throw PipelineException(fmt::format("Failed to dump new commits: {}", res.error().fmtMessage()));
    }

    _changeIDCol->resize(1);
    _changeIDCol->set(0, changeID);
}

void ChangeProcessor::deleteChange() const {
    Profile profile {"ChangeProcessor::deleteChange"};

    Transaction* abstractTx = _ctxt->getTransaction();
    bioassert(abstractTx, "ChangeProcessor: Transaction must be set");

    if (!abstractTx->writingPendingCommit()) {
        throw PipelineException("ChangeProcessor: Transaction must be writing a pending commit");
    }

    auto& tx = abstractTx->get<PendingCommitWriteTx>();

    ChangeAccessor& accessor = tx.changeAccessor();
    bioassert(accessor.isValid(), "ChangeProcessor: Change accessor must be valid");

    SystemManager* sysMan = _ctxt->getSystemManager();
    ChangeManager& changeMan = sysMan->getChangeManager();

    const ChangeID changeID = accessor.getID();

    if (const auto res = changeMan.deleteChange(accessor, changeID); !res) {
        throw PipelineException(fmt::format("Failed to delete change: {}", res.error().fmtMessage()));
    }

    _changeIDCol->resize(1);
    _changeIDCol->set(0, changeID);
}

void ChangeProcessor::listChanges() const {
    Profile profile {"ChangeProcessor::listChanges"};

    SystemManager* sysMan = _ctxt->getSystemManager();

    std::vector<const Change*> changes;
    const std::string graphName {_ctxt->getGraphName()};

    const Graph* graph = sysMan->getGraph(graphName);
    bioassert(graph, "ChangeProcessor: Graph must exist");

    sysMan->getChangeManager().listChanges(changes, graph);

    _changeIDCol->clear();
    for (const auto* change : changes) {
        _changeIDCol->push_back(change->id());
    }
}
