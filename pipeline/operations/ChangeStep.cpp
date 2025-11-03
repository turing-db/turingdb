#include "ChangeStep.h"

#include <sstream>

#include "ExecutionContext.h"
#include "Graph.h"
#include "Profiler.h"
#include "versioning/ChangeManager.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "SystemManager.h"
#include "PipelineException.h"

using namespace db;

ChangeStep::ChangeStep(ChangeOpType type,
                       ColumnVector<ChangeID>* output)
    : _type(type),
    _output(output)
{
}

ChangeStep::~ChangeStep() {
}

void ChangeStep::prepare(ExecutionContext* ctxt) {
    _sysMan = ctxt->getSystemManager();
    _jobSystem = ctxt->getJobSystem();
    _view = ctxt->getGraphView();
    _tx = ctxt->getTransaction();
    _graph = _sysMan->getGraph(std::string(ctxt->getGraphName()));

    switch (_type) {
        case ChangeOpType::NEW: {
            _changeInfo = std::string {ctxt->getGraphName()};
        } break;
        case ChangeOpType::SUBMIT:
        case ChangeOpType::DELETE:
            _changeInfo = ctxt->getChangeID();
        case ChangeOpType::LIST: {
        } break;
        case ChangeOpType::_SIZE: {
            throw PipelineException("ChangeStep: Unknown change type");
        } break;
    }
}

void ChangeStep::execute() {
    switch (_type) {
        case ChangeOpType::NEW: {
            createChange();
        } break;
        case ChangeOpType::SUBMIT: {
            submitChange();
        } break;
        case ChangeOpType::DELETE: {
            deleteChange();
        } break;
        case ChangeOpType::LIST: {
            listChanges();
        } break;
        case ChangeOpType::_SIZE: {
            throw PipelineException("ChangeStep: Unknown change type");
        } break;
    }
}

void ChangeStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "ChangeStep";
    ss << " changeType=" << ChangeOpTypeName::value(_type);
    descr.assign(ss.str());
}

void ChangeStep::createChange() const {
    Profile profile {"ChangeStep::createChange"};
    _output->clear();

    if (!std::holds_alternative<std::string>(_changeInfo)) {
        throw PipelineException("ChangeStep: Change info must contain the graph name");
    }

    const auto& graphName = std::get<std::string>(_changeInfo);

    auto res = _sysMan->newChange(graphName);
    if (!res) {
        throw PipelineException(fmt::format("Failed to create change: {}", res.error().fmtMessage()));
    }

    _output->push_back(res.value().getID());
}

void ChangeStep::submitChange() const {
    Profile profile {"ChangeStep::submitChange"};

    if (!_tx->writingPendingCommit()) {
        throw PipelineException("ChangeStep: Cannot accept change outside of a write transaction");
    }

    auto& tx = _tx->get<PendingCommitWriteTx>();

    if (!std::holds_alternative<ChangeID>(_changeInfo)) {
        throw PipelineException("ChangeStep: Change info must contain the change hash");
    }

    ChangeManager& changeMan = _graph->getChangeManager();

    // Step 1: Submit the change
    if (const auto res = changeMan.submit(std::move(tx.changeAccessor()), *_jobSystem); !res) {
        throw PipelineException(fmt::format("Failed to submit change: {}", res.error().fmtMessage()));
    }

    // Step 2: Dump newly created commits
    if (const auto res = _sysMan->dumpGraph(_graph->getName()); !res) {
        throw PipelineException(fmt::format("Failed to dump new commits: {}", res.error().fmtMessage()));
    }
}

void ChangeStep::deleteChange() const {
    Profile profile {"ChangeStep::deleteChange"};

    if (!_tx->writingPendingCommit()) {
        throw PipelineException("ChangeStep: Cannot delete change outside of a write transaction");
    }

    auto& tx = _tx->get<PendingCommitWriteTx>();

    if (!std::holds_alternative<ChangeID>(_changeInfo)) {
        throw PipelineException("ChangeStep: Change info must contain the change hash");
    }

    ChangeManager& changeMan = _graph->getChangeManager();

    if (const auto res = changeMan.deleteChange(std::move(tx.changeAccessor())); !res) {
        throw PipelineException(fmt::format("Failed to delete change: {}", res.error().fmtMessage()));
    }
}

void ChangeStep::listChanges() const {
    Profile profile {"ChangeStep::listChanges"};

    if (!_output) {
        throw PipelineException("ChangeStep: List changes requires an allocated column of changes");
    }

    auto& changeMan = _graph->getChangeManager();
    changeMan.listChanges(_output);
}
