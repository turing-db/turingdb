#include "ChangeStep.h"

#include <sstream>

#include "ChangeManager.h"
#include "ExecutionContext.h"
#include "Profiler.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "SystemManager.h"
#include "PipelineException.h"

using namespace db;

ChangeStep::ChangeStep(ChangeOpType type,
                       ColumnVector<const Change*>* output)
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
            if (auto res = createChange(); !res) {
                throw PipelineException(fmt::format("Failed to create change: {}", res.error().fmtMessage()));
            }
        } break;
        case ChangeOpType::SUBMIT: {
            if (auto res = acceptChange(); !res) {
                throw PipelineException(fmt::format("Failed to submit change: {}", res.error().fmtMessage()));
            }
        } break;
        case ChangeOpType::DELETE: {
            if (auto res = deleteChange(); !res) {
                throw PipelineException(fmt::format("Failed to delete change: {}", res.error().fmtMessage()));
            }
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

ChangeResult<Change*> ChangeStep::createChange() const {
    Profile profile {"ChangeStep::createChange"};
    _output->clear();

    if (!std::holds_alternative<std::string>(_changeInfo)) {
        throw PipelineException("ChangeStep: Change info must contain the graph name");
    }

    const auto& graphName = std::get<std::string>(_changeInfo);

    const auto res = _sysMan->newChange(graphName);
    if (!res) {
        return res;
    }

    _output->push_back(res.value());

    return {};
}

ChangeResult<void> ChangeStep::acceptChange() const {
    Profile profile {"ChangeStep::acceptChange"};

    if (!_tx->writingPendingCommit()) {
        throw PipelineException("ChangeStep: Cannot accept change outside of a write transaction");
    }

    auto& tx = _tx->get<PendingCommitWriteTx>();

    if (!std::holds_alternative<ChangeID>(_changeInfo)) {
        throw PipelineException("ChangeStep: Change info must contain the change hash");
    }

    ChangeManager& changeMan = _sysMan->getChangeManager();
    
    return changeMan.acceptChange(tx.changeAccessor(), *_jobSystem);
}

ChangeResult<void> ChangeStep::deleteChange() const {
    Profile profile {"ChangeStep::deleteChange"};

    if (!_tx->writingPendingCommit()) {
        throw PipelineException("ChangeStep: Cannot delete change outside of a write transaction");
    }

    auto& tx = _tx->get<PendingCommitWriteTx>();

    if (!std::holds_alternative<ChangeID>(_changeInfo)) {
        throw PipelineException("ChangeStep: Change info must contain the change hash");
    }

    ChangeID changeID = std::get<ChangeID>(_changeInfo);
    return _sysMan->getChangeManager().deleteChange(tx.changeAccessor(), changeID);
}

void ChangeStep::listChanges() const {
    Profile profile {"ChangeStep::listChanges"};

    if (!_output) {
        throw PipelineException("ChangeStep: List changes requires an allocated column of changes");
    }

    _sysMan->getChangeManager().listChanges(_output->getRaw(), _graph);
}
