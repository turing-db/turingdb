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
    _writeTx = ctxt->getWriteTransaction();

    if (_type == ChangeOpType::NEW) {
        _changeInfo = std::string {ctxt->getGraphName()};
    } else {
        _changeInfo = ctxt->getChangeID();
    }
}

void ChangeStep::execute() {
    switch (_type) {
        case ChangeOpType::NEW:
            if (auto res = createChange(); !res) {
                throw PipelineException(fmt::format("Failed to create change: {}", res.error().fmtMessage()));
            }
            break;
        case ChangeOpType::SUBMIT:
            if (auto res = acceptChange(); !res) {
                throw PipelineException(fmt::format("Failed to submit change: {}", res.error().fmtMessage()));
            }
            break;
        case ChangeOpType::DELETE:
            if (auto res = deleteChange(); !res) {
                throw PipelineException(fmt::format("Failed to delete change: {}", res.error().fmtMessage()));
            }
            break;
        case ChangeOpType::LIST:
            listChanges();
            break;
        case ChangeOpType::_SIZE:
            throw PipelineException("ChangeStep: Unknown change type");
    }
}

void ChangeStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "ChangeStep";
    ss << " changeType=" << ChangeOpTypeName::value(_type);
    descr.assign(ss.str());
}

ChangeResult<ChangeID> ChangeStep::createChange() const {
    Profile profile {"ChangeStep::createChange"};


    if (!std::holds_alternative<std::string>(_changeInfo)) {
        throw PipelineException("ChangeStep: Change info must contain the graph name");
    }

    const auto& graphName = std::get<std::string>(_changeInfo);

    auto res = _sysMan->newChange(graphName);
    if (res) {
        auto* change = _sysMan->getChangeManager().getChange(res.value()).value();
        _output->push_back(change);
    }

    return res;
}

ChangeResult<void> ChangeStep::acceptChange() const {
    Profile profile {"ChangeStep::acceptChange"};

    if (!std::holds_alternative<ChangeID>(_changeInfo)) {
        throw PipelineException("ChangeStep: Change info must contain the change hash");
    }

    return _sysMan->getChangeManager().acceptChange(_writeTx->changeAccessor(), *_jobSystem);
}

ChangeResult<void> ChangeStep::deleteChange() const {
    Profile profile {"ChangeStep::deleteChange"};

    if (!std::holds_alternative<ChangeID>(_changeInfo)) {
        throw PipelineException("ChangeStep: Change info must contain the change hash");
    }

    ChangeID changeID = std::get<ChangeID>(_changeInfo);
    return _sysMan->getChangeManager().deleteChange(changeID);
}

void ChangeStep::listChanges() const {
    Profile profile {"ChangeStep::listChanges"};

    if (!_output) {
        throw PipelineException("ChangeStep: List changes requires an allocated column of changes");
    }

    _sysMan->getChangeManager().listChanges(_output->getRaw());
}
