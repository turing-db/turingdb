#include "ChangeAccessor.h"

#include "Change.h"
#include "views/GraphView.h"
#include "versioning/VersionController.h"

using namespace db;

ChangeAccessor::ChangeAccessor(Change* change)
    : _lock(change->_mutex),
    _change(change)
{
}

CommitBuilder* ChangeAccessor::getTip() const {
    return _change->_tip;
}

CommitResult<void> ChangeAccessor::commit(JobSystem& jobsystem) {
    return _change->commit(jobsystem);
}

ChangeID ChangeAccessor::getID() const {
    return _change->_id;
}

GraphView ChangeAccessor::viewGraph(CommitHash commitHash) const {
    return _change->viewGraph(commitHash);
}
