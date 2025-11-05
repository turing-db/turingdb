#include "ChangeAccessor.h"

#include "Change.h"
#include "views/GraphView.h"
#include "versioning/VersionController.h"

using namespace db;

ChangeAccessor::ChangeAccessor(Change* change,
                               std::unique_lock<std::mutex> lock)
    : _change(change),
    _lock(std::move(lock))
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
