#include "ChangeAccessor.h"

#include "Change.h"
#include "views/GraphView.h"
#include "versioning/VersionController.h"

using namespace db;

CommitBuilder* ChangeAccessor::getTip() const {
    return _change->_tip;
}

auto ChangeAccessor::begin() const {
    return _change->_commits.cbegin();
}
auto ChangeAccessor::end() const {
    return _change->_commits.cend();
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

std::span<const std::unique_ptr<CommitBuilder>> ChangeAccessor::pendingCommits() const {
    return std::span<const std::unique_ptr<CommitBuilder>> {_change->_commits.data(),
                                                            _change->_commits.size()};
}

ChangeAccessor::ChangeAccessor(Change* change)
    : _lock(change->_mutex),
    _change(change)
{
}
