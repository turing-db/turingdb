#include "Commit.h"

#include "VersionController.h"
#include "Transaction.h"

using namespace db;

Commit::Commit() = default;

Commit::Commit(VersionController* controller,
               const WeakArc<CommitData>& data,
               const Commit* prevCommit,
               bool isMergeCommit)
    : _controller(controller),
    _hash(data->hash()),
    _data(data),
    _prevCommit(prevCommit),
    _mergeCommit(isMergeCommit)
{
}

Commit::~Commit() = default;

bool Commit::isHead() const {
    return _controller->getHeadHash() == _hash;
}

FrozenCommitTx Commit::openTransaction() const {
    return FrozenCommitTx(_data);
}

CommitView Commit::view() const {
    return CommitView(this);
}

std::unique_ptr<Commit> Commit::createNextCommit(VersionController* controller,
                                                 const WeakArc<CommitData>& data,
                                                 const Commit* prevCommit) {
    auto* ptr = new Commit(controller, data, prevCommit);

    const GraphView view = GraphView(&prevCommit->data());

    // Copy previous commit history and metadata
    ptr->_data->_history.newCommitHistoryFromPrevious(view.history());
    ptr->_data->_metadata = view.metadata();

    return std::unique_ptr<Commit>(ptr);
}

std::unique_ptr<Commit> Commit::createMergeCommit(VersionController* controller,
                                                  const WeakArc<CommitData>& data,
                                                  const Commit* prevCommit) {
    auto* ptr = new Commit(controller, data, prevCommit, true);

    const GraphView view = GraphView(&prevCommit->data());

    // Copy previous commit history and metadata
    ptr->_data->_history.newMergeCommitHistory(view.history());
    ptr->_data->_metadata = view.metadata();

    return std::unique_ptr<Commit>(ptr);
}
