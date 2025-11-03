#include "Commit.h"

#include "Transaction.h"

using namespace db;

Commit::Commit() = default;

Commit::Commit(const WeakArc<CommitData>& data)
    : _hash(data->hash()),
    _data(data)
{
}

Commit::~Commit() = default;

FrozenCommitTx Commit::openTransaction() const {
    return FrozenCommitTx {_data};
}

CommitView Commit::view() const {
    return CommitView {this};
}

std::unique_ptr<Commit> Commit::createNextCommit(const WeakArc<CommitData>& data,
                                                 const CommitView& prevCommit) {
    auto* ptr = new Commit {data};

    // Copy previous commit history and metadata
    ptr->_data->_history.newCommitHistoryFromPrevious(prevCommit.history());
    ptr->_data->_history.pushCommit(ptr->view());
    ptr->_data->_metadata = prevCommit.metadata();

    return std::unique_ptr<Commit> {ptr};
}
