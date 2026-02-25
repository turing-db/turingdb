#include "Transaction.h"

#include "views/GraphView.h"
#include "reader/GraphReader.h"
#include "versioning/CommitBuilder.h"

using namespace db;

FrozenCommitTx::FrozenCommitTx() = default;

FrozenCommitTx::~FrozenCommitTx() = default;

GraphView FrozenCommitTx::viewGraph() const {
    return GraphView(_data.get());
}

GraphReader FrozenCommitTx::readGraph() const {
    return viewGraph().read();
}

CommitHash FrozenCommitTx::getCommitHash() const {
    return _data->hash();
}

PendingCommitReadTx::PendingCommitReadTx() = default;

PendingCommitReadTx::~PendingCommitReadTx() = default;

PendingCommitReadTx::PendingCommitReadTx(ChangeAccessor&& changeAccessor,
                                         const CommitBuilder* commitBuilder)
    : _changeAccessor(std::move(changeAccessor)),
    _commitBuilder(commitBuilder)
{
}

GraphView PendingCommitReadTx::viewGraph() const {
    return GraphView {&_commitBuilder->commitData()};
}

GraphReader PendingCommitReadTx::readGraph() const {
    return GraphView {&_commitBuilder->commitData()}.read();
}

CommitHash PendingCommitReadTx::getCommitHash() const {
    return _commitBuilder->hash();
}

PendingCommitWriteTx::PendingCommitWriteTx() = default;

PendingCommitWriteTx::~PendingCommitWriteTx() = default;

PendingCommitWriteTx::PendingCommitWriteTx(ChangeAccessor&& changeAccessor,
                                           CommitBuilder* commitBuilder)
    : _changeAccessor(std::move(changeAccessor)),
    _commitBuilder(commitBuilder)
{
}

GraphView PendingCommitWriteTx::viewGraph() const {
    return GraphView {&_commitBuilder->commitData()};
}

GraphReader PendingCommitWriteTx::readGraph() const {
    return GraphView {&_commitBuilder->commitData()}.read();
}

CommitHash PendingCommitWriteTx::getCommitHash() const {
    return _commitBuilder->hash();
}

bool Transaction::isValid() const {
    return std::visit([](auto&& tx) { return tx.isValid(); }, _tx);
}

GraphView Transaction::viewGraph() const {
    return std::visit([](auto&& tx) { return tx.viewGraph(); }, _tx);
}

GraphReader Transaction::readGraph() const {
    return std::visit([](auto&& tx) { return tx.readGraph(); }, _tx);
}

CommitHash Transaction::getCommitHash() const {
    return std::visit([](auto&& tx) { return tx.getCommitHash(); }, _tx);
}
