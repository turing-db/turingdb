#include "ChangeManager.h"

#include <shared_mutex>

#include "VersionController.h"
#include "Change.h"

#include "columns/ColumnVector.h"
#include "Profiler.h"

using namespace db;

ChangeManager::ChangeManager(VersionController* versionController,
                             ArcManager<DataPart>* partManager,
                             ArcManager<CommitData>* commitDataManager)
    : _versionController(versionController),
    _dataPartManager(partManager),
    _commitDataManager(commitDataManager)
{
}

ChangeManager::~ChangeManager() = default;

ChangeAccessor ChangeManager::createChange(FrozenCommitTx baseTx) {
    Profile profile("ChangeManager::createChange");

    std::unique_lock lock(_lock);

    if (!baseTx.isValid()) {
        // Branching out of head
        baseTx = _versionController->openTransaction();
    }

    std::unique_ptr change = Change::create(*_dataPartManager,
                                            *_commitDataManager,
                                            ChangeID {_nextChangeID.fetch_add(1)},
                                            baseTx.commitData());

    Change* ptr = change.get();
    ChangeID id = change->id();
    _changes[id] = std::move(change);

    return ptr->access();
}

ChangeResult<Transaction> ChangeManager::openTransaction(ChangeID changeID, CommitHash hash) const {
    Profile profile("ChangeManager::getChange");

    std::shared_lock lock(_lock);

    auto it = _changes.find(changeID);
    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    if (hash == CommitHash::head()) {
        return it->second->openWriteTransaction();
    }

    auto tx = it->second->openReadTransaction(hash);
    if (!tx.isValid()) {
        return ChangeError::result(ChangeErrorType::COMMIT_NOT_FOUND);
    }

    return tx;
}

ChangeResult<void> ChangeManager::submit(ChangeAccessor access, JobSystem& jobSystem) {
    Profile profile("ChangeManager::submitChange");

    std::unique_lock lock(_lock);
    const ChangeID id = access.getID();
    access.release();

    return submitImpl(id, jobSystem);
}

ChangeResult<void> ChangeManager::deleteChange(ChangeAccessor access) {
    Profile profile("ChangeManager::deleteChange");

    std::unique_lock lock(_lock);
    const ChangeID id = access.getID();
    access.release();

    return deleteImpl(id);
}

ChangeResult<void> ChangeManager::submit(ChangeID id, JobSystem& jobSystem) {
    Profile profile("ChangeManager::submitChange");

    std::unique_lock lock(_lock);
    return submitImpl(id, jobSystem);
}

ChangeResult<void> ChangeManager::deleteChange(ChangeID changeID) {
    Profile profile("ChangeManager::deleteChange");

    std::unique_lock lock(_lock);
    return deleteImpl(changeID);
}

void ChangeManager::listChanges(ColumnVector<ChangeID>* output) const {
    std::shared_lock lock(_lock);
    output->clear();

    for (const auto& change : _changes) {
        output->push_back(change.first);
    }
}

ChangeResult<void> ChangeManager::submitImpl(ChangeID id, JobSystem& jobSystem) {
    std::unique_lock lock(_lock);

    auto it = _changes.find(id);

    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    const auto res = _versionController->submitChange(it->second.get(), jobSystem);

    _changes.erase(it);

    if (!res) {
        // TODO, check if we should delete the change here
        return ChangeError::result(ChangeErrorType::COULD_NOT_ACCEPT_CHANGE, res.error());
    }

    return {};
}

ChangeResult<void> ChangeManager::deleteImpl(ChangeID changeID) {
    std::unique_lock lock(_lock);

    auto it = _changes.find(changeID);

    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    _changes.erase(it);

    return {};
}
