#include "ChangeManager.h"

#include <shared_mutex>

#include "VersionController.h"
#include "Change.h"

#include "columns/ColumnVector.h"
#include "Profiler.h"
#include "versioning/CommitBuilder.h"

using namespace db;

ChangeManager::ChangeManager(VersionController* versionController,
                             ArcManager<DataPart>* partManager,
                             ArcManager<CommitData>* commitDataManager)
    : _versionController(versionController),
      _dataPartManager(partManager),
      _commitDataManager(commitDataManager) {
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

    _changes[id] = {std::move(change)};

    return ptr->access();
}

ChangeResult<Transaction> ChangeManager::openTransaction(ChangeID changeID, CommitHash hash) const {
    Profile profile("ChangeManager::getChange");

    std::shared_lock lock(_lock);

    // Step 1. Retrieve the change
    auto it = _changes.find(changeID);
    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    // Step 2. Increment the reference count to keep the change alive
    std::shared_ptr<Change> change = it->second;

    // Step 3. Unlock the ChangeManager.
    _lock.unlock();

    // Step 4. Lock the change
    ChangeAccessor access {change->access()};

    // Step 5. Check if it was already submitted
    if (change->isSubmitted()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    // Step 6. Return the transaction

    //      6.1. Check if we are reading the tip
    if (hash == CommitHash::head()) {
        return PendingCommitWriteTx {std::move(access), change->_tip};
    }

    //      6.2. Check if we are reading a pending commit
    auto& commitOffsets = change->_commitOffsets;
    auto& commitBuilders = change->_commits;
    auto commitIt = commitOffsets.find(hash);
    if (commitIt != commitOffsets.end()) {
        return PendingCommitReadTx {std::move(access), commitBuilders[commitIt->second].get()};
    }

    //      6.3. Check if we are reading a frozen commit
    std::span commits = change->_base->history().commits();
    for (const auto& commit : commits) {
        if (commit.hash() == hash) {
            return FrozenCommitTx {commit.data()};
        }
    }

    return ChangeError::result(ChangeErrorType::COMMIT_NOT_FOUND);
}

ChangeResult<void> ChangeManager::submit(ChangeAccessor access, JobSystem& jobSystem) {
    Profile profile("ChangeManager::submitChange");

    std::unique_lock lock(_lock);

    const ChangeID id = access.getID();

    // Step 1. Retrieve the change
    auto it = _changes.find(id);
    if (it == _changes.end()) {
        // Should never happen
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    // Step 2. Increment the reference count to keep the change alive
    std::shared_ptr<Change> change = it->second;

    // Step 3. Erase the change from the map so that no one else can access it
    _changes.erase(it);

    // Step 4. Unlock the ChangeManager
    _lock.unlock();

    // Step 5. Check if the change was already submitted
    if (change->isSubmitted()) {
        // Should never happen
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    access.release();
    const auto res = _versionController->submitChange(change.get(), jobSystem);

    if (!res) {
        // TODO, check if we should delete the change here
        return ChangeError::result(ChangeErrorType::COULD_NOT_ACCEPT_CHANGE, res.error());
    }

    return {};
}

ChangeResult<void> ChangeManager::remove(ChangeAccessor access) {
    Profile profile("ChangeManager::deleteChange");

    std::unique_lock lock(_lock);

    const ChangeID id = access.getID();

    auto it = _changes.find(id);
    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    _changes.erase(it);

    return {};
}

void ChangeManager::listChanges(ColumnVector<ChangeID>* output) const {
    Profile profile("ChangeManager::listChanges");

    std::shared_lock lock(_lock);

    output->clear();

    for (const auto& change : _changes) {
        output->push_back(change.first);
    }
}
