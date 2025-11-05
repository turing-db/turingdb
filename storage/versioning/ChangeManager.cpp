#include "ChangeManager.h"

#include <shared_mutex>

#include "VersionController.h"
#include "Change.h"

#include "columns/ColumnVector.h"
#include "Profiler.h"
#include "versioning/CommitBuilder.h"

using namespace db;

namespace db {

struct ChangeHolder {
    Change _change;
    mutable std::mutex _mutex;
    bool _valid {true};

    ChangeHolder(ArcManager<DataPart>& partManager, ArcManager<CommitData>& commitDataManager,
                 ChangeID id, const WeakArc<const CommitData>& base)
        : _change(partManager, commitDataManager, id, base)
    {
    }

    ChangeAccessor acquire() {
        return ChangeAccessor {&_change, std::unique_lock {_mutex}};
    }
};

}

ChangeManager::ChangeManager(VersionController* versionController)
    : _versionController(versionController)
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

    ChangeID id {_nextChangeID.fetch_add(1)};

    auto& partManager = *_versionController->_partManager;
    auto& dataManager = *_versionController->_dataManager;

    auto changeHolder = std::make_shared<ChangeHolder>(partManager, dataManager, id, baseTx.commitData());

    _changes.emplace(id, changeHolder);

    return changeHolder->acquire();
}

ChangeResult<Transaction> ChangeManager::openTransaction(ChangeID changeID, CommitHash hash) const {
    Profile profile("ChangeManager::getChange");

    std::shared_ptr<ChangeHolder> changeHolder;

    {
        std::shared_lock lock(_lock);

        auto it = _changes.find(changeID);
        if (it == _changes.end()) {
            return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
        }

        changeHolder = it->second;
    }

    ChangeAccessor access = changeHolder->acquire();

    if (!changeHolder->_valid) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    Change& change = changeHolder->_change;

    // Step 6. Return the transaction

    //      6.1. Check if we are reading the tip
    if (hash == CommitHash::head()) {
        return PendingCommitWriteTx {std::move(access), change._tip};
    }

    //      6.2. Check if we are reading a pending commit
    auto& commitOffsets = change._commitOffsets;
    auto& commitBuilders = change._commits;
    auto commitIt = commitOffsets.find(hash);
    if (commitIt != commitOffsets.end()) {
        return PendingCommitReadTx {std::move(access), commitBuilders[commitIt->second].get()};
    }

    //      6.3. Check if we are reading a frozen commit
    std::span commits = change._base->history().commits();
    for (const auto& commit : commits) {
        if (commit.hash() == hash) {
            return FrozenCommitTx {commit.data()};
        }
    }

    return ChangeError::result(ChangeErrorType::COMMIT_NOT_FOUND);
}

ChangeResult<void> ChangeManager::submit(ChangeAccessor access, JobSystem& jobSystem) {
    Profile profile("ChangeManager::submitChange");

    std::shared_ptr<ChangeHolder> changeHolder;

    {
        std::unique_lock lock(_lock);

        const ChangeID id = access.getID();

        // Step 1. Retrieve the change
        auto it = _changes.find(id);
        if (it == _changes.end()) {
            // Should never happen
            return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
        }

        // Step 3. Erase the change from the map so that no one else can access it
        changeHolder = it->second;
        _changes.erase(it);
    }

    Change& change = changeHolder->_change;

    // Step 4. Check if the change was already submitted
    if (!changeHolder->_valid) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    // Step 5. - Tag the change as submitted and submit it
    changeHolder->_valid = false;
    const CommitResult<void> res = _versionController->submitChange(&change, jobSystem);

    if (!res) {
        // TODO: What should we do here? The change disappears if the submittion fails, do we want
        // to keep it around? So that the user can re-try later on?
        return ChangeError::result(ChangeErrorType::COULD_NOT_ACCEPT_CHANGE, res.error());
    }

    return {};
}

ChangeResult<void> ChangeManager::remove(ChangeAccessor access) {
    Profile profile("ChangeManager::deleteChange");

    std::unique_lock lock(_lock);

    const ChangeID id = access.getID();

    // Step 1. Retrieve the change
    auto it = _changes.find(id);
    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    // Step 2. Remove the change from the map so that no one else can access it
    it->second->_valid = false;
    _changes.erase(it);

    // At the end of the scope, the change is marked for deletion and unlocked.
    // Once all readers are done, the change is destroyed

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

void ChangeManager::clear() {
    std::unique_lock lock(_lock);

    for (auto it = _changes.begin(); it != _changes.end();) {

        // Increment the reference count to keep the change alive during logic deletion
        std::shared_ptr<ChangeHolder> changeHolder = it->second;

        // Acquire a lock and mark the change as invalid
        ChangeAccessor access = changeHolder->acquire();
        changeHolder->_valid = false;

        // Erase the change from the map so that no one else can access it
        it = _changes.erase(it);

        // At the end of this scope, the change is marked for deletion and unlocked.
        // Once all readers are done, the change is destroyed
    }

    _nextChangeID.store(0);
}
