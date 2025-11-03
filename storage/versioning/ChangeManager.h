#pragma once

#include <unordered_map>

#include "ChangeID.h"
#include "Transaction.h"

#include "RWSpinLock.h"
#include "ChangeResult.h"

namespace db {

class Change;
class VersionController;

template<typename T>
class ColumnVector;

class ChangeManager {
public:
    ChangeManager(VersionController*, ArcManager<DataPart>*, ArcManager<CommitData>*);
    ~ChangeManager();

    ChangeManager(const ChangeManager&) = delete;
    ChangeManager(ChangeManager&&) = delete;
    ChangeManager& operator=(const ChangeManager&) = delete;
    ChangeManager& operator=(ChangeManager&&) = delete;

    ChangeAccessor createChange(FrozenCommitTx baseTx = {});
    ChangeResult<Transaction> openTransaction(ChangeID id, CommitHash hash = CommitHash::head()) const;

    ChangeResult<void> submit(ChangeAccessor access, JobSystem& jobSystem);
    ChangeResult<void> deleteChange(ChangeAccessor access);

    ChangeResult<void> submit(ChangeID id, JobSystem& jobSystem);
    ChangeResult<void> deleteChange(ChangeID id);

    void listChanges(ColumnVector<ChangeID>* output) const;

private:
    mutable RWSpinLock _lock;

    VersionController* _versionController {nullptr};
    ArcManager<DataPart>* _dataPartManager {nullptr};
    ArcManager<CommitData>* _commitDataManager {nullptr};

    std::unordered_map<ChangeID, std::unique_ptr<Change>> _changes;
    std::atomic<ChangeID::ValueType> _nextChangeID {0};

    ChangeResult<void> submitImpl(ChangeID id, JobSystem& jobSystem);
    ChangeResult<void> deleteImpl(ChangeID id);
};

}
