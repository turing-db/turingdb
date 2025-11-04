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
    ChangeManager(VersionController*);
    ~ChangeManager();

    ChangeManager(const ChangeManager&) = delete;
    ChangeManager(ChangeManager&&) = delete;
    ChangeManager& operator=(const ChangeManager&) = delete;
    ChangeManager& operator=(ChangeManager&&) = delete;

    ChangeAccessor createChange(FrozenCommitTx baseTx = {});
    ChangeResult<Transaction> openTransaction(ChangeID id, CommitHash hash = CommitHash::head()) const;

    ChangeResult<void> submit(ChangeAccessor access, JobSystem& jobSystem);
    ChangeResult<void> remove(ChangeAccessor access);

    void listChanges(ColumnVector<ChangeID>* output) const;

private:
    mutable RWSpinLock _lock;

    VersionController* _versionController {nullptr};

    std::unordered_map<ChangeID, std::shared_ptr<Change>> _changes;
    std::atomic<ChangeID::ValueType> _nextChangeID {0};
};

}
