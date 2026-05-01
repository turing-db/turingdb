#pragma once

#include "QueryConfig.h"
#include "QueryCallbacks.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace db {

class LocalMemory;
class SystemManager;

class InterpreterContext {
public:
    InterpreterContext(LocalMemory* mem,
                       const QueryCallbacks* callbacks,
                       SystemManager* systemManager,
                       CommitHash commitHash = CommitHash::head(),
                       ChangeID changeID = ChangeID::head())
        : _mem(mem),
        _callbacks(callbacks),
        _systemManager(systemManager),
        _commitHash(commitHash),
        _changeID(changeID)
    {
    }

    ~InterpreterContext() = default;

    LocalMemory* getLocalMemory() const { return _mem; }
    SystemManager* getSystemManager() const { return _systemManager; }
    const QueryCallbacks* getQueryCallbacks() const { return _callbacks; }
    const QueryConfig* getQueryConfig() const { return _queryConfig; }
    CommitHash getCommitHash() const { return _commitHash; }
    ChangeID getChangeID() const { return _changeID; }

    void setQueryConfig(const QueryConfig* config) { _queryConfig = config; }

private:
    LocalMemory* _mem {nullptr};
    const QueryCallbacks* _callbacks {nullptr};
    SystemManager* _systemManager {nullptr};
    const QueryConfig* _queryConfig {nullptr};
    CommitHash _commitHash;
    ChangeID _changeID;
};

}
