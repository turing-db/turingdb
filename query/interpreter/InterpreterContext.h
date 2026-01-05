#pragma once

#include "QueryCallback.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace db {
class LocalMemory;
}

namespace db {

class ProcedureBlueprintMap;

class InterpreterContext {
public:
    InterpreterContext(LocalMemory* mem,
                       QueryCallbackV2 callback,
                       const ProcedureBlueprintMap* procedures,
                       CommitHash commitHash = CommitHash::head(),
                       ChangeID changeID = ChangeID::head())
        : _mem(mem),
        _callback(callback),
        _procedures(procedures),
        _commitHash(commitHash),
        _changeID(changeID)
    {
    }

    ~InterpreterContext() = default;

    LocalMemory* getLocalMemory() const { return _mem; }
    const ProcedureBlueprintMap* getProcedures() const { return _procedures; }
    QueryCallbackV2 getQueryCallback() const { return _callback; }
    CommitHash getCommitHash() const { return _commitHash; }
    ChangeID getChangeID() const { return _changeID; }

private:
    LocalMemory* _mem {nullptr};
    QueryCallbackV2 _callback;
    const ProcedureBlueprintMap* _procedures {nullptr};
    CommitHash _commitHash;
    ChangeID _changeID;
};

}
