#pragma once

#include "QueryCallbacks.h"
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
                       QueryCallbacks& callbacks,
                       const ProcedureBlueprintMap* procedures,
                       CommitHash commitHash = CommitHash::head(),
                       ChangeID changeID = ChangeID::head())
        : _mem(mem),
        _callbacks(callbacks),
        _procedures(procedures),
        _commitHash(commitHash),
        _changeID(changeID)
    {
    }

    ~InterpreterContext() = default;

    LocalMemory* getLocalMemory() const { return _mem; }
    const ProcedureBlueprintMap* getProcedures() const { return _procedures; }
    QueryCallbacks& getQueryCallbacks() const { return _callbacks; }
    CommitHash getCommitHash() const { return _commitHash; }
    ChangeID getChangeID() const { return _changeID; }

private:
    LocalMemory* _mem {nullptr};
    QueryCallbacks& _callbacks;
    const ProcedureBlueprintMap* _procedures {nullptr};
    CommitHash _commitHash;
    ChangeID _changeID;
};

}
