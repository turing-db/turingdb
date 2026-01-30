#pragma once

#include "QueryCallbacks.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace db {
class LocalMemory;
}

namespace db {

class ProcedureManager;

class InterpreterContext {
public:
    InterpreterContext(LocalMemory* mem,
                       const QueryCallbacks* callbacks,
                       const ProcedureManager* procedures,
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
    const ProcedureManager* getProcedures() const { return _procedures; }
    const QueryCallbacks* getQueryCallbacks() const { return _callbacks; }
    CommitHash getCommitHash() const { return _commitHash; }
    ChangeID getChangeID() const { return _changeID; }

private:
    LocalMemory* _mem {nullptr};
    const QueryCallbacks* _callbacks {nullptr};
    const ProcedureManager* _procedures {nullptr};
    CommitHash _commitHash;
    ChangeID _changeID;
};

}
