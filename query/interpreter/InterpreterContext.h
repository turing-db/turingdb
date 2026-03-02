#pragma once

#include "QueryCallbacks.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace db {
class LocalMemory;
}

namespace vec {
class VectorDatabase;
}

namespace db {

class ExtensionManager;
class ProcedureManager;

class InterpreterContext {
public:
    InterpreterContext(LocalMemory* mem,
                       const QueryCallbacks* callbacks,
                       const ProcedureManager* procedures,
                       ExtensionManager* extensions,
                       vec::VectorDatabase* vectorDatabase,
                       CommitHash commitHash = CommitHash::head(),
                       ChangeID changeID = ChangeID::head())
        : _mem(mem),
        _callbacks(callbacks),
        _procedures(procedures),
        _extensions(extensions),
        _vectorDatabase(vectorDatabase),
        _commitHash(commitHash),
        _changeID(changeID)
    {
    }

    ~InterpreterContext() = default;

    LocalMemory* getLocalMemory() const { return _mem; }
    const ProcedureManager* getProcedures() const { return _procedures; }
    ExtensionManager* getExtensions() const { return _extensions; }
    const QueryCallbacks* getQueryCallbacks() const { return _callbacks; }
    vec::VectorDatabase* getVectorDatabase() const { return _vectorDatabase; }
    CommitHash getCommitHash() const { return _commitHash; }
    ChangeID getChangeID() const { return _changeID; }

private:
    LocalMemory* _mem {nullptr};
    const QueryCallbacks* _callbacks {nullptr};
    const ProcedureManager* _procedures {nullptr};
    ExtensionManager* _extensions {nullptr};
    vec::VectorDatabase* _vectorDatabase {nullptr};
    CommitHash _commitHash;
    ChangeID _changeID;
};

}
