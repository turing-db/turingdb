#pragma once

#include "QueryConfig.h"
#include "QueryCallbacks.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace vec {
class VectorDatabase;
}

namespace db {

class ExtensionManager;
class ProcedureManager;
class LocalMemory;

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
    const QueryConfig* getQueryConfig() const { return _queryConfig; }
    CommitHash getCommitHash() const { return _commitHash; }
    ChangeID getChangeID() const { return _changeID; }

    void setQueryConfig(const QueryConfig* config) { _queryConfig = config; }

private:
    LocalMemory* _mem {nullptr};
    const QueryCallbacks* _callbacks {nullptr};
    const ProcedureManager* _procedures {nullptr};
    ExtensionManager* _extensions {nullptr};
    vec::VectorDatabase* _vectorDatabase {nullptr};
    const QueryConfig* _queryConfig {nullptr};
    CommitHash _commitHash;
    ChangeID _changeID;
};

}
