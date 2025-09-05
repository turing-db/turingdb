#pragma once

#include "QueryCallback.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace db {

class LocalMemory;

class InterpreterContext {
public:
    InterpreterContext(LocalMemory* mem,
                       QueryCallback callback,
                       QueryHeaderCallback headerCallback,
                       CommitHash commitHash = CommitHash::head(),
                       ChangeID changeID = ChangeID::head())
        : _mem(mem),
        _callback(callback),
        _headerCallback(headerCallback),
        _commitHash(commitHash),
        _changeID(changeID)
    {
    }

    ~InterpreterContext() = default;

    LocalMemory* getLocalMemory() const { return _mem; }
    QueryCallback getQueryCallback() const { return _callback; }
    QueryHeaderCallback getQueryHeaderCallback() const { return _headerCallback; }
    CommitHash getCommitHash() const { return _commitHash; }
    ChangeID getChangeID() const { return _changeID; }

private:
    LocalMemory* _mem {nullptr};
    QueryCallback _callback;
    QueryHeaderCallback _headerCallback;
    CommitHash _commitHash;
    ChangeID _changeID;
};

}
