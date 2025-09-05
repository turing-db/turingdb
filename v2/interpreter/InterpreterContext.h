#pragma once

#include "QueryCallback.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace db {

class LocalMemory;

}

namespace db::v2 {

class InterpreterContext {
public:
    InterpreterContext(db::LocalMemory* mem,
                       db::QueryCallback callback,
                       db::QueryHeaderCallback headerCallback,
                       db::CommitHash commitHash = CommitHash::head(),
                       db::ChangeID changeID = ChangeID::head())
        : _mem(mem),
        _callback(callback),
        _headerCallback(headerCallback),
        _commitHash(commitHash),
        _changeID(changeID)
    {
    }

    ~InterpreterContext() = default;

    db::LocalMemory* getLocalMemory() const { return _mem; }
    db::QueryCallback getQueryCallback() const { return _callback; }
    db::QueryHeaderCallback getQueryHeaderCallback() const { return _headerCallback; }
    db::CommitHash getCommitHash() const { return _commitHash; }
    db::ChangeID getChangeID() const { return _changeID; }

private:
    db::LocalMemory* _mem {nullptr};
    db::QueryCallback _callback;
    db::QueryHeaderCallback _headerCallback;
    db::CommitHash _commitHash;
    db::ChangeID _changeID;
};

}
