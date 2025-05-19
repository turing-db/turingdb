#pragma once

#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

namespace db {

class GraphView;
class SystemManager;
class DataPartBuilder;
class JobSystem;
class WriteTransaction;

class ExecutionContext {
public:
    explicit ExecutionContext(SystemManager* sysMan,
                              JobSystem* jobSystem,
                              const GraphView& dbView,
                              std::string_view graphName = {},
                              CommitHash commitHash = CommitHash::head(),
                              ChangeID changeID = ChangeID::head(),
                              WriteTransaction* tx = nullptr)
        : _sysMan(sysMan),
        _jobSystem(jobSystem),
        _dbView(dbView),
        _graphName(graphName),
        _commitHash(commitHash),
        _changeID(changeID),
        _writeTx(tx)
    {
    }

    SystemManager* getSystemManager() const { return _sysMan; }
    JobSystem* getJobSystem() const { return _jobSystem; }
    const GraphView& getGraphView() const { return _dbView; }
    std::string_view getGraphName() const { return _graphName; }
    CommitHash getCommitHash() const { return _commitHash; }
    ChangeID getChangeID() const { return _changeID; }
    WriteTransaction* getWriteTransaction() { return _writeTx; }

private:
    SystemManager* _sysMan {nullptr};
    JobSystem* _jobSystem {nullptr};
    const GraphView& _dbView;
    std::string_view _graphName;
    CommitHash _commitHash;
    ChangeID _changeID;
    WriteTransaction* _writeTx {nullptr};
};

}
