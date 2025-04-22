#pragma once

#include "versioning/CommitHash.h"

namespace db {

class GraphView;
class SystemManager;

class ExecutionContext {
public:
    explicit ExecutionContext(SystemManager* sysMan,
                              const GraphView& dbView,
                              std::string_view graphName = {},
                              CommitHash commitHash = CommitHash::head())
        : _sysMan(sysMan),
        _dbView(dbView),
        _graphName(graphName),
        _commitHash(commitHash)
    {
    }

    SystemManager* getSystemManager() const { return _sysMan; }
    const GraphView& getGraphView() const { return _dbView; }
    std::string_view getGraphName() const { return _graphName; }
    CommitHash getCommitHash() const { return _commitHash; }

private:
    SystemManager* _sysMan {nullptr};
    const GraphView& _dbView;
    std::string_view _graphName;
    CommitHash _commitHash;
};

}
