#pragma once

#include <string_view>

namespace fs {
class Path;
}

namespace db {

class CommitBuilder;
class SystemAccessor;
class SystemManager;
class Transaction;

// The server-level facilities the system commands act through: the accessor they
// call, the transaction whose change they commit or submit, the pending commit
// they stage an index on, and the graph the session selected.
//
// An ordinary query needs none of it - it reads the graph through the view and
// writes through the commit write buffer - so a program with no system command
// runs against an empty context. A command handed a context missing what it
// needs reports that as a user-facing error, since it means the session opened no
// change or selected no graph.
class NLSystemContext {
public:
    NLSystemContext();
    ~NLSystemContext();

    SystemManager* getSystemManager() const { return _systemManager; }
    SystemAccessor* getAccessor() const { return _accessor; }
    Transaction* getTransaction() const { return _transaction; }
    CommitBuilder* getCommitBuilder() const { return _commitBuilder; }
    std::string_view getGraphName() const { return _graphName; }

    // Resolve a path the query gave relative to @param dataDir, the one directory a
    // command or a load may read or write in. Throws when the path reaches out of it.
    // Static, since the caller has already found the directory - a command through the
    // system manager it requires, a dataflow loop through the execution context.
    static void resolveInDataDir(fs::Path& resolved,
                                 const fs::Path& dataDir,
                                 std::string_view path);

    void setSystemManager(SystemManager* systemManager) { _systemManager = systemManager; }
    void setAccessor(SystemAccessor* accessor) { _accessor = accessor; }
    void setTransaction(Transaction* transaction) { _transaction = transaction; }
    void setCommitBuilder(CommitBuilder* commitBuilder) { _commitBuilder = commitBuilder; }
    void setGraphName(std::string_view graphName) { _graphName = graphName; }

private:
    SystemManager* _systemManager {nullptr};
    SystemAccessor* _accessor {nullptr};
    Transaction* _transaction {nullptr};
    CommitBuilder* _commitBuilder {nullptr};
    std::string_view _graphName;
};

}
