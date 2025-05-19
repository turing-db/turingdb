#pragma once

#include <string_view>
#include <memory>
#include <variant>

#include "QueryStatus.h"
#include "QueryCallback.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace db {

class SystemManager;
class JobSystem;
class LocalMemory;
class Executor;
class Transaction;
class WriteTransaction;

class QueryInterpreter {
public:
    QueryInterpreter(SystemManager* sysMan, JobSystem* jobSystem);
    ~QueryInterpreter();

    QueryStatus execute(std::string_view query,
                        std::string_view graphName,
                        LocalMemory* mem,
                        QueryCallback callback,
                        CommitHash commitHash = CommitHash::head(),
                        ChangeID changeID = ChangeID::head());

private:
    SystemManager* _sysMan {nullptr};
    JobSystem* _jobSystem {nullptr};
    std::unique_ptr<Executor> _executor;

    BasicResult<std::variant<Transaction, WriteTransaction>, QueryStatus> openTransaction(std::string_view graphName,
                                                                                          CommitHash commitHash,
                                                                                          ChangeID changeID);
};

}
