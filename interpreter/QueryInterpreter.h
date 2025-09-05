#pragma once

#include <string_view>

#include "QueryStatus.h"
#include "QueryCallback.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace db {

class SystemManager;
class JobSystem;
class LocalMemory;
class Executor;

class QueryInterpreter {
public:
    QueryInterpreter(SystemManager* sysMan, JobSystem* jobSystem);
    ~QueryInterpreter();

    QueryStatus execute(std::string_view query,
                        std::string_view graphName,
                        LocalMemory* mem,
                        QueryCallback callback,
                        QueryHeaderCallback headerCallback,
                        CommitHash commitHash = CommitHash::head(),
                        ChangeID changeID = ChangeID::head());

private:
    SystemManager* _sysMan {nullptr};
    JobSystem* _jobSystem {nullptr};

    QueryStatus executeV1(std::string_view query,
                          std::string_view graphName,
                          LocalMemory* mem,
                          QueryCallback callback,
                          QueryHeaderCallback headerCallback,
                          CommitHash commitHash,
                          ChangeID changeID);

    QueryStatus executeV2(std::string_view query,
                          std::string_view graphName,
                          LocalMemory* mem,
                          QueryCallback callback,
                          QueryHeaderCallback headerCallback,
                          CommitHash commitHash,
                          ChangeID changeID);
};

}
