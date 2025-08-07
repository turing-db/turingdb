#pragma once

#include <string_view>

#include "QueryStatus.h"
#include "QueryCallback.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace db {

class SystemManager;
class LocalMemory;
class JobSystem;
class Block;

class TuringDB {
public:
    TuringDB();
    ~TuringDB();

    SystemManager& getSystemManager() {
        return *_systemManager;
    }

    JobSystem& getJobSystem() {
        return *_jobSystem;
    }

    QueryStatus query(std::string_view query,
                      std::string_view graphName,
                      LocalMemory* mem,
                      QueryCallback callback,
                      CommitHash hash = CommitHash::head(),
                      ChangeID change = ChangeID::head());

    QueryStatus query(std::string_view query,
                      std::string_view graphName,
                      LocalMemory* mem,
                      QueryCallback callback,
                      QueryHeaderCallback headerCallback,
                      CommitHash hash = CommitHash::head(),
                      ChangeID change = ChangeID::head());


    QueryStatus query(std::string_view query,
                      std::string_view graphName,
                      LocalMemory* mem,
                      CommitHash hash = CommitHash::head(),
                      ChangeID change = ChangeID::head());

private:
    std::unique_ptr<SystemManager> _systemManager;
    std::unique_ptr<JobSystem> _jobSystem;
};

}
