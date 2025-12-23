#pragma once

#include <string_view>

#include "QueryStatus.h"
#include "QueryCallback.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace db {

class TuringConfig;
class SystemManager;
class LocalMemory;
class JobSystem;
class Block;

namespace v2 {
class ProcedureBlueprintMap;
}

class TuringDB {
public:
    TuringDB(const TuringConfig* config);
    ~TuringDB();

    void init();

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

    QueryStatus queryV2(std::string_view query,
                        std::string_view graphName,
                        LocalMemory* mem,
                        QueryCallbackV2 callback,
                        CommitHash hash = CommitHash::head(),
                        ChangeID change = ChangeID::head());

    SystemManager& getSystemManager() {
        return *_systemManager;
    }

    JobSystem& getJobSystem() {
        return *_jobSystem;
    }

    const v2::ProcedureBlueprintMap& getProcedures() const {
        return *_procedures;
    }

private:
    const TuringConfig* _config;
    std::unique_ptr<SystemManager> _systemManager;
    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<v2::ProcedureBlueprintMap> _procedures;
};

}
