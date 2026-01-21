#pragma once

#include <string_view>

#include "QueryStatus.h"
#include "QueryCallback.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace vec {
class VectorDatabase;
}

namespace db {

class TuringConfig;
class SystemManager;
class LocalMemory;
class JobSystem;
class Block;
class ProcedureBlueprintMap;

class TuringDB {
public:
    TuringDB(const TuringConfig* config);
    ~TuringDB();

    void init();

    QueryStatus query(std::string_view query,
                      std::string_view graphName,
                      LocalMemory* mem,
                      QueryCallbackV2 callback,
                      CommitHash hash = CommitHash::head(),
                      ChangeID change = ChangeID::head());

    QueryStatus query(std::string_view query,
                      std::string_view graphName,
                      LocalMemory* mem,
                      CommitHash hash = CommitHash::head(),
                      ChangeID change = ChangeID::head());

    SystemManager& getSystemManager() {
        return *_systemManager;
    }

    JobSystem& getJobSystem() {
        return *_jobSystem;
    }

    const ProcedureBlueprintMap& getProcedures() const {
        return *_procedures;
    }

private:
    const TuringConfig* _config;
    std::unique_ptr<SystemManager> _systemManager;
    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<ProcedureBlueprintMap> _procedures;
    std::unique_ptr<vec::VectorDatabase> _vectorDatabase;
};

}
