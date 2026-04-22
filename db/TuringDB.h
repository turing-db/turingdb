#pragma once

#include <string_view>

#include "LockFile.h"
#include "QueryConfig.h"
#include "QueryState.h"
#include "QueryStatus.h"

namespace vec {
class VectorDatabase;
}

namespace db {

class TuringConfig;
class SystemManager;
class JobSystem;
class ExtensionManager;
class ProcedureManager;

class TuringDB {
public:
    TuringDB(const TuringConfig* config);
    ~TuringDB();

    void init();

    void stop();

    QueryStatus query(std::string_view query, const QueryState& state);

    const QueryConfig& getDefaultQueryConfig() const { return _defaultQueryConfig; }

    SystemManager& getSystemManager() { return *_systemManager; }

    JobSystem& getJobSystem() { return *_jobSystem; }

    const ProcedureManager* getProcedures() const { return _procedures.get(); }

    ExtensionManager* getExtensions() { return _extensions.get(); }

private:
    const TuringConfig* _config {nullptr};
    QueryConfig _defaultQueryConfig;
    LockFile _lockFile;

    std::unique_ptr<SystemManager> _systemManager;
    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<ProcedureManager> _procedures;
    std::unique_ptr<ExtensionManager> _extensions;
    std::unique_ptr<vec::VectorDatabase> _vectorDatabase;
};

}
