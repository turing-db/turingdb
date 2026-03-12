#pragma once

#include <string_view>

#include "LockFile.h"
#include "QueryConfig.h"
#include "QueryStatus.h"
#include "QueryCallbacks.h"
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
class ExtensionManager;
class ProcedureManager;
class SystemEventHandler;

class TuringDB {
public:
    TuringDB(const TuringConfig* config);
    ~TuringDB();

    void init();

    QueryStatus query(std::string_view query,
                      std::string_view graphName,
                      LocalMemory* mem,
                      const QueryConfig* queryConfig,
                      const QueryCallbacks& callbacks,
                      CommitHash hash = CommitHash::head(),
                      ChangeID change = ChangeID::head());

    QueryStatus query(std::string_view query,
                      std::string_view graphName,
                      LocalMemory* mem,
                      const QueryConfig* queryConfig,
                      const QueryCallbacks::OnOutputData& callback,
                      CommitHash hash = CommitHash::head(),
                      ChangeID change = ChangeID::head());

    QueryStatus query(std::string_view query,
                      std::string_view graphName,
                      LocalMemory* mem,
                      const QueryConfig* queryConfig,
                      CommitHash hash = CommitHash::head(),
                      ChangeID change = ChangeID::head());

    const QueryConfig& getDefaultQueryConfig() const { return _defaultQueryConfig; }

    SystemManager& getSystemManager() { return *_systemManager; }
    JobSystem& getJobSystem() { return *_jobSystem; }
    const ProcedureManager* getProcedures() const { return _procedures.get(); }
    ExtensionManager* getExtensions() { return _extensions.get(); }

    void stop();

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
