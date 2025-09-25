#include "TuringDB.h"

#include "SystemManager.h"
#include "QueryInterpreter.h"
#include "JobSystem.h"

using namespace db;

TuringDB::TuringDB(TuringConfig& config)
    : _systemManager(std::make_unique<SystemManager>(config)),
    _jobSystem(JobSystem::create())
{
}

TuringDB::~TuringDB()
{
}

QueryStatus TuringDB::query(std::string_view query,
                            std::string_view graphName,
                            LocalMemory* mem,
                            QueryCallback callback,
                            CommitHash commit,
                            ChangeID change) {
    QueryInterpreter interp(_systemManager.get(), _jobSystem.get());
    return interp.execute(query, graphName, mem, callback, [](const auto) {}, commit, change);
}

QueryStatus TuringDB::query(std::string_view query,
                            std::string_view graphName,
                            LocalMemory* mem,
                            QueryCallback callback,
                            QueryHeaderCallback headerCallback,
                            CommitHash commit,
                            ChangeID change) {
    QueryInterpreter interp(_systemManager.get(), _jobSystem.get());
    return interp.execute(query, graphName, mem, callback, headerCallback, commit, change);
}

QueryStatus TuringDB::query(std::string_view query,
                            std::string_view graphName,
                            LocalMemory* mem,
                            CommitHash commit,
                            ChangeID change) {
    QueryInterpreter interp(_systemManager.get(), _jobSystem.get());
    return interp.execute(query, graphName, mem, [](const auto&) {}, [](const auto) {}, commit, change);
}
