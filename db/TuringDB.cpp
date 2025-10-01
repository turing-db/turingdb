#include "TuringDB.h"

#include <spdlog/spdlog.h>

#include "SystemManager.h"
#include "QueryInterpreter.h"
#include "JobSystem.h"

#include "Panic.h"
#include "TuringConfig.h"

using namespace db;

TuringDB::TuringDB(const TuringConfig* config)
    : _config(config),
    _systemManager(std::make_unique<SystemManager>(config)),
    _jobSystem(JobSystem::create())
{
}

TuringDB::~TuringDB() {
}

void TuringDB::run() {
    // Create turing directory if it does not exist
    const auto& turingDir = _config->getTuringDir();
    const auto& graphsDir = _config->getGraphsDir();
    const auto& dataDir = _config->getDataDir();

    spdlog::info("Starting TuringDB. Root directory: '{}'", turingDir.get());

    if (turingDir.empty()) {
        panic("Turing directory is not set");
    }

    if (graphsDir.empty()) {
        panic("Graphs directory is not set");
    }

    if (dataDir.empty()) {
        panic("Data directory is not set");
    }

    if (!turingDir.exists()) {
        spdlog::info("Creating turing directory {}", turingDir.get());

        if (auto res = turingDir.mkdir(); !res) {
            panic("Could not create turing directory '{}': {}",
                  turingDir.get(), res.error().fmtMessage());
        }
    }

    if (!graphsDir.exists()) {
        spdlog::info("Creating graphs directory {}", graphsDir.get());

        if (auto res = graphsDir.mkdir(); !res) {
            panic("Could not create graphs directory '{}': {}",
                  graphsDir.get(), res.error().fmtMessage());
        }
    }

    if (!dataDir.exists()) {
        spdlog::info("Creating data directory {}", dataDir.c_str());

        if (auto res = dataDir.mkdir(); !res) {
            panic("Could not create data directory '{}': {}",
                  dataDir.get(), res.error().fmtMessage());
        }
    }

    _systemManager->init();
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
