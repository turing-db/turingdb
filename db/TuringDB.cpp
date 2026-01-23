#include "TuringDB.h"

#include <spdlog/spdlog.h>

#include "VectorDatabase.h"
#include "SystemManager.h"
#include "JobSystem.h"
#include "QueryInterpreterV2.h"
#include "InterpreterContext.h"
#include "procedures/ProcedureBlueprintMap.h"

#include "Panic.h"
#include "TuringConfig.h"

using namespace db;

TuringDB::TuringDB(const TuringConfig* config)
    : _config(config),
    _systemManager(std::make_unique<SystemManager>(config)),
    _jobSystem(JobSystem::create()),
    _procedures(ProcedureBlueprintMap::create())
{
}

TuringDB::~TuringDB() {
}

void TuringDB::init() {
    // Create turing directory if it does not exist
    const auto& turingDir = _config->getTuringDir();
    const auto& graphsDir = _config->getGraphsDir();
    const auto& dataDir = _config->getDataDir();
    const auto& vectorDir = _config->getVectorDir();

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

    if (vectorDir.empty()) {
        panic("Vector directory is not set");
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

    if (!vectorDir.exists()) {
        spdlog::info("Creating vector directory {}", vectorDir.c_str());

        if (auto res = vectorDir.mkdir(); !res) {
            panic("Could not create vector directory '{}': {}",
                  vectorDir.get(), res.error().fmtMessage());
        }
    }

    if (auto res = vec::VectorDatabase::create(vectorDir)) {
        _vectorDatabase = std::move(res.value());
    } else {
        panic("Could not create vector database: {}", res.error().fmtMessage());
    }

    _systemManager->init();
}

QueryStatus TuringDB::query(std::string_view query,
                            std::string_view graphName,
                            LocalMemory* mem,
                            QueryCallbackV2 callback,
                            CommitHash commit,
                            ChangeID change) {
    QueryInterpreterV2 interp(_systemManager.get(), _jobSystem.get());

    InterpreterContext ctxt(mem, callback, _procedures.get(), commit, change);
    return interp.execute(ctxt, query, graphName);
}

QueryStatus TuringDB::query(std::string_view query,
                            std::string_view graphName,
                            LocalMemory* mem,
                            CommitHash commit,
                            ChangeID change) {
    QueryInterpreterV2 interp(_systemManager.get(), _jobSystem.get());

    const QueryCallbackV2 callback = [](const Dataframe*){};

    InterpreterContext ctxt(mem, callback, _procedures.get(), commit, change);
    return interp.execute(ctxt, query, graphName);
}
