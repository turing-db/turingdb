#include "TuringDB.h"

#include <spdlog/spdlog.h>

#include "VectorDatabase.h"
#include "SystemManager.h"
#include "JobSystem.h"
#include "QueryInterpreterV2.h"
#include "InterpreterContext.h"
#include "procedures/ProcedureManager.h"
#include "ExtensionManager.h"
#include "SystemEventHandler.h"

#include "Panic.h"
#include "TuringConfig.h"

using namespace db;

TuringDB::TuringDB(const TuringConfig* config)
    : _config(config),
    _systemManager(std::make_unique<SystemManager>(config)),
    _jobSystem(JobSystem::create()),
    _procedures(ProcedureManager::create()),
    _extensions(std::make_unique<ExtensionManager>(config->getUserExtensionsDir(),
                                                   config->getInstallExtensionsDir(),
                                                   _procedures.get()))
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

    const auto& extensionsDir = _config->getUserExtensionsDir();

    if (!extensionsDir.exists()) {
        spdlog::info("Creating extensions directory {}", extensionsDir.get());

        if (auto res = extensionsDir.mkdir(); !res) {
            panic("Could not create extensions directory '{}': {}",
                  extensionsDir.get(), res.error().fmtMessage());
        }
    }

    // Create vector database
    if (auto res = vec::VectorDatabase::create(vectorDir)) {
        _vectorDatabase = std::move(res.value());
    } else {
        panic("Could not create vector database: {}", res.error().fmtMessage());
    }

    // Acquire lock file
    _lockFile.setPath(fs::Path(_config->getLockFilePath()));
    auto lockRes = _lockFile.tryLock();
    if (!lockRes) {
        panic("Could not acquire lock file: {}", lockRes.error().fmtMessage());
    }

    // Initialize socket/signal communication system
    if (!SystemEventHandler::initialize(_config->getSocketPath())) {
        panic("Could not initialize system event handler");
    }

    // Init system manager
    _systemManager->init();

    // Set on stop callback
    SystemEventHandler::setOnStop([this] {
        _config->getOnStopRequest()();
    });

    // Init procedures
    _procedures->init();

}

QueryStatus TuringDB::query(std::string_view query,
                            std::string_view graphName,
                            LocalMemory* mem,
                            const QueryCallbacks& callbacks,
                            CommitHash hash,
                            ChangeID change) {
    QueryInterpreterV2 interp(_systemManager.get(), _jobSystem.get());

    // TODO: This is still using NRVO
    //       We should pass the QueryStatus& as argument to the function
    //       however, the function is becoming quite complex. We should
    //       probably refactor it to take in a QueryContext& instead,
    //       that would contain everything needed to execute the query.
    QueryStatus status;
    const InterpreterContext ctxt(mem,
                                  &callbacks,
                                  _procedures.get(),
                                  _extensions.get(),
                                  _vectorDatabase.get(),
                                  hash,
                                  change);
    interp.execute(ctxt, status, query, graphName);

    return status;
}

QueryStatus TuringDB::query(std::string_view query,
                            std::string_view graphName,
                            LocalMemory* mem,
                            const QueryCallbacks::OnOutputData& callback,
                            CommitHash hash,
                            ChangeID change) {
    QueryCallbacks callbacks;
    callbacks.setOnOutputData(callback);

    return this->query(query, graphName, mem, callbacks, hash, change);
}

QueryStatus TuringDB::query(std::string_view query,
                            std::string_view graphName,
                            LocalMemory* mem,
                            CommitHash hash,
                            ChangeID change) {
    const QueryCallbacks handler;

    return this->query(query, graphName, mem, handler, hash, change);
}
