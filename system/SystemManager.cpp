#include "SystemManager.h"

#include <spdlog/spdlog.h>

#include "Graph.h"

#include "TuringConfig.h"

#include "JobSystem.h"

#include "versioning/Commit.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "versioning/VersionController.h"

#include "dump/GraphDumper.h"

#include "SystemEventHandler.h"

#include "Panic.h"
#include "TuringException.h"

using namespace db;

SystemManager::SystemManager(const TuringConfig* config)
    : _config(config),
    _graphManager(config)
{
}

SystemManager::~SystemManager() {
    SystemEventHandler::terminate();
}

SystemAccessor SystemManager::accessShared() {
    return SystemAccessor(this, Accessor::SharedAccess{});
}

SystemAccessor SystemManager::accessUnique() {
    return SystemAccessor(this, Accessor::UniqueAccess{});
}

void SystemManager::init() {
    initTuringDirectory();
    initLockFile();
    initVectorDatabase();
    initSystemEvents();
    _jobSystem.init();
    _graphManager.loadOrCreateDefaultGraph();

    _procedures.init();
    _extensions.init(&_procedures,
                     _config->getUserExtensionsDir(),
                     _config->getInstallExtensionsDir());
}

void SystemManager::initTuringDirectory() {
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
}

void SystemManager::initLockFile() {
    // Acquire lock file
    _lockFile.setPath(fs::Path(_config->getLockFilePath()));
    const auto lockRes = _lockFile.tryLock();
    if (!lockRes) {
        panic("Could not acquire lock file: {}", lockRes.error().fmtMessage());
    }
}

void SystemManager::initVectorDatabase() {
    const auto& vectorDir = _config->getVectorDir();

    if (auto res = _vectorDatabase.init(vectorDir); !res) {
        panic("Could not create vector database: {}", res.error().fmtMessage());
    }
}

void SystemManager::initSystemEvents() {
    // Initialize socket/signal communication system
    if (_config->usingSystemEvents()) {
        if (!SystemEventHandler::initialize(_config->getSocketPath())) {
            panic("Could not initialize system event handler");
        }
    }

    // Set on stop callback
    SystemEventHandler::setOnStop([this] {
        _config->getOnStopRequest()();
    });
}

Graph* SystemManager::loadGraph(std::string_view name) {
    return _graphManager.loadGraph(name);
}

Graph* SystemManager::createGraph(std::string_view name) {
    return _graphManager.createGraph(name);
}

Graph* SystemManager::getDefaultGraph() const {
    return _graphManager.getDefaultGraph();
}

void SystemManager::setDefaultGraph(std::string_view name) {
    _graphManager.setDefaultGraph(name);
}

Graph* SystemManager::getGraph(std::string_view graphName) const {
    return _graphManager.getGraph(graphName);
}

void SystemManager::listGraphs(std::vector<std::string_view>& names) const {
    _graphManager.listGraphs(names);
}

Graph* SystemManager::importGraph(const fs::Path& path, std::string_view graphName) {
    return _graphManager.importGraph(graphName, path, &_jobSystem);
}

DumpResult<void> SystemManager::dumpGraph(std::string_view graphName) {
    if (!_config->isSyncedOnDisk()) {
        spdlog::warn("Cannot dump graph, The system is running in full in-memory mode");
        return {};
    }

    Graph* graph = _graphManager.getGraph(graphName);
    if (!graph) {
        return DumpError::result(DumpErrorType::GRAPH_DOES_NOT_EXIST);
    }

    return GraphDumper::dump(graph, graph->getPath());
}

GraphFileType SystemManager::getGraphFileType(const fs::Path& graphPath) const {
    return _graphManager.getGraphFileType(graphPath);
}

bool SystemManager::isGraphLoading(std::string_view graphName) const {
    return _graphManager.isGraphLoading(graphName);
}

void SystemManager::listAvailableGraphs(std::vector<std::string>& names) const {
    const auto list = _config->getGraphsDir().listDir();
    if (!list) {
        throw TuringException("Can not list graphs in turing directory");
    }

    for (const auto& path : list.value()) {
        names.emplace_back(path.filename());
    }
}

ChangeResult<Change*> SystemManager::newChange(std::string_view graphName, CommitHash baseHash) {
    return _graphManager.newChange(graphName, baseHash);
}

ChangeResult<Change*> SystemManager::getChange(const Graph* graph, ChangeID changeID) {
    return _graphManager.getChange(graph, changeID);
}

ChangeResult<void> SystemManager::submitChange(ChangeAccessor& accessor, JobSystem& jobSystem) {
    return _graphManager.submitChange(accessor, jobSystem);
}

ChangeResult<void> SystemManager::deleteChange(ChangeAccessor& accessor, ChangeID changeID) {
    return _graphManager.deleteChange(accessor, changeID);
}

void SystemManager::listChanges(std::vector<const Change*>& changes, const Graph* graph) const {
    _graphManager.listChanges(changes, graph);
}

DataPartMergeResult<void> SystemManager::mergeDataParts(Graph* graph, JobSystem& jobSystem) {
    return _graphManager.mergeDataParts(graph, jobSystem);
}

ChangeResult<Transaction> SystemManager::openTransaction(std::string_view graphName,
                                                         CommitHash commitHash,
                                                         ChangeID changeID) {
    Graph* graph = graphName.empty() ? this->getDefaultGraph()
                                     : this->getGraph(graphName);
    if (!graph) {
        return ChangeError::result(ChangeErrorType::GRAPH_NOT_FOUND);
    }

    if (changeID == ChangeID::head()) {
        // Not in a change, reading frozen commit
        if (auto tx = graph->openTransaction(commitHash); tx.isValid()) {
            return tx;
        }

        // Determine why it failed
        const Commit* commit = graph->getVersionController().getCommitSafe(commitHash);
        if (commit != nullptr && !commit->hasData()) {
            return ChangeError::result(ChangeErrorType::COMMIT_NOT_LOADED);
        }
        return ChangeError::result(ChangeErrorType::COMMIT_NOT_FOUND);
    }

    const auto changeRes = _graphManager.getChange(graph, changeID);
    if (!changeRes) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    // In a valid change
    auto* change = changeRes.value();

    // If hash == head: Requesting a write on the tip of the change
    if (commitHash == CommitHash::head()) {
        if (auto tx = change->openWriteTransaction(); tx.isValid()) {
            return tx;
        }

        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    // if hash != head: Requesting a read on a specific commit (either pending or frozen)
    if (auto tx = change->openReadTransaction(commitHash); tx.isValid()) {
        // Reading pending commit
        return tx;
    } else if (auto tx = graph->openTransaction(commitHash); tx.isValid()) {
        // Reading frozen commit
        return tx;
    }

    // Determine why it failed
    {
        const Commit* commit = graph->getVersionController().getCommitSafe(commitHash);
        if (commit != nullptr && !commit->hasData()) {
            return ChangeError::result(ChangeErrorType::COMMIT_NOT_LOADED);
        }
    }
    return ChangeError::result(ChangeErrorType::COMMIT_NOT_FOUND);
}

DumpResult<void> SystemManager::loadCommit(std::string_view graphName, CommitHash hash) {
    Graph* graph = graphName.empty() ? getDefaultGraph()
                                     : getGraph(graphName);
    if (!graph) {
        return DumpError::result(DumpErrorType::GRAPH_DOES_NOT_EXIST);
    }
    return graph->loadCommit(hash);
}

void SystemManager::createS3Client(const std::string& accessId,
                                   const std::string& secretKey,
                                   const std::string& region) {
    auto wrapper = S3::MinioS3ClientWrapper(accessId, secretKey, region);
    _s3Client = std::make_unique<S3::TuringS3Client<S3::MinioS3ClientWrapper>>(std::move(wrapper));
}
