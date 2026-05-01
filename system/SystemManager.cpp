#include "SystemManager.h"

#include <spdlog/spdlog.h>

#include "Graph.h"

#include "TuringConfig.h"

#include "JobSystem.h"

#include "versioning/Commit.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "versioning/VersionController.h"

#include "GraphSerializer.h"
#include "dump/GraphLoader.h"

#include "GMLImporter.h"
#include "JsonlParser.h"

#include "SystemEventHandler.h"

#include "FileUtils.h"
#include "Panic.h"
#include "TuringException.h"

using namespace db;

SystemManager::SystemManager(const TuringConfig* config)
    : _config(config)
{
}

SystemManager::~SystemManager() {
    SystemEventHandler::terminate();
}

void SystemManager::init() {
    initTuringDirectory();
    initLockFile();
    initVectorDatabase();
    initSystemEvents();
    _jobSystem.init();
    loadOrCreateDefaultGraph();

    _procedures.init();
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

void SystemManager::loadOrCreateDefaultGraph() {
    // Try to load default graph from disk
    _defaultGraph = loadGraph("default");
    if (_defaultGraph) {
        return;
    }

    // Create default graph if it does not exist
    _defaultGraph = createGraph("default");
    if (!_defaultGraph) {
        throw TuringException("Could not initialise the default graph");
    }
}

Graph* SystemManager::loadGraph(const std::string& name) {
    const fs::Path graphPath = _config->getGraphsDir() / name;

    auto graph = Graph::create(name, graphPath);
    Graph* graphPtr = graph.get();

    if (const auto res = graph->getSerializer().load(); !res) {
        return nullptr;
    }

    if (!addGraph(std::move(graph))) {
        return nullptr;
    }

    return graphPtr;
}

Graph* SystemManager::createGraph(const std::string& name) {
    const fs::Path path = _config->getGraphsDir() / name;

    const bool syncedOnDisk = _config->isSyncedOnDisk();
    if (syncedOnDisk) {
        if (path.exists()) {
            throw TuringException(fmt::format("Graph '{}' already exists", name));
        }
    }

    auto graph = Graph::create(name, path);
    Graph* graphPtr = graph.get();

    if (_config->isSyncedOnDisk()) {
        if (auto res = graph->getSerializer().dump(); !res) {
            spdlog::error(res.error().fmtMessage());
            return nullptr;
        }
    }

    if (!addGraph(std::move(graph))) {
        return nullptr;
    }

    return graphPtr;
}

bool SystemManager::addGraph(std::unique_ptr<Graph> graph) {
    std::unique_lock guard(_graphsLock);

    const auto& name = graph->getName();

    // Search if a graph with the same name exists
    const auto it = _graphs.find(name);
    if (it != _graphs.end()) {
        return false;
    }

    _graphs[name] = std::move(graph);

    return true;
}

Graph* SystemManager::getDefaultGraph() const {
    std::shared_lock guard(_graphsLock);
    return _defaultGraph;
}

void SystemManager::setDefaultGraph(const std::string& name) {
    std::unique_lock guard(_graphsLock);

    const auto it = _graphs.find(name);
    if (it != _graphs.end()) {
        _defaultGraph = it->second.get();
    }
}

Graph* SystemManager::getGraph(const std::string& graphName) const {
    std::shared_lock guard(_graphsLock);

    const auto it = _graphs.find(graphName);
    if (it == _graphs.end()) {
        return nullptr;
    }

    return it->second.get();
}

void SystemManager::listGraphs(std::vector<std::string_view>& names) {
    std::shared_lock guard(_graphsLock);

    for (const auto& [name, graph] : _graphs) {
        names.push_back(name);
    }
}

bool SystemManager::importGraph(const std::string& graphName, const fs::Path& filePath, JobSystem& jobSystem) {
    const fs::Path graphPath = _config->getGraphsDir() / filePath;

    // Step 1. Check if graph was already loaded || is already loading
    if (getGraph(graphName) || isGraphLoading(graphName)) {
        return false;
    }

    // Step 2. Validate the path. It should be within the data directory
    const fs::Path absolute = _config->getDataDir() / filePath;
    if (!absolute.isSubDirectory(_config->getDataDir())) {
        spdlog::error("File is not within the data directory: {}", absolute.get());
        return false;
    }

    if (!absolute.exists()) {
        spdlog::error("File does not exist: {}", absolute.get());
        return false;
    }

    // Step 3. Determine the file type
    const auto fileType = getGraphFileType(absolute);

    // Step 4. Load the graph
    if (!fileType) {
        // If we can not determine the file type, assume it is a JSONL graph
        // to be changed in the future
        return loadJsonlDB(graphName, absolute, jobSystem);
    }

    switch (*fileType) {
        case GraphFileType::GML:
            return loadGmlDB(graphName, absolute, jobSystem);
        case GraphFileType::JSONL:
            return loadJsonlDB(graphName, absolute, jobSystem);
        case GraphFileType::BINARY:
            return loadBinaryDB(graphName, absolute, jobSystem);
        case GraphFileType::_SIZE:
            throw TuringException("Unsupported graph type");
    }

    throw TuringException("Unsupported graph type");
}

DumpResult<void> SystemManager::dumpGraph(const std::string& graphName) {
    std::shared_lock guard(_graphsLock);

    if (!_config->isSyncedOnDisk()) {
        spdlog::warn("Cannot dump graph, The system is running in full in-memory mode");
        return {};
    }

    const auto graphIt = _graphs.find(graphName);
    if (graphIt == _graphs.end()) {
        return DumpError::result(DumpErrorType::GRAPH_DOES_NOT_EXIST);
    }

    return graphIt->second->getSerializer().dump();
}

std::optional<GraphFileType> SystemManager::getGraphFileType(const fs::Path& graphPath) {
    if (graphPath.extension() == ".gml") {
        return GraphFileType::GML;
    }

    if (graphPath.extension() == ".json" || graphPath.extension() == ".jsonl") {
        return GraphFileType::JSONL;
    }

    const auto typeFilePath = graphPath / "type";
    std::string typeName;
    if (!FileUtils::readContent(typeFilePath.get(), typeName)) {
        return {};
    }

    if (typeName == GraphFileTypeDescription::value(GraphFileType::BINARY)) {
        return GraphFileType::BINARY;
    } else if (typeName == GraphFileTypeDescription::value(GraphFileType::JSONL)) {
        return GraphFileType::JSONL;
    }

    return {};
}

bool SystemManager::loadBinaryDB(const std::string& graphName,
                                 const fs::Path& dbPath,
                                 JobSystem& jobsystem) {
    if (!_graphLoadStatus.addLoadingGraph(graphName)) {
        return false;
    }

    // in the case of turingDB binaries the path is the same path we load from.
    auto graph = Graph::create(graphName, dbPath);

    if (auto res = graph->getSerializer().load(); !res) {
        spdlog::error("Could not load graph {}: {}", graphName, res.error().fmtMessage());
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    if (!addGraph(std::move(graph))) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    _graphLoadStatus.removeLoadingGraph(graphName);

    return true;
}

bool SystemManager::isGraphLoading(const std::string& graphName) const {
    return _graphLoadStatus.isGraphLoading(graphName);
}

bool SystemManager::loadJsonlDB(const std::string& graphName,
                                const fs::Path& dbPath,
                                JobSystem& jobsystem) {
    if (!_graphLoadStatus.addLoadingGraph(graphName)) {
        return false;
    }

    const auto& graphPath = _config->getGraphsDir() / graphName;
    if (graphPath == dbPath) {
        return false;
    }

    auto graph = Graph::create(graphName, graphPath);
    std::ifstream file(dbPath.get());
    if (!file) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    Change* change = _changes.createChange(graph.get(), CommitHash::head());
    ChangeAccessor changeAccessor = change->access();

    const auto importRes = JsonlParser::parse(changeAccessor, file);

    if (!importRes) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        spdlog::error(importRes.error().fmtMessage());
        return false;
    }

    const auto submitRes = _changes.submitChange(changeAccessor, jobsystem);

    if (!submitRes) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        spdlog::error(submitRes.error().fmtMessage());
        return false;
    }

    if (_config->isSyncedOnDisk()) {
        const auto dumpRes = graph->getSerializer().dump();

        if (!dumpRes) {
            _graphLoadStatus.removeLoadingGraph(graphName);
            spdlog::error(dumpRes.error().fmtMessage());
            return false;
        }
    }

    if (!addGraph(std::move(graph))) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    _graphLoadStatus.removeLoadingGraph(graphName);
    return true;
}

bool SystemManager::loadGmlDB(const std::string& graphName,
                              const fs::Path& dbPath,
                              JobSystem& jobsystem) {
    if (!_graphLoadStatus.addLoadingGraph(graphName)) {
        return false;
    }

    const auto& graphPath = _config->getGraphsDir() / graphName;
    if (graphPath == dbPath) {
        return false;
    }

    // Load graph
    auto graph = Graph::create(graphName, graphPath);

    // load GMLs
    GMLImporter importer;

    if (!importer.importFile(jobsystem, graph.get(), FileUtils::Path {dbPath.c_str()})) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    if (_config->isSyncedOnDisk()) {
        if (!graph->getSerializer().dump()) {
            _graphLoadStatus.removeLoadingGraph(graphName);
            return false;
        }
    }

    if (!addGraph(std::move(graph))) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    _graphLoadStatus.removeLoadingGraph(graphName);
    return true;
}

void SystemManager::listAvailableGraphs(std::vector<fs::Path>& names) {
    const auto list = _config->getGraphsDir().listDir();
    if (!list) {
        throw TuringException("Can not list graphs in turing directory");
    }

    for (const auto& path : list.value()) {
        names.push_back(path);
    }
}

ChangeResult<Change*> SystemManager::newChange(const std::string& graphName, CommitHash baseHash) {
    std::shared_lock graphGuard(_graphsLock);

    const auto it = _graphs.find(graphName);
    if (it == _graphs.end()) {
        return ChangeError::result(ChangeErrorType::GRAPH_NOT_FOUND);
    }

    Graph* graph = it->second.get();
    return _changes.createChange(graph, baseHash);
}

DataPartMergeResult<void> SystemManager::mergeDataParts(Graph* graph, JobSystem& jobSystem) {
    return _changes.mergeDataParts(graph, jobSystem);
}

ChangeResult<Transaction> SystemManager::openTransaction(std::string_view graphName,
                                                         CommitHash commitHash,
                                                         ChangeID changeID) {
    std::shared_lock guard(_graphsLock);

    Graph* graph = graphName.empty() ? this->getDefaultGraph()
                                     : this->getGraph(std::string(graphName));
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

    const auto changeRes = this->getChangeManager().getChange(graph, changeID);
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
    std::shared_lock guard(_graphsLock);
    Graph* graph = graphName.empty() ? getDefaultGraph()
                                     : getGraph(std::string(graphName));
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
