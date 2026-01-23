#include "SystemManager.h"

#include <shared_mutex>
#include <mutex>
#include <spdlog/spdlog.h>

#include "ChangeManager.h"
#include "Graph.h"
#include "Neo4j/Neo4JParserConfig.h"
#include "Neo4jImporter.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "GMLImporter.h"
#include "JobSystem.h"
#include "GraphSerializer.h"
#include "dump/GraphLoader.h"
#include "FileUtils.h"
#include "TuringConfig.h"
#include "TuringException.h"

using namespace db;

SystemManager::SystemManager(const TuringConfig* config)
    : _config(config),
    _changes(std::make_unique<ChangeManager>()),
    _neo4JImporter(std::make_unique<Neo4jImporter>())
{
}

SystemManager::~SystemManager() {
}

void SystemManager::init() {
    const auto list = _config->getGraphsDir().listDir();

    if (!list) {
        throw TuringException(fmt::format("Can not list graphs in turing directory '{}'",
                                          _config->getGraphsDir().get()));
    }

    const fs::Path defaultPath = _config->getGraphsDir() / "default"; auto found = std::find(list.value().begin(), list.value().end(), defaultPath);

    if (found != list->end()) {
        _defaultGraph = loadGraph("default");
    } else {
        _defaultGraph = createGraph("default");
    }

    if (!_defaultGraph) {
        throw TuringException("Could not initialise the default graph");
    }
}

Graph* SystemManager::loadGraph(const std::string& name) {
    const fs::Path graphPath = _config->getGraphsDir() / name;

    auto graph = Graph::create(name, graphPath);
    Graph* graphPtr = graph.get();

    if (const auto res = graph->getSerializer().load(); !res) {
        spdlog::error(res.error().fmtMessage());
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
        // If we can not determine the file type, assume it is a Neo4j JSON graph
        // to be changed in the future
        return loadNeo4jJsonDB(graphName, absolute, jobSystem);
    }

    switch (*fileType) {
        case GraphFileType::GML:
            return loadGmlDB(graphName, absolute, jobSystem);
        case GraphFileType::NEO4J:
            return loadNeo4jDB(graphName, absolute, jobSystem);
        case GraphFileType::NEO4J_JSON:
            return loadNeo4jJsonDB(graphName, absolute, jobSystem);
        case GraphFileType::BINARY:
            return loadBinaryDB(graphName, absolute, jobSystem);
        default:
            return false;
    }
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

    if (graphPath.extension() == ".dump") {
        return GraphFileType::NEO4J;
    }

    const auto typeFilePath = graphPath / "type";
    std::string typeName;
    if (!FileUtils::readContent(typeFilePath.get(), typeName)) {
        return {};
    }

    if (typeName == GraphFileTypeDescription::value(GraphFileType::NEO4J_JSON)) {
        return GraphFileType::NEO4J_JSON;
    } else if (typeName == GraphFileTypeDescription::value(GraphFileType::BINARY)) {
        return GraphFileType::BINARY;
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

bool SystemManager::loadNeo4jJsonDB(const std::string& graphName,
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

    Neo4jImporter::ImportJsonDirArgs args;
    args._jsonDir = FileUtils::Path {dbPath.c_str()};

    if (!_neo4JImporter->importJsonDir(jobsystem,
                                       graph.get(),
                                       db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                       db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                       args)) {
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

bool SystemManager::loadNeo4jDB(const std::string& graphName,
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

    Neo4jImporter::DumpFileToJsonDirArgs dumpArgs;
    dumpArgs._workDir = "/tmp";
    dumpArgs._dumpFilePath = dbPath.c_str();

    if (!_neo4JImporter->fromDumpFileToJsonDir(jobsystem,
                                               graph.get(),
                                               db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                               db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                               dumpArgs)) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    Neo4jImporter::ImportJsonDirArgs args;
    args._jsonDir = FileUtils::Path {dumpArgs._workDir / "json"};

    if (!_neo4JImporter->importJsonDir(jobsystem,
                                       graph.get(),
                                       db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                       db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                       args)) {
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
    return _changes->createChange(graph, baseHash);
}

DataPartMergeResult<void> SystemManager::mergeDataParts(Graph* graph, JobSystem& jobSystem) {
    return _changes->mergeDataParts(graph, jobSystem);
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

    return ChangeError::result(ChangeErrorType::COMMIT_NOT_FOUND);
}
