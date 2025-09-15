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
#include "GraphLoader.h"
#include "FileUtils.h"
#include "TuringConfig.h"
#include "TuringException.h"

using namespace db;

SystemManager::SystemManager(const TuringConfig& config)
    : _config(config),
    _changes(std::make_unique<ChangeManager>())
{
    _defaultGraph = createGraph("default");
}

SystemManager::~SystemManager() {
}

Graph* SystemManager::createGraph(const std::string& name) {
    std::unique_ptr<Graph> graph = Graph::create(name);
    auto* rawPtr = graph.get();

    if (!addGraph(std::move(graph), name)) {
        return nullptr;
    }

    return rawPtr;
}

bool SystemManager::addGraph(std::unique_ptr<Graph> graph, const std::string& name) {
    std::unique_lock guard(_graphsLock);

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

bool SystemManager::loadGraph(const std::string& graphName, const std::string& graphFileName, JobSystem& jobSystem) {
    const fs::Path graphPath = _config.getGraphsDir() / graphFileName;

    // Check if graph was already loaded || is already loading
    if (getGraph(graphName) || isGraphLoading(graphName)) {
        return false;
    }

    if (!graphPath.exists()) {
        return false;
    }

    const auto fileType = getGraphFileType(graphPath);
    if (!fileType) {
        // If we can not determine the file type, assume it is a Neo4j JSON graph
        // to be changed in the future
        return loadNeo4jJsonDB(graphName, graphPath, jobSystem);
    }

    switch (*fileType) {
        case GraphFileType::GML:
            return loadGmlDB(graphName, graphPath, jobSystem);
        case GraphFileType::NEO4J:
            return loadNeo4jDB(graphName, graphPath, jobSystem);
        case GraphFileType::NEO4J_JSON:
            return loadNeo4jJsonDB(graphName, graphPath, jobSystem);
        case GraphFileType::BINARY:
            return loadBinaryDB(graphName, graphPath, jobSystem);
        default:
            return false;
    }
}

bool SystemManager::loadGraph(const std::string& graphFileName, JobSystem& jobSystem) {
    const fs::Path graphPath = _config.getGraphsDir() / graphFileName;

    // Check if graph was already loaded || is already loading
    if (getGraph(graphFileName) || isGraphLoading(graphFileName)) {
        return false;
    }

    if (!graphPath.exists()) {
        return false;
    }

    const auto fileType = getGraphFileType(graphPath);
    if (!fileType) {
        // If we can not determine the file type, assume it is a Neo4j JSON graph
        // to be changed in the future
        return loadNeo4jJsonDB(graphFileName, graphPath, jobSystem);
    }

    switch (*fileType) {
        case GraphFileType::GML:
            return loadGmlDB(graphFileName, graphPath, jobSystem);
        case GraphFileType::NEO4J:
            return loadNeo4jDB(graphFileName, graphPath, jobSystem);
        case GraphFileType::NEO4J_JSON:
            return loadNeo4jJsonDB(graphFileName, graphPath, jobSystem);
        case GraphFileType::BINARY:
            return loadBinaryDB(graphFileName, graphPath, jobSystem);
        default:
            return false;
    }
}

// Load graph using non-default path
bool SystemManager::loadGraph(const fs::Path graphPath, const std::string& graphName, JobSystem& jobSystem) {
    // Check if graph was already loaded || is already loading
    if (getGraph(graphName) || isGraphLoading(graphName)) {
        return false;
    }

    if (!graphPath.exists()) {
        return false;
    }

    const auto fileType = getGraphFileType(graphPath);
    if (!fileType) {
        // If we can not determine the file type, assume it is a Neo4j JSON graph
        // to be changed in the future
        return loadNeo4jJsonDB(graphName, graphPath, jobSystem);
    }

    switch (*fileType) {
        case GraphFileType::GML:
            return loadGmlDB(graphName, graphPath, jobSystem);
        case GraphFileType::NEO4J:
            return loadGmlDB(graphName, graphPath, jobSystem);
        case GraphFileType::NEO4J_JSON:
            return loadNeo4jJsonDB(graphName, graphPath, jobSystem);
        case GraphFileType::BINARY:
            return loadBinaryDB(graphName, graphPath, jobSystem);
        default:
            return false;
    }
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

    auto graph = Graph::create(graphName);

    if (auto res = GraphLoader::load(graph.get(), dbPath); !res) {
        spdlog::error("Could not load graph {}: {}", graphName, res.error().fmtMessage());
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    addGraph(std::move(graph), graphName);
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

    auto graph = Graph::create();

    Neo4jImporter::ImportJsonDirArgs args;
    args._jsonDir = FileUtils::Path {dbPath.c_str()};

    if (!_neo4JImporter.importJsonDir(jobsystem,
                                      graph.get(),
                                      db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                      db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                      args)) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    addGraph(std::move(graph), graphName);
    _graphLoadStatus.removeLoadingGraph(graphName);
    return true;
}

bool SystemManager::loadNeo4jDB(const std::string& graphName,
                                const fs::Path& dbPath,
                                JobSystem& jobsystem) {
    if (!_graphLoadStatus.addLoadingGraph(graphName)) {
        return false;
    }

    auto graph = Graph::create();

    Neo4jImporter::DumpFileToJsonDirArgs dumpArgs;
    dumpArgs._workDir = "/tmp";
    dumpArgs._dumpFilePath = dbPath.c_str();

    if (!_neo4JImporter.fromDumpFileToJsonDir(jobsystem,
                                              graph.get(),
                                              db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                              db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                              dumpArgs)) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    Neo4jImporter::ImportJsonDirArgs args;
    args._jsonDir = FileUtils::Path {dumpArgs._workDir / "json"};

    if (!_neo4JImporter.importJsonDir(jobsystem,
                                      graph.get(),
                                      db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                      db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                      args)) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    addGraph(std::move(graph), graphName);
    _graphLoadStatus.removeLoadingGraph(graphName);
    return true;
}

bool SystemManager::loadGmlDB(const std::string& graphName,
                              const fs::Path& dbPath,
                              JobSystem& jobsystem) {
    if (!_graphLoadStatus.addLoadingGraph(graphName)) {
        return false;
    }

    // Load graph
    auto graph = Graph::create();

    // load GMLs
    GMLImporter importer;

    if (!importer.importFile(jobsystem, graph.get(), FileUtils::Path {dbPath.c_str()})) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    addGraph(std::move(graph), graphName);
    _graphLoadStatus.removeLoadingGraph(graphName);
    return true;
}

void SystemManager::listAvailableGraphs(std::vector<fs::Path>& names) {
    const auto list = _config.getGraphsDir().listDir();
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
    auto change = graph->newChange(baseHash);

    return _changes->storeChange(graph, std::move(change));
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

    auto changeRes = this->getChangeManager().getChange(graph, changeID);
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
