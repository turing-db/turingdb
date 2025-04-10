#include "SystemManager.h"

#include <shared_mutex>
#include <mutex>
#include <spdlog/spdlog.h>

#include "Graph.h"
#include "Neo4j/Neo4JParserConfig.h"
#include "Neo4jImporter.h"
#include "GMLImporter.h"
#include "JobSystem.h"
#include "GraphLoader.h"
#include "FileUtils.h"
#include "Panic.h"

using namespace db;

SystemManager::SystemManager() {
    const char* home = std::getenv("HOME");
    if (!home) {
        panic("HOME environment variable not set");
    }

    _graphsDir = fs::Path(home) / "graphs_v2";
    if (!_graphsDir.exists()) {
        panic("graphs_v2 directory not found at {}", _graphsDir.get());
    }

    _defaultGraph = createGraph("default");
}

SystemManager::~SystemManager() {
}

void SystemManager::setGraphsDir(const fs::Path& dir) {
    _graphsDir = dir;
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
    std::unique_lock guard(_lock);

    // Search if a graph with the same name exists
    const auto it = _graphs.find(name);
    if (it != _graphs.end()) {
        return false;
    }

    _graphs[name] = std::move(graph);
    return true;
}

Graph* SystemManager::getDefaultGraph() const {
    std::shared_lock guard(_lock);
    return _defaultGraph;
}

void SystemManager::setDefaultGraph(const std::string& name) {
    std::unique_lock guard(_lock);

    const auto it = _graphs.find(name);
    if (it != _graphs.end()) {
        _defaultGraph = it->second.get();
    }
}

Graph* SystemManager::getGraph(const std::string& graphName) const {
    std::shared_lock guard(_lock);

    const auto it = _graphs.find(graphName);
    if (it == _graphs.end()) {
        return nullptr;
    }

    return it->second.get();
}

void SystemManager::listGraphs(std::vector<std::string_view>& names) {
    std::shared_lock guard(_lock);

    for (const auto& [name, graph] : _graphs) {
        names.push_back(name);
    }
}

bool SystemManager::loadGraph(const std::string& graphName) {
    const fs::Path graphPath = fs::Path(_graphsDir) / graphName;

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
        return loadNeo4jJsonDB(graphName, graphPath);
    }

    switch (*fileType) {
        case GraphFileType::GML:
            return loadGmlDB(graphName, graphPath);
        case GraphFileType::NEO4J_JSON:
            return loadNeo4jJsonDB(graphName, graphPath);
        case GraphFileType::BINARY:
            return loadBinaryDB(graphName, graphPath);
        default:
            return false;
    }
}

std::optional<GraphFileType> SystemManager::getGraphFileType(const fs::Path& graphPath) {
    if (graphPath.extension() == ".gml") {
        return GraphFileType::GML;
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
                                 const fs::Path& dbPath) {
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
                                    const fs::Path& dbPath) {
    if (!_graphLoadStatus.addLoadingGraph(graphName)) {
        return false;
    }

    auto graph = Graph::create();
    auto jobsystem = JobSystem::create();

    Neo4jImporter::ImportJsonDirArgs args;
    args._jsonDir = FileUtils::Path {dbPath.c_str()};

    if (!Neo4jImporter::importJsonDir(*jobsystem,
                                      graph.get(),
                                      db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                      db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                      args)) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    addGraph(std::move(graph), graphName);
    _graphLoadStatus.removeLoadingGraph(graphName);
    jobsystem->terminate();
    return true;
}

bool SystemManager::loadGmlDB(const std::string& graphName,
                              const fs::Path& dbPath) {
    if (!_graphLoadStatus.addLoadingGraph(graphName)) {
        return false;
    }

    // Load graph
    auto graph = Graph::create();
    auto jobsystem = JobSystem::create();

    // load GMLs
    GMLImporter importer;

    if (!importer.importFile(*jobsystem, graph.get(), FileUtils::Path {dbPath.c_str()})) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    addGraph(std::move(graph), graphName);
    _graphLoadStatus.removeLoadingGraph(graphName);
    jobsystem->terminate();
    return true;
}

void SystemManager::listAvailableGraphs(std::vector<fs::Path>& names) {
    const auto list = fs::Path(_graphsDir).listDir();
    if (!list) {
        spdlog::error("Failed to list available graphs in {}", _graphsDir.get());
        return;
    }

    for (const auto& path : list.value()) {
        names.push_back(path);
    }
}

BasicResult<Transaction, std::string_view> SystemManager::openTransaction(const std::string& graphName,
                                                                          const CommitHash& commit) const {
    const auto* graph = getGraph(graphName);
    if (!graph) {
        return BadResult<std::string_view> {"Graph does not exist"};
    }

    Transaction tr = graph->openTransaction(commit);
    if (!tr.isValid()) {
        return BadResult<std::string_view> {"Invalid commit hash"};
    }

    return tr;
}
