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

SystemManager::SystemManager()
{
    const char* home = std::getenv("HOME");
    if (!home) {
        panic("HOME environment variable not set");
    }

    _graphsDir = fs::Path(home)/"graphs_v2";
    if (!_graphsDir.exists()) {
        panic("graphs_v2 directory not found at {}", _graphsDir.get());
    }

    _defaultGraph = createGraph("default");
}

SystemManager::~SystemManager() {
    for (const auto& [name, graph] : _graphs) {
        delete graph;
    }
}

void SystemManager::setGraphsDir(const fs::Path& dir) {
    _graphsDir = dir;
}

Graph* SystemManager::createGraph(const std::string& name) {
    Graph* graph = new Graph(name);
    
    if (!addGraph(graph, name)) {
        delete graph;
        return nullptr;
    }

    return graph;
}

bool SystemManager::addGraph(Graph* graph, const std::string& name) {
    std::unique_lock guard(_lock);

    // Search if a graph with the same name exists
    const auto it = _graphs.find(name);
    if (it != _graphs.end()) {
        return false;
    }

    _graphs[name] = graph;
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
        _defaultGraph = it->second;
    }
}

Graph* SystemManager::getGraph(const std::string& graphName) const {
    std::shared_lock guard(_lock);

    const auto it = _graphs.find(graphName);
    if (it == _graphs.end()) {
        return nullptr;
    }

    return it->second;
}

void SystemManager::listGraphs(std::vector<std::string_view>& names) {
    std::shared_lock guard(_lock);

    for (const auto& [name, graph] : _graphs) {
        names.push_back(name);
    }
}

bool SystemManager::loadGraph(const std::string& graphName) {
    const fs::Path graphPath = fs::Path(_graphsDir)/graphName;

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

    const auto typeFilePath = graphPath/"type";
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

    Graph* graph = new Graph();

    if (!GraphLoader::load(graph, dbPath)) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        delete graph;
        return false;
    }

    addGraph(graph, graphName);
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

    Graph* graph = new Graph();
    auto jobsystem = JobSystem::create();

    Neo4jImporter::ImportJsonDirArgs args;
    args._jsonDir = FileUtils::Path {dbPath.c_str()};

    if (!Neo4jImporter::importJsonDir(*jobsystem,
                                      graph,
                                      db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                      db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                      args)) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        delete graph;
        return false;
    }

    addGraph(graph, graphName);
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
    Graph* graph = new Graph();
    auto jobsystem = JobSystem::create();

    // load GMLs
    GMLImporter importer;

    if (!importer.importFile(*jobsystem, graph, FileUtils::Path {dbPath.c_str()})) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        delete graph;
        return false;
    }

    addGraph(graph, graphName);
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
