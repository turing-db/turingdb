#include "GraphManager.h"

#include <fstream>
#include <string>
#include <utility>

#include <spdlog/spdlog.h>

#include "TuringConfig.h"
#include "JobSystem.h"

#include "Graph.h"
#include "GraphSerializer.h"
#include "versioning/ChangeAccessor.h"

#include "GMLImporter.h"
#include "JsonlParser.h"

#include "Path.h"
#include "FileUtils.h"
#include "FatalException.h"
#include "TuringException.h"

using namespace db;

GraphManager::GraphManager(const TuringConfig* config)
    : _config(config)
{
}

GraphManager::~GraphManager() {
}

Graph* GraphManager::getGraph(std::string_view graphName) const {
    const auto* slot = _graphs.getObject(graphName);
    if (!slot) {
        return nullptr;
    }

    return slot->getObject();
}

Graph* GraphManager::createGraph(std::string_view name) {
    const fs::Path path = _config->getGraphsDir() / name;

    if (_config->isSyncedOnDisk() && path.exists()) {
        throw FatalException(fmt::format("Graph '{}' already exists", name));
    }

    auto reservation = _graphs.reserve(name);
    if (!reservation.isValid()) {
        return nullptr;
    }

    auto graph = Graph::create(std::string(name), path);
    Graph* graphPtr = graph.get();

    if (_config->isSyncedOnDisk()) {
        if (const auto res = graph->getSerializer().dump(); !res) {
            spdlog::error(res.error().fmtMessage());
            return nullptr;
        }
    }

    reservation.publish(std::move(graph));

    return graphPtr;
}

Graph* GraphManager::loadGraph(std::string_view name) {
    const fs::Path graphPath = _config->getGraphsDir() / name;

    auto reservation = _graphs.reserve(name);
    if (!reservation.isValid()) {
        spdlog::error("Failed to register graph '{}': "
                      "a graph with this name is already loaded", name);
        return nullptr;
    }

    auto graph = Graph::create(std::string(name), graphPath);
    Graph* graphPtr = graph.get();

    if (const auto res = graph->getSerializer().load(); !res) {
        spdlog::error("Failed to load graph '{}' from {}: {}",
                      name, graphPath.get(), res.error().fmtMessage());
        return nullptr;
    }

    reservation.publish(std::move(graph));

    return graphPtr;
}

bool GraphManager::addGraph(std::unique_ptr<Graph> graph) {
    const std::string_view name = graph->getName();

    auto reservation = _graphs.reserve(name);
    if (!reservation.isValid()) {
        return false;
    }

    reservation.publish(std::move(graph));

    return true;
}

void GraphManager::setDefaultGraph(std::string_view name) {
    Graph* graph = getGraph(name);
    if (graph) {
        _defaultGraph.store(graph);
    }
}

void GraphManager::listGraphs(std::vector<std::string_view>& names) const {
    _graphs.listNames(names);
}

void GraphManager::loadOrCreateDefaultGraph() {
    Graph* graph = loadGraph("default");

    if (!graph) {
        graph = createGraph("default");
    }

    if (!graph) {
        throw FatalException("Could not initialise the default graph");
    }

    _defaultGraph.store(graph);
}

bool GraphManager::importGraph(std::string_view graphName, const fs::Path& filePath, JobSystem& jobSystem) {
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

std::optional<GraphFileType> GraphManager::getGraphFileType(const fs::Path& graphPath) {
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

bool GraphManager::isGraphLoading(std::string_view graphName) const {
    return _graphLoadStatus.isGraphLoading(graphName);
}

bool GraphManager::loadBinaryDB(std::string_view graphName, const fs::Path& dbPath, JobSystem& jobSystem) {
    if (!_graphLoadStatus.addLoadingGraph(graphName)) {
        return false;
    }

    // in the case of turingDB binaries the path is the same path we load from.
    auto graph = Graph::create(std::string(graphName), dbPath);

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

bool GraphManager::loadJsonlDB(std::string_view graphName, const fs::Path& dbPath, JobSystem& jobSystem) {
    if (!_graphLoadStatus.addLoadingGraph(graphName)) {
        return false;
    }

    const fs::Path graphPath = _config->getGraphsDir() / graphName;
    if (graphPath == dbPath) {
        return false;
    }

    auto graph = Graph::create(std::string(graphName), graphPath);
    std::ifstream file(dbPath.get());
    if (!file) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return false;
    }

    ChangeManager& changeManager = getChangeManager();
    Change* change = changeManager.createChange(graph.get(), CommitHash::head());
    ChangeAccessor changeAccessor = change->access();

    const auto importRes = JsonlParser::parse(changeAccessor, file);

    if (!importRes) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        spdlog::error(importRes.error().fmtMessage());
        return false;
    }

    const auto submitRes = changeManager.submitChange(changeAccessor, jobSystem);

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

bool GraphManager::loadGmlDB(std::string_view graphName, const fs::Path& dbPath, JobSystem& jobSystem) {
    if (!_graphLoadStatus.addLoadingGraph(graphName)) {
        return false;
    }

    const fs::Path graphPath = _config->getGraphsDir() / graphName;
    if (graphPath == dbPath) {
        return false;
    }

    // Load graph
    auto graph = Graph::create(std::string(graphName), graphPath);

    // load GMLs
    GMLImporter importer;

    if (!importer.importFile(jobSystem, graph.get(), FileUtils::Path {dbPath.c_str()})) {
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
