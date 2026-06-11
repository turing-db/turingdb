#include "GraphManager.h"

#include <string>
#include <utility>

#include <spdlog/spdlog.h>

#include "Graph.h"

#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "versioning/ChangeAccessor.h"

#include "GMLImporter.h"
#include "JsonlParser.h"

#include "TuringConfig.h"
#include "JobSystem.h"

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
    const ObjectMap<Graph>::ObjectSlot* slot = _graphs.getObject(graphName);
    if (!slot) {
        return nullptr;
    }

    return slot->getObject();
}

Graph* GraphManager::createGraph(std::string_view name) {
    const fs::Path path = _config->getGraphsDir() / name;

    if (_config->isSyncedOnDisk() && path.exists()) {
        throw TuringException(fmt::format("Graph '{}' already exists", name));
    }

    ObjectMap<Graph>::SlotReservation reservation = _graphs.reserve(name);
    if (!reservation.isValid()) {
        return nullptr;
    }

    auto graph = Graph::create(std::string(name), path);
    Graph* graphPtr = graph.get();

    if (_config->isSyncedOnDisk()) {
        if (const auto res = GraphDumper::dump(graphPtr, path); !res) {
            spdlog::error(res.error().fmtMessage());
            return nullptr;
        }
    }

    reservation.publish(std::move(graph));

    return graphPtr;
}

Graph* GraphManager::loadGraph(std::string_view name) {
    const fs::Path graphPath = _config->getGraphsDir() / name;

    ObjectMap<Graph>::SlotReservation reservation = _graphs.reserve(name);
    if (!reservation.isValid()) {
        spdlog::error("Failed to register graph '{}': "
                      "a graph with this name is already loaded", name);
        return nullptr;
    }

    auto graph = Graph::create(std::string(name), graphPath);
    Graph* graphPtr = graph.get();

    if (const auto res = GraphLoader::load(graph.get(), graphPath); !res) {
        spdlog::error("Failed to load graph '{}' from {}: {}",
                      name, graphPath.get(), res.error().fmtMessage());
        return nullptr;
    }

    reservation.publish(std::move(graph));

    return graphPtr;
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

Graph* GraphManager::importGraph(std::string_view graphName,
                                 const fs::Path& filePath,
                                 JobSystem* jobSystem,
                                const std::unordered_map<std::string_view, size_t>& embeddingSpecs) {
    const fs::Path graphPath = _config->getGraphsDir() / filePath;

    // Step 1. Check if graph was already loaded || is already loading
    if (getGraph(graphName) || isGraphLoading(graphName)) {
        return nullptr;
    }

    // Step 2. Validate the path. It should be within the data directory
    const fs::Path absolute = _config->getDataDir() / filePath;
    if (!absolute.isSubDirectory(_config->getDataDir())) {
        spdlog::error("File is not within the data directory: {}", absolute.get());
        return nullptr;
    }

    if (!absolute.exists()) {
        spdlog::error("File does not exist: {}", absolute.get());
        return nullptr;
    }

    // Step 3. Determine the file type
    const GraphFileType fileType = getGraphFileType(absolute);

    // Step 4. Load the graph
    switch (fileType) {
        case GraphFileType::GML:
            return loadGmlDB(graphName, absolute, jobSystem);
        break;
        case GraphFileType::JSONL:
            return loadJsonlDB(graphName, absolute, jobSystem, embeddingSpecs);
        break;
        case GraphFileType::BINARY:
            return loadBinaryDB(graphName, absolute, jobSystem);
        break;
        case GraphFileType::UNKNOWN:
            // If we can not determine the file type, assume it is a JSONL graph
            // to be changed in the future
            return loadJsonlDB(graphName, absolute, jobSystem);
        break;
        case GraphFileType::_SIZE:
            throw TuringException("Unsupported graph type");
        break;
    }

    throw TuringException("Unsupported graph type");
}

GraphFileType GraphManager::getGraphFileType(const fs::Path& graphPath) const {
    if (graphPath.extension() == ".gml") {
        return GraphFileType::GML;
    } else if (graphPath.extension() == ".json" || graphPath.extension() == ".jsonl") {
        return GraphFileType::JSONL;
    }

    const auto typeFilePath = graphPath / "type";
    std::string typeName;
    if (!FileUtils::readContent(typeFilePath.get(), typeName)) {
        return GraphFileType::UNKNOWN;
    }

    if (typeName == GraphFileTypeDescription::value(GraphFileType::BINARY)) {
        return GraphFileType::BINARY;
    } else if (typeName == GraphFileTypeDescription::value(GraphFileType::JSONL)) {
        return GraphFileType::JSONL;
    }

    return GraphFileType::UNKNOWN;
}

bool GraphManager::isGraphLoading(std::string_view graphName) const {
    return _graphLoadStatus.isGraphLoading(graphName);
}

Graph* GraphManager::loadBinaryDB(std::string_view graphName,
                                  const fs::Path& dbPath,
                                  JobSystem* jobSystem) {
    if (!_graphLoadStatus.addLoadingGraph(graphName)) {
        return nullptr;
    }

    ObjectMap<Graph>::SlotReservation reservation = _graphs.reserve(graphName);
    if (!reservation.isValid()) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return nullptr;
    }

    // in the case of turingDB binaries the path is the same path we load from.
    auto graph = Graph::create(std::string(graphName), dbPath);
    Graph* graphPtr = graph.get();

    if (auto res = GraphLoader::load(graph.get(), dbPath); !res) {
        spdlog::error("Could not load graph {}: {}", graphName, res.error().fmtMessage());
        _graphLoadStatus.removeLoadingGraph(graphName);
        return nullptr;
    }

    _graphLoadStatus.removeLoadingGraph(graphName);

    reservation.publish(std::move(graph));

    return graphPtr;
}

Graph* GraphManager::loadJsonlDB(std::string_view graphName,
                                 const fs::Path& dbPath,
                                 JobSystem* jobSystem,
                                 const std::unordered_map<std::string_view, size_t>& embeddingSpecs) {
    const fs::Path graphPath = _config->getGraphsDir() / graphName;
    if (graphPath == dbPath) {
        return nullptr;
    }

    if (!_graphLoadStatus.addLoadingGraph(graphName)) {
        return nullptr;
    }

    ObjectMap<Graph>::SlotReservation reservation = _graphs.reserve(graphName);
    if (!reservation.isValid()) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return nullptr;
    }

    auto graph = Graph::create(std::string(graphName), graphPath);
    Graph* graphPtr = graph.get();

    std::ifstream file(dbPath.get());
    if (!file) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return nullptr;
    }

    Change* change = _changes.createChange(graph.get(), CommitHash::head());
    ChangeAccessor changeAccessor = change->access();

    const auto importRes = JsonlParser::parse(changeAccessor, file, embeddingSpecs);

    if (!importRes) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        spdlog::error(importRes.error().fmtMessage());
        return nullptr;
    }

    const auto submitRes = _changes.submitChange(changeAccessor, *jobSystem);

    if (!submitRes) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        spdlog::error(submitRes.error().fmtMessage());
        return nullptr;
    }

    if (_config->isSyncedOnDisk()) {
        const auto dumpRes = GraphDumper::dump(graph.get(), graphPath);

        if (!dumpRes) {
            _graphLoadStatus.removeLoadingGraph(graphName);
            spdlog::error(dumpRes.error().fmtMessage());
            return nullptr;
        }
    }

    _graphLoadStatus.removeLoadingGraph(graphName);

    reservation.publish(std::move(graph));

    return graphPtr;
}

Graph* GraphManager::loadGmlDB(std::string_view graphName,
                               const fs::Path& dbPath,
                               JobSystem* jobSystem) {
    const fs::Path graphPath = _config->getGraphsDir() / graphName;
    if (graphPath == dbPath) {
        return nullptr;
    }

    if (!_graphLoadStatus.addLoadingGraph(graphName)) {
        return nullptr;
    }

    ObjectMap<Graph>::SlotReservation reservation = _graphs.reserve(graphName);
    if (!reservation.isValid()) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return nullptr;
    }

    // Load graph
    auto graph = Graph::create(std::string(graphName), graphPath);
    Graph* graphPtr = graph.get();

    // load GMLs
    GMLImporter importer;

    if (!importer.importFile(*jobSystem, graph.get(), FileUtils::Path {dbPath.c_str()})) {
        _graphLoadStatus.removeLoadingGraph(graphName);
        return nullptr;
    }

    if (_config->isSyncedOnDisk()) {
        if (!GraphDumper::dump(graph.get(), graphPath)) {
            _graphLoadStatus.removeLoadingGraph(graphName);
            return nullptr;
        }
    }

    _graphLoadStatus.removeLoadingGraph(graphName);

    reservation.publish(std::move(graph));

    return graphPtr;
}

ChangeResult<Change*> GraphManager::newChange(std::string_view graphName, CommitHash baseHash) {
    Graph* graph = getGraph(graphName);
    if (!graph) {
        return ChangeError::result(ChangeErrorType::GRAPH_NOT_FOUND);
    }

    return _changes.createChange(graph, baseHash);
}

ChangeResult<Change*> GraphManager::getChange(const Graph* graph, ChangeID changeID) {
    return _changes.getChange(graph, changeID);
}

ChangeResult<void> GraphManager::submitChange(ChangeAccessor& accessor, JobSystem& jobSystem) {
    return _changes.submitChange(accessor, jobSystem);
}

ChangeResult<void> GraphManager::deleteChange(ChangeAccessor& accessor, ChangeID changeID) {
    return _changes.deleteChange(accessor, changeID);
}

void GraphManager::listChanges(std::vector<const Change*>& changes, const Graph* graph) const {
    _changes.listChanges(changes, graph);
}

DataPartMergeResult<void> GraphManager::mergeDataParts(Graph* graph, JobSystem& jobSystem) {
    return _changes.mergeDataParts(graph, jobSystem);
}
