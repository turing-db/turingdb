#include "StorageManager.h"

#include <string>
#include <utility>

#include <spdlog/spdlog.h>

#include "Graph.h"
#include "GraphSerializer.h"

#include "FatalException.h"

using namespace db;

StorageManager::StorageManager(const fs::Path& graphsDir, bool syncedOnDisk)
    : _graphsDir(graphsDir),
    _syncedOnDisk(syncedOnDisk)
{
}

StorageManager::~StorageManager() {
}

Graph* StorageManager::getGraph(std::string_view graphName) const {
    const auto* slot = _graphs.getObject(graphName);
    if (!slot) {
        return nullptr;
    }

    return slot->getObject();
}

Graph* StorageManager::createGraph(std::string_view name) {
    const fs::Path path = _graphsDir / name;

    if (_syncedOnDisk && path.exists()) {
        throw FatalException(fmt::format("Graph '{}' already exists", name));
    }

    auto reservation = _graphs.reserve(name);
    if (!reservation.isValid()) {
        return nullptr;
    }

    auto graph = Graph::create(std::string(name), path);
    Graph* graphPtr = graph.get();

    if (_syncedOnDisk) {
        if (const auto res = graph->getSerializer().dump(); !res) {
            spdlog::error(res.error().fmtMessage());
            return nullptr;
        }
    }

    reservation.publish(std::move(graph));

    return graphPtr;
}

Graph* StorageManager::loadGraph(std::string_view name) {
    const fs::Path graphPath = _graphsDir / name;

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

bool StorageManager::addGraph(std::unique_ptr<Graph> graph) {
    const std::string_view name = graph->getName();

    auto reservation = _graphs.reserve(name);
    if (!reservation.isValid()) {
        return false;
    }

    reservation.publish(std::move(graph));

    return true;
}

void StorageManager::setDefaultGraph(std::string_view name) {
    Graph* graph = getGraph(name);
    if (graph) {
        _defaultGraph.store(graph);
    }
}

void StorageManager::listGraphs(std::vector<std::string_view>& names) const {
    _graphs.listNames(names);
}

void StorageManager::loadOrCreateDefaultGraph() {
    Graph* graph = loadGraph("default");

    if (!graph) {
        graph = createGraph("default");
    }

    if (!graph) {
        throw FatalException("Could not initialise the default graph");
    }

    _defaultGraph.store(graph);
}
