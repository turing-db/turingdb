#include "GraphSerializer.h"

#include <mutex>

#include "Graph.h"
#include "GraphLoader.h"
#include "GraphDumper.h"
#include "spdlog/spdlog.h"


using namespace db;

GraphSerializer::GraphSerializer(Graph* graph)
    : _graph(graph)
{
}

DumpResult<void> GraphSerializer::load() const {
    std::unique_lock<std::mutex> lock(_mutex);
    spdlog::info("Loading graph {} to {}", _graph->getName(), _graph->getPath());
    return GraphLoader::load(_graph, fs::Path {_graph->getPath()});
}

DumpResult<void> GraphSerializer::dump() const {
    std::unique_lock<std::mutex> lock(_mutex);
    spdlog::info("Dumping graph {} to {}", _graph->getName(), _graph->getPath());
    return GraphDumper::dump(*_graph, fs::Path {_graph->getPath()});
}
