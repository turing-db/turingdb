#include "GraphSerializer.h"

#include <spdlog/spdlog.h>

#include "Graph.h"
#include "dump/GraphLoader.h"
#include "dump/GraphDumper.h"

using namespace db;

GraphSerializer::GraphSerializer(Graph* graph)
    : _graph(graph)
{
}

DumpResult<void> GraphSerializer::load() const {
    spdlog::info("Loading graph {}", _graph->getName());
    return GraphLoader::load(_graph, _graph->getPath());
}

DumpResult<void> GraphSerializer::dump() const {
    spdlog::info("Dumping graph {}", _graph->getName());
    return GraphDumper::dump(*_graph, _graph->getPath());
}
