#include "Graph.h"

#include "GraphMetadata.h"
#include "DataPartManager.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"
#include "writers/ConcurrentWriter.h"
#include "writers/DataPartBuilder.h"

using namespace db;

Graph::Graph()
    : _graphName("default"),
      _metadata(new GraphMetadata()),
      _parts(std::make_unique<DataPartManager>())
{
}

Graph::Graph(const std::string& name)
    : _graphName(name),
      _metadata(new GraphMetadata()),
      _parts(std::make_unique<DataPartManager>())
{
}

Graph::~Graph() {
}

GraphView Graph::view() {
    return GraphView(_parts->getView(), *_metadata);
}

GraphView Graph::view() const {
    return GraphView(_parts->getView(), *_metadata);
}

GraphReader Graph::read() {
    return GraphReader(view());
}

GraphReader Graph::read() const {
    return GraphReader(view());
}

std::unique_ptr<DataPartBuilder> Graph::newPartWriter() {
    const auto [firstNodeID, firstEdgeID] = getNextFreeIDs();
    return std::make_unique<DataPartBuilder>(firstNodeID, firstEdgeID, this);
}

std::unique_ptr<ConcurrentWriter> Graph::newConcurrentPartWriter() {
    const auto [firstNodeID, firstEdgeID] = getNextFreeIDs();
    return std::make_unique<ConcurrentWriter>(firstNodeID, firstEdgeID, this);
}

Graph::EntityIDs Graph::getNextFreeIDs() const {
    std::shared_lock lock(_entityIDsMutex);
    return _nextFreeIDs;
}

Graph::EntityIDs Graph::allocIDs() {
    std::unique_lock lock(_entityIDsMutex);
    const auto ids = _nextFreeIDs;
    ++_nextFreeIDs._node;
    ++_nextFreeIDs._edge;
    return ids;
}

Graph::EntityIDs Graph::allocIDRange(size_t nodeCount, size_t edgeCount) {
    std::unique_lock lock(_entityIDsMutex);
    const auto ids = _nextFreeIDs;
    _nextFreeIDs._node += nodeCount;
    _nextFreeIDs._edge += edgeCount;
    return ids;
}
