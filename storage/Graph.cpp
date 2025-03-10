#include "Graph.h"

#include "GraphMetadata.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"
#include "writers/DataPartBuilder.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Commit.h"
#include "versioning/VersionController.h"

using namespace db;

Graph::Graph()
    : _graphName("default"),
      _metadata(new GraphMetadata()),
      _versionController(new VersionController) {
    _versionController->initialize(this);
}

Graph::Graph(const std::string& name)
    : _graphName(name),
      _metadata(new GraphMetadata()),
      _versionController(new VersionController)
{
    _versionController->initialize(this);
}

Graph::~Graph() {
}

GraphView Graph::view(CommitHash hash) {
    return _versionController->view(hash);
}

GraphView Graph::view(CommitHash hash) const {
    return _versionController->view(hash);
}

GraphReader Graph::read() {
    return GraphReader(view());
}

GraphReader Graph::read() const {
    return GraphReader(view());
}

std::unique_ptr<CommitBuilder> Graph::prepareCommit() const {
    return view().prepareCommit();
}

void Graph::commit(std::unique_ptr<CommitBuilder> commitBuilder, JobSystem& jobSystem) {
    _versionController->commit(commitBuilder->build(*this, jobSystem));
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
