#include "Graph.h"

#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Commit.h"
#include "versioning/VersionController.h"

using namespace db;

Graph::~Graph() {
}

Transaction Graph::openTransaction(CommitHash hash) const {
    return _versionController->openTransaction(hash);
}

WriteTransaction Graph::openWriteTransaction(CommitHash hash) const {
    return _versionController->openWriteTransaction(hash);
}

CommitResult<void> Graph::commit(std::unique_ptr<CommitBuilder>& commitBuilder, JobSystem& jobSystem) {
    if (!commitBuilder) {
        return CommitError::result(CommitErrorType::COMMIT_INVALID);
    }

    return _versionController->commit(commitBuilder, jobSystem);
}

CommitResult<void> Graph::rebaseAndCommit(std::unique_ptr<CommitBuilder> commitBuilder, JobSystem& jobSystem) {
    if (!commitBuilder) {
        return CommitError::result(CommitErrorType::COMMIT_INVALID);
    }

    if (auto res = _versionController->rebase(*commitBuilder, jobSystem); !res) {
        return res;
    }

    return _versionController->commit(commitBuilder, jobSystem);
}

Graph::EntityIDs Graph::getNextFreeIDs() const {
    std::shared_lock lock(_entityIDsMutex);
    return _nextFreeIDs;
}

CommitHash Graph::getHeadHash() const {
    return _versionController->getHeadHash();
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

std::unique_ptr<Graph> Graph::create() {
    auto* graph = new Graph;
    graph->_versionController->createFirstCommit(graph);
    return std::unique_ptr<Graph> {graph};
}

std::unique_ptr<Graph> Graph::create(const std::string& name) {
    auto* graph = new Graph(name);
    graph->_versionController->createFirstCommit(graph);
    return std::unique_ptr<Graph>(graph);
}

std::unique_ptr<Graph> Graph::createEmptyGraph() {
    return std::unique_ptr<Graph>(new Graph);
}

std::unique_ptr<Graph> Graph::createEmptyGraph(const std::string& name) {
    return std::unique_ptr<Graph>(new Graph(name));
}


Graph::Graph()
    : _graphName("default"),
    _versionController(new VersionController)
{
}

Graph::Graph(const std::string& name)
    : _graphName(name),
    _versionController(new VersionController)
{
}
