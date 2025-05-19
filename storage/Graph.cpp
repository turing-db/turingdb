#include "Graph.h"

#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "versioning/Change.h"
#include "versioning/Commit.h"
#include "versioning/VersionController.h"
#include "versioning/Transaction.h"

using namespace db;

Graph::~Graph() {
}

std::unique_ptr<Change> Graph::newChange(CommitHash base) {
    return _versionController->newChange(base);
}

Transaction Graph::openTransaction(CommitHash hash) const {
    return _versionController->openTransaction(hash);
}

CommitHash Graph::getHeadHash() const {
    return _versionController->getHeadHash();
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
