#include "Graph.h"

#include "GraphSerializer.h"
#include "versioning/ChangeManager.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "versioning/Change.h"
#include "versioning/Commit.h"
#include "versioning/VersionController.h"
#include "versioning/Transaction.h"

using namespace db;

Graph::~Graph() {
}

FrozenCommitTx Graph::openTransaction(CommitHash hash) const {
    return _versionController->openTransaction(hash);
}

CommitHash Graph::getHeadHash() const {
    return _versionController->getHeadHash();
}

std::unique_ptr<Graph> Graph::create() {
    auto* graph = new Graph();
    graph->_versionController->createFirstCommit();
    return std::unique_ptr<Graph> {graph};
}

std::unique_ptr<Graph> Graph::create(const std::string& name, const fs::Path& path) {
    auto* graph = new Graph(name, path);
    graph->_versionController->createFirstCommit();
    return std::unique_ptr<Graph>(graph);
}

Graph::Graph()
    : _graphName("default"),
    _graphPath("/dev/null"),
    _versionController(new VersionController {this}),
    _changeManager(new ChangeManager {this->_versionController.get(),
                                      _versionController->_partManager.get(),
                                      _versionController->_dataManager.get()}),
    _serializer(new GraphSerializer {this})
{
}

Graph::Graph(const std::string& name, const fs::Path& path)
    : _graphName(name),
    _graphPath(path),
    _versionController(new VersionController {this}),
    _changeManager(new ChangeManager {this->_versionController.get(),
                                      _versionController->_partManager.get(),
                                      _versionController->_dataManager.get()}),
    _serializer(new GraphSerializer {this})
{
}
