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

std::unique_ptr<Graph> Graph::create(const std::string& name, const fs::Path& localPath) {
    auto* graph = new Graph(name, localPath);
    graph->_versionController->createFirstCommit();
    return std::unique_ptr<Graph>(graph);
}

std::unique_ptr<Graph> Graph::createEmptyGraph() {
    return std::unique_ptr<Graph>(new Graph());
}

std::unique_ptr<Graph> Graph::createEmptyGraph(const std::string& name, const fs::Path& localPath) {
    return std::unique_ptr<Graph>(new Graph(name, localPath));
}


Graph::Graph()
    : _graphName("default"),
    _graphPath("/dev/null"),
    _versionController(new VersionController {this}),
    _dumpAndLoadManger(new DumpAndLoadManager {this})
{
}

Graph::Graph(const std::string& name, const fs::Path& localPath)
    : _graphName(name),
    _graphPath(localPath),
    _versionController(new VersionController {this}),
    _dumpAndLoadManger(new DumpAndLoadManager {this})
{
}
