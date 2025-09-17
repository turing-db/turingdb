#include "DumpAndLoadManager.h"

#include <iostream>
#include <mutex>

#include "Graph.h"
#include "GraphLoader.h"
#include "versioning/VersionController.h"
#include "GraphDumper.h"
#include "CommitDumper.h"
#include "versioning/Commit.h"


using namespace db;

DumpAndLoadManager::DumpAndLoadManager(Graph* graph)
    :_graph(graph)
{
}

DumpResult<void> DumpAndLoadManager::loadGraph() const {
    std::unique_lock<std::mutex> lock(_mutex);
    return GraphLoader::load(_graph, _graph->getPath());
}

DumpResult<void> DumpAndLoadManager::dumpGraph() const {
    std::unique_lock<std::mutex> lock(_mutex);
    return GraphDumper::dump(*_graph, _graph->getPath());
}

DumpResult<void> DumpAndLoadManager::dumpCommit(const Commit& commit) const {
    std::unique_lock<std::mutex> lock(_mutex);
    if (commit.controller().getGraph() != _graph) {
        return DumpError::result(DumpErrorType::COMMIT_DOES_NOT_EXIST);
    }

    // is this guaranteed to return a valid value (not -1)?
    const long index = commit.controller().getCommitIndex(commit.hash());

    const std::string fileName = fmt::format("commit-{}-{}", index, commit.hash().get());
    const fs::Path commitPath = _graph->getPath() / fileName;

    return CommitDumper::dump(commit, commitPath);
}
