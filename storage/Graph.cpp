#include "Graph.h"

#include "GraphMetadata.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Commit.h"
#include "versioning/VersionController.h"

using namespace db;

Graph::Graph()
    : _graphName("default"),
      _metadata(new GraphMetadata()),
      _versionController(new VersionController)
{
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

Transaction Graph::openTransaction(CommitHash hash) const {
    return _versionController->openTransaction(hash);
}

WriteTransaction Graph::openWriteTransaction(CommitHash hash) const {
    return _versionController->openWriteTransaction(hash);
}

CommitResult<void> Graph::rebase(Commit& commit) {
    return _versionController->rebase(commit);
}

CommitResult<void> Graph::commit(std::unique_ptr<CommitBuilder> commitBuilder, JobSystem& jobSystem) {
    if (!commitBuilder) {
        return CommitError::result(CommitErrorType::COMMIT_INVALID);
    }

    return _versionController->commit(commitBuilder->build(jobSystem));
}

CommitResult<void> Graph::commit(std::unique_ptr<Commit> commit, JobSystem& jobSystem) {
    if (!commit) {
        return CommitError::result(CommitErrorType::COMMIT_INVALID);
    }

    return _versionController->commit(std::move(commit));
}

CommitResult<void> Graph::rebaseAndCommit(std::unique_ptr<CommitBuilder> commitBuilder, JobSystem& jobSystem) {
    if (!commitBuilder) {
        return CommitError::result(CommitErrorType::COMMIT_INVALID);
    }

    auto commit = commitBuilder->build(jobSystem);

    if (auto res = _versionController->rebase(*commit); !res) {
        return res;
    }

    return _versionController->commit(std::move(commit));
}

CommitResult<void> Graph::rebaseAndCommit(std::unique_ptr<Commit> commit, JobSystem& jobSystem) {
    if (!commit) {
        return CommitError::result(CommitErrorType::COMMIT_INVALID);
    }

    if (auto res = _versionController->rebase(*commit); !res) {
        return res;
    }

    return _versionController->commit(std::move(commit));
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
