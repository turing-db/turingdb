#include "VersionController.h"

#include <optional>

#include <range/v3/view/enumerate.hpp>
#include <sys/types.h>
#include <shared_mutex>

#include "JobSystem.h"
#include "Graph.h"
#include "dump/CommitLoader.h"
#include "dump/DumpResult.h"
#include "CommitView.h"
#include "mergers/DataPartMerger.h"
#include "writers/DataPartBuilder.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/CommitHash.h"
#include "versioning/DataPartRebaser.h"
#include "versioning/Transaction.h"
#include "CommitJournal.h"

#include "Profiler.h"
#include "BioAssert.h"

using namespace db;

VersionController::VersionController(Graph* graph)
    : _graph(graph),
    _dataManager(std::make_unique<ArcManager<CommitData>>()),
    _partManager(std::make_unique<ArcManager<DataPart>>())
{
}

VersionController::~VersionController() {
    _commits.clear();
    _dataManager.reset();
    _partMap.clear(); // Delete all WeakArc<DataPart> before deleting @ref _partManager
    _partManager.reset();
    // @ref ~IndexManager() calls reset() on its arc manager
}

void VersionController::createFirstCommit() {
    auto commitData = _dataManager->create(CommitHash::create());
    // initialise the previous Commit* of the first commit to nullptr
    auto commit = std::make_unique<Commit>(this, commitData, nullptr);

    this->addCommit(std::move(commit));
}

DataPartMergeResult<void> VersionController::mergeDataParts(JobSystem& jobSystem) {
    Profile profile("VersionController::mergeDataParts");

    std::unique_lock<std::shared_mutex> uniqueLock(_mutex);

    Commit* headCommit = _head.load();

    auto newTip = CommitBuilder::prepareMerge(*this,
                                              nullptr,
                                              headCommit);

    const auto merger = DataPartMerger(&headCommit->data(), newTip->metadata());

    newTip->appendBuilder(merger.merge(headCommit->data().allDataparts()));

    auto buildRes = newTip->build(jobSystem);
    if (!buildRes) {
        return DataPartMergeError::result(DataPartMergeErrorType::MERGE_GRAPH_FAILED);
    }

    addCommit(std::move(buildRes.value()));

    return {};
}

FrozenCommitTx VersionController::openTransaction(CommitHash hash) const {
    if (hash == CommitHash::head()) {
        return _head.load()->openTransaction();
    }

    std::shared_lock<std::shared_mutex> sharedLock(_mutex);

    auto it = _offsets.find(hash);
    if (it == _offsets.end()) {
        return FrozenCommitTx(); // Invalid hash
    }

    return _commits[it->second]->openTransaction();
}

CommitHash VersionController::getHeadHash() const {
    std::shared_lock<std::shared_mutex> sharedLock(_mutex);

    const Commit* head = _head.load();
    if (!head) {
        return CommitHash::head();
    }

    return head->hash();
}

CommitResult<void> VersionController::submitChange(Change* change, JobSystem& jobSystem) {
    Profile profile("VersionController::submitChange");

    std::unique_lock<std::shared_mutex> uniqueLock(_mutex);

    // atomic load main
    Commit* headCommit = _head.load();

    // rebase if main has changed under us
    if (headCommit->hash() != change->baseHash()) {
        if (auto res = change->rebase(jobSystem); !res) {
            return res;
        }
    }

    for (auto& commitBuilder : change->_commits) {
        // If this Change has modifications which were not applied by a "COMMIT" command,
        // then flush them now
        if (!commitBuilder->writeBuffer().isFlushed()) {
            commitBuilder->flushWriteBuffer(jobSystem);
        }

        auto buildRes = commitBuilder->build(jobSystem);
        if (!buildRes) {
            return buildRes.get_unexpected();
        }

        auto& newCommit = buildRes.value();

        addCommit(std::move(newCommit));
    }

    return {};
}

std::unique_ptr<Change> VersionController::newChange(CommitHash base) {
    return Change::create(this, ChangeID {_nextChangeID.fetch_add(1)}, base);
}

std::unique_lock<std::shared_mutex> VersionController::lock() {
    return std::unique_lock<std::shared_mutex> {_mutex};
}

// Needs to be called in a locked context
void VersionController::addCommit(std::unique_ptr<Commit> commit) {
    auto* ptr = commit.get();

    _offsets.emplace(commit->hash(), _commits.size());
    _commits.emplace_back(std::move(commit));
    _head.store(ptr);
}

std::optional<size_t> VersionController::getCommitIndex(CommitHash hash) const {
    auto it = _offsets.find(hash);

    if (it == _offsets.end()) {
        return std::nullopt;
    }

    return it->second;
}

const Commit* VersionController::getCommitUnsafe(CommitHash hash) const {
    if (hash == CommitHash::head()) {
        return _head.load();
    }

    const std::optional<size_t> offset = getCommitIndex(hash);

    if (!offset.has_value()) {
        return nullptr;
    }

    return _commits[*offset].get();
}

const Commit* VersionController::getCommitSafe(CommitHash hash) const {
    std::shared_lock lock(_mutex);

    if (hash == CommitHash::head()) {
        return _head.load();
    }

    const std::optional<size_t> offset = getCommitIndex(hash);

    if (!offset.has_value()) {
        return nullptr;
    }

    return _commits[*offset].get();
}

// NOTE: Called within locked-context
Commit::CommitSpan VersionController::getCommitsSinceCommitHash(CommitHash from) const {
    // Should not be trying to check conflicts if we are not rebasing
    bioassert(from != CommitHash::head(), "should not be checking conflicts if not rebasing");

    const std::optional<size_t> startIndexOpt = getCommitIndex(from);
    bioassert(startIndexOpt, "Could not find Commit with hash {:x}", from.get());

    const size_t startIndex = *startIndexOpt;

    // +1 to skip the commit we branched from
    const auto* spanStart = _commits.data() + startIndex + 1;
    const size_t numCommitsSinceFrom = _commits.size() - (startIndex + 1);

    return {spanStart, numCommitsSinceFrom};
}

DumpResult<void> VersionController::loadCommit(CommitHash hash,
                                               const fs::Path& commitDir,
                                               const fs::Path& partsDir,
                                               Graph* graph) {
    std::shared_lock lock(_mutex);

    const auto offset = getCommitIndex(hash);
    if (!offset.has_value()) {
        return DumpError::result(DumpErrorType::COMMIT_HASH_NOT_FOUND);
    }

    Commit* commit = _commits[offset.value()].get();
    if (commit->hasData()) {
        return {}; // already loaded — success
    }

    auto res = CommitLoader::loadData(commitDir, partsDir, graph, commit);
    if (!res) {
        return res.get_unexpected();
    }
    return {};
}


template <SupportedType P>
WeakArc<Index> VersionController::createNodePropertyIndex(std::string_view indexName,
                                                          PropertyTypeID ptID) {
    return _indexManager.createNodeIndex<P>(indexName, ptID);
}

template <SupportedType P>
WeakArc<Index> VersionController::createEdgePropertyIndex(std::string_view indexName,
                                                          PropertyTypeID ptID) {
    return _indexManager.createEdgeIndex<P>(indexName, ptID);
}

namespace db {
template WeakArc<Index> VersionController::createNodePropertyIndex<types::Int64>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> VersionController::createNodePropertyIndex<types::UInt64>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> VersionController::createNodePropertyIndex<types::Double>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> VersionController::createNodePropertyIndex<types::String>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> VersionController::createNodePropertyIndex<types::Bool>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> VersionController::createNodePropertyIndex<types::Embedding>(std::string_view indexName, PropertyTypeID ptID);

template WeakArc<Index> VersionController::createEdgePropertyIndex<types::Int64>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> VersionController::createEdgePropertyIndex<types::UInt64>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> VersionController::createEdgePropertyIndex<types::Double>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> VersionController::createEdgePropertyIndex<types::String>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> VersionController::createEdgePropertyIndex<types::Bool>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> VersionController::createEdgePropertyIndex<types::Embedding>(std::string_view indexName, PropertyTypeID ptID);
}
