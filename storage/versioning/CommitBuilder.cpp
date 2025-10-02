#include "CommitBuilder.h"

#include <bits/ranges_algo.h>
#include <range/v3/view/enumerate.hpp>

#include "EnumerateFrom.h"
#include "JobSystem.h"
#include "reader/GraphReader.h"
#include "Profiler.h"
#include "Graph.h"
#include "versioning/Commit.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/DataPartModifier.h"
#include "versioning/VersionController.h"
#include "versioning/Transaction.h"
#include "versioning/CommitView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

CommitBuilder::CommitBuilder() = default;

CommitBuilder::~CommitBuilder() = default;

std::unique_ptr<CommitBuilder> CommitBuilder::prepare(VersionController& controller,
                                                      Change* change,
                                                      const GraphView& view) {
    auto* ptr = new CommitBuilder {controller, change, view};
    ptr->initialize();
    return std::unique_ptr<CommitBuilder> {ptr};
}

CommitHash CommitBuilder::hash() const {
    return _commitData->hash();
}

GraphView CommitBuilder::viewGraph() const {
    return GraphView {*_commitData};
}

GraphReader CommitBuilder::readGraph() const {
    return viewGraph().read();
}

DataPartBuilder& CommitBuilder::newBuilder() {
    std::scoped_lock lock {_mutex};
    GraphView view {*_commitData};
    const size_t partIndex = view.dataparts().size() + _builders.size();
    auto& builder = _builders.emplace_back(DataPartBuilder::prepare(*_metadataBuilder, view, partIndex));

    return *builder;
}

DataPartBuilder& CommitBuilder::newBuilder(size_t partIndex) {
    std::scoped_lock lock {_mutex};
    GraphView view {*_commitData};
    auto& builder = _builders.emplace_back(DataPartBuilder::prepare(*_metadataBuilder, view, partIndex));

    return *builder;
}

CommitResult<void> CommitBuilder::buildNewDataPart(JobSystem& jobsystem,
                                                   const GraphView view,
                                                   DataPartBuilder* builder,
                                                   CommitHistoryBuilder& historyBuilder) {
    // If caller is @ref Change::submit, @ref _nextNodeID _nextEdgeID are synced to
    // the current next ID for the latest commit on main. If the caller is @ref
    // Change::commit we do not want to perform this sync, and can continue with our
    // local next ID.
    auto part = _controller->createDataPart(_nextNodeID, _nextEdgeID);

    // Update these values so the next builder which is created starts where the last
    // left off
    _nextNodeID += builder->nodeCount();
    _nextEdgeID += builder->edgeCount();

    if (!part->load(view, jobsystem, *builder)) {
        return CommitError::result(CommitErrorType::BUILD_DATAPART_FAILED);
    }

    historyBuilder.addDatapart(part);

    return {};
}

CommitResult<void> CommitBuilder::buildModifiedDataPart(JobSystem& jobsystem,
                                                        const GraphView view,
                                                        DataPartBuilder* builder,
                                                        CommitHistoryBuilder& historyBuilder) {
    // The builder knows what ID space it is building in: provide the first node/edge IDs
    // from the builder.
    WeakArc<DataPart> part =
        _controller->createDataPart(builder->firstNodeID(), builder->firstEdgeID());

    if (!part->load(view, jobsystem, *builder)) {
        return CommitError::result(CommitErrorType::BUILD_DATAPART_FAILED);
    }

    // Replace the unmodified datapart (copied from previous commit in historyBuilder),
    // with the modified version
    historyBuilder.replaceDataPartAtIndex(part, builder->getPartIndex());

    // Do not increment datapart count, as this is a replacement of an existing datapart,
    // and not an additional datapart

    return {};
}

CommitResult<void> CommitBuilder::buildAllPending(JobSystem& jobsystem) {
    Profile profile {"CommitBuilder::buildAllPending"};

    std::scoped_lock lock {_mutex};

    GraphView view {*_commitData};
    size_t numExistingDataparts = view.dataparts().size();

    CommitHistoryBuilder historyBuilder {_commitData->_history};
    for (const auto& builder : _builders) {
        auto res =
            builder->getPartIndex() < numExistingDataparts
                ? buildModifiedDataPart(jobsystem, view, builder.get(), historyBuilder)
                : buildNewDataPart(jobsystem, view, builder.get(), historyBuilder);

        if (!res) {
            return res.get_unexpected();
        }
    }

    _datapartCount += _builders.size();
    historyBuilder.setCommitDatapartCount(_datapartCount);

    _builders.clear();

    return {};
}

CommitResult<std::unique_ptr<Commit>> CommitBuilder::build(JobSystem& jobsystem) {
    if (auto res = buildAllPending(jobsystem); !res) {
        return res.get_unexpected();
    }

    return std::move(_commit);
}

void CommitBuilder::applyDeletions() {
    CommitWriteBuffer& wb = writeBuffer();
    auto dataparts = _commitData->allDataparts();

    // Performs various set up tasks to ensure deletions are performed correctly
    wb.prepare(this);

    // Both the below should be sorted and contain no duplicates
    auto& delNodes = wb.deletedNodes();
    auto& delEdges = wb.deletedEdges();

    if (delNodes.empty() && delEdges.empty()) {
        return;
    }

    for (const auto& [idx, part] : rv::enumerate(dataparts)) {
        std::span thisDPDeletedNodes = wb.deletedNodesFromDataPart(idx);
        std::span thisDPDeletedEdges = wb.deletedEdgesFromDataPart(idx);

        // Nothing in this datapart to delete
        if (thisDPDeletedNodes.empty() && thisDPDeletedEdges.empty()) {
            continue;
        }

        auto& newDataPartBuilder = newBuilder(idx);
        auto modifier = DataPartModifier(part, newDataPartBuilder, thisDPDeletedNodes,
                                         thisDPDeletedEdges);
        modifier.applyModifications();
    }
}

void CommitBuilder::flushWriteBuffer([[maybe_unused]] JobSystem& jobsystem) {
    CommitWriteBuffer& wb = writeBuffer();

    // If there is nothing to flush, return early without creating a new builder
    if (wb.empty()) {
        return;
    }

    // Adds DataPartBuilders to @ref _builders to be built by @ref
    // CommitBuilder::buildAllPending
    applyDeletions();

    if (wb.pendingNodes().empty() && wb.pendingEdges().empty()) {
        return;
    }

    // We create one more datapart which contains our CREATE commands when flushing
    // the buffer
    DataPartBuilder& dpBuilder = newBuilder();
    wb.buildFromBuffer(dpBuilder);
}

CommitBuilder::CommitBuilder(VersionController& controller, Change* change,
                             const GraphView& view)
    : _controller(&controller),
      _change(change),
      _view(view) {
}

void CommitBuilder::initialize() {
    Profile profile {"CommitBuilder::initialize"};

    auto reader = _view.read();

    // The first ID of this commit will be one more than the max ID in the graph
    _firstNodeID = reader.getNodeCount();
    _firstEdgeID = reader.getEdgeCount();

    // Update the 'next' ID values for use when creating dataparts
    // NOTE: In the case of @ref Change::submit, these values will be resynced to be
    // the next ID in the graph on main at the time of submission.
    _nextNodeID = reader.getNodeCount();
    _nextEdgeID = reader.getEdgeCount();

    const CommitView prevCommit = reader.commits().back();

    // Create new commit data
    _commitData = _controller->createCommitData(CommitHash::create());
    _commit = Commit::createNextCommit(_controller, _commitData, prevCommit);

    // Create metadata builder
    _metadataBuilder = MetadataBuilder::create(_view.metadata(), &_commitData->_metadata);

    // Create the write buffer for this commit
    _writeBuffer = std::make_unique<CommitWriteBuffer>();
}
