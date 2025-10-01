#include "CommitBuilder.h"

#include <algorithm>
#include <bits/ranges_algo.h>
#include <range/v3/view/enumerate.hpp>

#include "EnumerateFrom.h"
#include "EdgeContainer.h"
#include "reader/GraphReader.h"
#include "Profiler.h"
#include "Graph.h"
#include "versioning/Commit.h"
#include "versioning/CommitWriteBuffer.h"
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

CommitResult<void> CommitBuilder::buildAllPending(JobSystem& jobsystem) {
    Profile profile {"CommitBuilder::buildAllPending"};

    std::scoped_lock lock {_mutex};

    GraphView view {*_commitData};

    CommitHistoryBuilder historyBuilder {_commitData->_history};
    for (const auto& builder : _builders) {
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

void CommitBuilder::detectHangingEdges() {
    DataPartSpan parts = _commitData->commitDataparts();
    CommitWriteBuffer& wb = writeBuffer();

    auto& delNodes = wb.deletedNodes();

    if (!std::ranges::is_sorted(delNodes)) {
        std::ranges::sort(delNodes);
    }

    for (const WeakArc<DataPart>& part : parts) {
        const EdgeContainer& edgeContainer = part->edges();
        const EdgeID edgeContainerFirstID = edgeContainer.getFirstEdgeID();

        // Consider the range of NodeIDs that exist in this datapart
        NodeID smallestNodeID = part->getFirstNodeID();
        NodeID largestNodeID = part->getFirstNodeID() + part->getNodeCount() - 1;
        // The nodes to be deleted from this datapart are in the interval [nlb, nub)
        auto nlb = std::ranges::lower_bound(delNodes, smallestNodeID);
        auto nub = std::ranges::upper_bound(delNodes, largestNodeID);

        // Subspan to reduce the search space
        std::span thisDPNodesToDelete(nlb, nub);

        if (thisDPNodesToDelete.empty()) {
            continue;
        }

        for (const auto& [edgeID, edgeRecord] :
             EnumerateFrom(edgeContainerFirstID.getValue(), edgeContainer.getOuts())) {
            NodeID src = edgeRecord._nodeID;
            NodeID tgt = edgeRecord._otherID;

            // If the source or target are deleted, we must also delete this edge
            if (std::ranges::binary_search(thisDPNodesToDelete, src)
                || std::ranges::binary_search(thisDPNodesToDelete, tgt)) {
                wb.addDeletedEdges({edgeID});
            }
        }
    }
}

void CommitBuilder::applyDeletions() {
    CommitWriteBuffer& wb = writeBuffer();
    auto dataparts = _commitData->commitDataparts();

    // Add to @ref _deletedEdges any edges which are incident to a node which will
    // be deleted
    detectHangingEdges();

    auto& delNodes = wb.deletedNodes();
    auto& delEdges = wb.deletedEdges();

    // Sort our vectors for O(logn) lookup whilst being more cache-friendly than a
    // std::set
    if (!std::ranges::is_sorted(delNodes)) {
        std::ranges::sort(delNodes);
    }
    // std::ranges::unique(delNodes);
    if (!std::ranges::is_sorted(delEdges)) {
        std::ranges::sort(delEdges);
    }
    // std::ranges::unique(delEdges);

    for (const auto& [idx, part] : rv::enumerate(dataparts)) {
        // Consider the range of NodeIDs that exist in this datapart
        NodeID smallestNodeID = part->getFirstNodeID();
        NodeID largestNodeID = part->getFirstNodeID() + part->getNodeCount() - 1;

        // The nodes to be deleted from this datapart are in the interval [nlb, nub)
        auto nlb = std::ranges::lower_bound(delNodes, smallestNodeID);
        auto nub = std::ranges::upper_bound(delNodes, largestNodeID);

        // Consider the range of EdgeIDs that exist in this datapart
        EdgeID smallestEdgeID = part->getFirstEdgeID();
        EdgeID largestEdgeID = part->getFirstEdgeID() + part->getEdgeCount() - 1;

        // The edges to be deleted from this datapart are in the interval [elb, eub)
        auto elb = std::ranges::lower_bound(delEdges, smallestEdgeID);
        auto eub = std::ranges::upper_bound(delEdges, largestEdgeID);

        // Subspans to reduce the search space of what we need delete from this datapart
        std::span thisDPDeletedNodes(nlb, nub);
        std::span thisDPDeletedEdges(elb, eub);

        if (thisDPDeletedNodes.empty() && thisDPDeletedEdges.empty()) {
            continue;
        }
    }
}

void CommitBuilder::flushWriteBuffer(JobSystem& jobsystem) {
    CommitWriteBuffer& wb = writeBuffer();

    // If there is nothing to flush, return early without creating a new builder
    if (wb.empty()) {
        return;
    }

    // Peform deletions
    applyDeletions();

    // We create a single datapart when flushing the buffer, to ensure it is synced with
    // the metadata provided when rebasing main
    DataPartBuilder& dpBuilder = newBuilder();
    wb.buildPending(dpBuilder);
}

CommitBuilder::CommitBuilder(VersionController& controller, Change* change, const GraphView& view)
    : _controller(&controller),
    _change(change),
    _view(view)
{
}

void CommitBuilder::initialize() {
    Profile profile {"CommitBuilder::initialize"};

    auto reader = _view.read();

    // The first ID of this commit will be one more than the max ID in the graph
    _firstNodeID = reader.getNodeCount();
    _firstEdgeID = reader.getEdgeCount();

    // Update the 'next' ID values for use when creating dataparts
    // NOTE: In the case of @ref Change::submit, these values will be resynced to be the
    // next ID in the graph on main at the time of submission.
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
