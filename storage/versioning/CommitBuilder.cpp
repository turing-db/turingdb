#include "CommitBuilder.h"

#include <range/v3/view/enumerate.hpp>

#include "reader/GraphReader.h"
#include "Profiler.h"
#include "Graph.h"
#include "spdlog/spdlog.h"
#include "versioning/Commit.h"
#include "versioning/VersionController.h"
#include "versioning/Transaction.h"
#include "versioning/CommitView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

using namespace db;

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

void CommitBuilder::flushWriteBuffer([[maybe_unused]] JobSystem& jobsystem) {
    CommitWriteBuffer& wb = writeBuffer();

    if (wb.empty()) {
        wb.setFlushed();
        return;
    }

    std::ranges::sort(wb.deletedNodes());
    std::ranges::sort(wb.deletedEdges());

    // We create a single datapart when flushing the buffer, to ensure it is synced with
    // the metadata provided when rebasing main
    DataPartBuilder& dpBuilder = newBuilder();
    wb.buildPending(dpBuilder);
    wb.setFlushed();
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
