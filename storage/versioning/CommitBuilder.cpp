#include "CommitBuilder.h"

#include <range/v3/view/enumerate.hpp>

#include "reader/GraphReader.h"
#include "Graph.h"
#include "versioning/Commit.h"
#include "versioning/CommitHistoryBuilder.h"
#include "versioning/Tombstones.h"
#include "versioning/VersionController.h"
#include "versioning/Transaction.h"
#include "versioning/CommitView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"
#include "CommitJournal.h"

#include "Profiler.h"
#include "BioAssert.h"

using namespace db;

CommitBuilder::CommitBuilder()
{
}

CommitBuilder::~CommitBuilder() {
}

std::unique_ptr<CommitBuilder> CommitBuilder::prepare(VersionController& controller,
                                                      Change* change,
                                                      const GraphView& view) {
    auto* ptr = new CommitBuilder {controller, change, view};
    ptr->initialize();
    return std::unique_ptr<CommitBuilder> {ptr};
}

std::unique_ptr<CommitBuilder> CommitBuilder::prepareMerge(VersionController& controller,
                                                           Change* change,
                                                           const GraphView& view) {
    auto* ptr = new CommitBuilder {controller, change, view};
    ptr->initializeMerge();
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

void CommitBuilder::appendBuilder(std::unique_ptr<DataPartBuilder> builder) {
    std::unique_lock<std::mutex> lock {_mutex};
    _builders.emplace_back(std::move(builder));
}

DataPartBuilder& CommitBuilder::newBuilder() {
    std::unique_lock<std::mutex> lock {_mutex};
    GraphView view {*_commitData};
    const size_t partIndex = view.dataparts().size() + _builders.size();
    auto& builder = _builders.emplace_back(DataPartBuilder::prepare(*_metadataBuilder,
                                                                    view.read().getTotalNodesAllocated(),
                                                                    view.read().getTotalEdgesAllocated(),
                                                                    partIndex));

    return *builder;
}

CommitResult<void> CommitBuilder::buildAllPending(JobSystem& jobsystem) {
    Profile profile {"CommitBuilder::buildAllPending"};

    std::unique_lock<std::mutex> lock {_mutex};

    const GraphView view {*_commitData};

    CommitHistoryBuilder historyBuilder {_commitData->_history};
    for (const auto& builder : _builders) {
        auto part = _controller->createDataPart(builder->_firstNodeID, builder->_firstEdgeID);

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

    _commit->history().journal().finalise();

    return std::move(_commit);
}

void CommitBuilder::flushWriteBuffer([[maybe_unused]] JobSystem& jobsystem) {
    CommitWriteBuffer& wb = writeBuffer();
    Tombstones& tombstones = _commitData->_tombstones;

    bioassert(_commit->history().journal().empty(), "Journal must be empty on flush.");

    if (wb.containsDeletes()) {
        // At this point, conflict checking should have already been done in @ref
        // Change::rebase, so all deletes are valid
        wb.applyDeletions(tombstones);
    }

    if (wb.containsCreates()) {
        // We create a single datapart when flushing the buffer,
        // to ensure it is synced with the metadata provided when rebasing main
        DataPartBuilder& dpBuilder = newBuilder();
        wb.buildPending(dpBuilder);
    }

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

    const auto reader = _view.read();

    // The first ID of this commit will be one more than the max ID in the graph
    _firstNodeID = reader.getTotalNodesAllocated();
    _firstEdgeID = reader.getTotalEdgesAllocated();

    // Update the 'next' ID values for use when creating dataparts
    // NOTE: In the case of @ref Change::submit, these values will be resynced to be the
    // next ID in the graph on main at the time of submission.
    _nextNodeID = reader.getTotalNodesAllocated();
    _nextEdgeID = reader.getTotalEdgesAllocated();

    const CommitView prevCommit = reader.commits().back();

    // Create new commit data
    _commitData = _controller->createCommitData(CommitHash::create());
    _commit = Commit::createNextCommit(_controller, _commitData, prevCommit);

    // Create metadata builder
    _metadataBuilder = MetadataBuilder::create(_view.metadata(), &_commitData->_metadata);

    _commitData->_history._journal = CommitJournal::emptyJournal();
    bioassert(_commitData->_history._journal, "Invalid journal");

    // Create the write buffer for this commit
    _writeBuffer = std::make_unique<CommitWriteBuffer>(commitData().history().journal());
    // Copy tombstones from previous commit
    _commitData->_tombstones = prevCommit.tombstones();
}

void CommitBuilder::initializeMerge() {
    Profile profile {"CommitBuilder::initialize"};

    const auto reader = _view.read();

    _firstNodeID = 0;
    _firstEdgeID = 0;

    _nextNodeID = 0;
    _nextEdgeID = 0;

    const CommitView prevCommit = reader.commits().back();

    // Create new commit data
    _commitData = _controller->createCommitData(CommitHash::create());
    _commit = Commit::createMergeCommit(_controller, _commitData, prevCommit);

    // Create metadata builder
    _metadataBuilder = MetadataBuilder::create(_view.metadata(), &_commitData->_metadata);

    _commitData->_history._journal = CommitJournal::emptyJournal();

    // Create the write buffer for this commit
    _writeBuffer = std::make_unique<CommitWriteBuffer>(commitData().history().journal());
}
