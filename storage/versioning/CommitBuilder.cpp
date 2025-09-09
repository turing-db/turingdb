#include "CommitBuilder.h"

#include "JobSystem.h"
#include "reader/GraphReader.h"
#include "Profiler.h"
#include "Graph.h"
#include "versioning/Commit.h"
#include "versioning/CommitHistory.h"
#include "versioning/CommitResult.h"
#include "versioning/VersionController.h"
#include "versioning/Transaction.h"
#include "versioning/CommitView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"
#include "metadata/GraphMetadata.h"

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
    return GraphView {*_commitData}; }

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

CommitResult<void> CommitBuilder::buildNewDataPart(DataPartBuilder* builder,
                                                   JobSystem& jobsystem,
                                                   const GraphView& view,
                                                   CommitHistoryBuilder& historyBuilder) {
    // Create Empty DataPart
    WeakArc<DataPart> part =
        _controller->createDataPart(_nextDataPartFirstNodeID, _nextDataPartFirstEdgeID);

    if (!part->load(view, jobsystem, *builder)) {
        return CommitError::result(CommitErrorType::BUILD_DATAPART_FAILED);
    }

    // Update these so the next builder gets the right _firstNodeID
    _nextDataPartFirstNodeID += builder->nodeCount();
    _nextDataPartFirstEdgeID += builder->edgeCount();

    historyBuilder.addDatapart(part);
    _datapartCount++;

    return {};
}

CommitResult<void> CommitBuilder::buildModifiedDataPart(DataPartBuilder* builder,
                                                        JobSystem& jobsystem,
                                                        const GraphView& view,
                                                        CommitHistoryBuilder& historyBuilder) {
    // The builder knows what ID space it is building in: provide the first node/edge IDs
    // from the builder. Do not use members _nextDataPartFirstNode/EdgeID, as these are
    // for completely new dataparts
    WeakArc<DataPart> part =
        _controller->createDataPart(builder->firstNodeID(), builder->firstEdgeID());

    if (!part->load(view,jobsystem, *builder)) {
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

    CommitHistoryBuilder historyBuilder {_commitData->_history};
    size_t numExistingDataparts = _commitData->_history.allDataparts().size();

    for (const auto& builder : _builders) {
        // Build a new datapart: result of CREATE query
        if (builder->_partIndex >= numExistingDataparts) {
            auto buildRes =
                buildNewDataPart(builder.get(), jobsystem, view, historyBuilder);

            if (!buildRes) {
                return buildRes.get_unexpected();
            }
        } else {
            auto buildRes =
                buildModifiedDataPart(builder.get(), jobsystem, view, historyBuilder);

            if (!buildRes) {
                return buildRes.get_unexpected();
            }
        }
    }

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

CommitBuilder::CommitBuilder(VersionController& controller, Change* change, const GraphView& view)
    : _controller(&controller),
    _change(change),
    _view(view)
{
}

void CommitBuilder::initialize() {
    Profile profile {"CommitBuilder::initialize"};

    auto reader = _view.read();
    _nextDataPartFirstNodeID = reader.getNodeCount();
    _nextDataPartFirstEdgeID = reader.getEdgeCount();

    const CommitView prevCommit = reader.commits().back();

    // Create new commit data
    _commitData = _controller->createCommitData(CommitHash::create());
    _commit = Commit::createNextCommit(_controller, _commitData, prevCommit);

    // Create metadata builder
    _metadataBuilder = MetadataBuilder::create(_view.metadata(), &_commitData->_metadata);

    // Create datapart builder
    this->newBuilder();
}
