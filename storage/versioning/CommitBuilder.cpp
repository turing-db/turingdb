#include "CommitBuilder.h"

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
        auto part = _controller->createDataPart(_firstNodeID, _firstEdgeID);

        _firstNodeID += builder->nodeCount();
        _firstEdgeID += builder->edgeCount();

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

CommitBuilder::CommitBuilder(VersionController& controller, Change* change, const GraphView& view)
    : _controller(&controller),
    _change(change),
    _view(view)
{
}

void CommitBuilder::initialize() {
    Profile profile {"CommitBuilder::initialize"};

    auto reader = _view.read();
    _firstNodeID = reader.getNodeCount();
    _firstEdgeID = reader.getEdgeCount();

    const CommitView prevCommit = reader.commits().back();

    // Create new commit data
    _commitData = _controller->createCommitData(CommitHash::create());
    _commit = Commit::createNextCommit(_controller, _commitData, prevCommit);

    // Create metadata builder
    _metadataBuilder = MetadataBuilder::create(_view.metadata(), &_commitData->_metadata);

    // Create the write buffer for this commit
    _writeBuffer = std::make_unique<CommitWriteBuffer>();

    // Create datapart builder
    this->newBuilder();
}
