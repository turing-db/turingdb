#include "CommitBuilder.h"

#include "reader/GraphReader.h"
#include "Profiler.h"
#include "Graph.h"
#include "versioning/Commit.h"
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
    ptr->newBuilder();
    return std::unique_ptr<CommitBuilder> {ptr};
}

Transaction CommitBuilder::openTransaction() {
    return Transaction {_commit->_data};
}

CommitHash CommitBuilder::hash() const {
    return _commit->hash();
}

GraphView CommitBuilder::viewGraph() const {
    return GraphView {*_commit->_data};
}

GraphReader CommitBuilder::readGraph() const {
    return viewGraph().read();
}

DataPartBuilder& CommitBuilder::newBuilder() {
    std::scoped_lock lock {_mutex};
    GraphView view {*_commit->_data};
    const size_t partIndex = view.dataparts().size() + _builders.size();
    auto& builder = _builders.emplace_back(DataPartBuilder::prepare(*_metadata, view, partIndex));

    return *builder;
}

CommitResult<void> CommitBuilder::buildAllPending(JobSystem& jobsystem) {
    Profile profile {"CommitBuilder::buildAllPending"};

    std::scoped_lock lock {_mutex};

    GraphView view {*_commit->_data};

    for (const auto& builder : _builders) {
        auto part = _controller->createDataPart(_firstNodeID, _firstEdgeID);

        _firstNodeID += builder->nodeCount();
        _firstEdgeID += builder->edgeCount();

        if (!part->load(view, jobsystem, *builder)) {
            return CommitError::result(CommitErrorType::BUILD_DATAPART_FAILED);
        }
        _commit->_data->_history._allDataparts.emplace_back(part);
        _commit->_data->_history._commitDataparts.emplace_back(part);
    }

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

    _commit = std::make_unique<Commit>();
    _commit->_controller = _controller;
    _commit->_data = _controller->createCommitData(_commit->hash());
    _commit->_data->_hash = _commit->hash();

    _metadata = MetadataBuilder::create(_view.metadata(), &_commit->_data->_metadata);

    auto& history = _commit->history();

    const DataPartSpan previousDataparts = reader.dataparts();
    const std::span<const CommitView> previousCommits = reader.commits();

    history.pushPreviousDataparts(previousDataparts);
    history.pushPreviousCommits(previousCommits);
    history.pushCommit(CommitView {_commit.get()});
}
