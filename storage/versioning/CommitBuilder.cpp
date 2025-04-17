#include "CommitBuilder.h"

#include "reader/GraphReader.h"
#include "Graph.h"
#include "versioning/Commit.h"
#include "versioning/VersionController.h"
#include "versioning/CommitView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"
#include "metadata/GraphMetadata.h"

using namespace db;

CommitBuilder::CommitBuilder() = default;

CommitBuilder::~CommitBuilder() = default;

std::unique_ptr<CommitBuilder> CommitBuilder::prepare(Graph& graph, const GraphView& view) {
    auto* ptr = new CommitBuilder {graph, view};
    ptr->initialize();
    return std::unique_ptr<CommitBuilder> {ptr};
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
    auto& builder = _builders.emplace_back(DataPartBuilder::prepare(*_metadata, *_graph, view, partIndex));

    return *builder;
}

void CommitBuilder::buildAllPending(JobSystem& jobsystem) {
    std::scoped_lock lock {_mutex};
    size_t nodeCount = 0;
    size_t edgeCount = 0;

    for (const auto& builder : _builders) {
        nodeCount += builder->nodeCount();
        edgeCount += builder->edgeCount();
    }

    auto [firstNodeID, firstEdgeID] = _graph->allocIDRange(nodeCount, edgeCount);
    GraphView view {*_commit->_data};

    for (const auto& builder : _builders) {
        auto part = _graph->_versionController->createDataPart(firstNodeID, firstEdgeID);

        firstNodeID += builder->nodeCount();
        firstEdgeID += builder->edgeCount();

        part->load(view, jobsystem, *builder);
        _commit->_data->_history._allDataparts.emplace_back(part);
        _commit->_data->_history._commitDataparts.emplace_back(part);
    }

    _builders.clear();
}

std::unique_ptr<Commit> CommitBuilder::build(JobSystem& jobsystem) {
    buildAllPending(jobsystem);

    return std::move(_commit);
}

CommitBuilder::CommitBuilder(Graph& graph, const GraphView& view)
    : _graph(&graph),
    _view(view)
{
}

void CommitBuilder::initialize() {
    auto reader = _view.read();
    _firstNodeID = reader.getNodeCount();
    _firstEdgeID = reader.getNodeCount();

    _commit = std::make_unique<Commit>();
    _commit->_graph = _graph;
    _commit->_data = _graph->_versionController->createCommitData(_commit->hash());
    _commit->_data->_hash = _commit->hash();

    _metadata = MetadataBuilder::create(_view.metadata(), &_commit->_data->_metadata);

    auto& history = _commit->history();

    const DataPartSpan previousDataparts = reader.dataparts();
    const std::span<const CommitView> previousCommits = reader.commits();

    history.pushPreviousDataparts(previousDataparts);
    history.pushPreviousCommits(previousCommits);
    history.pushCommit(CommitView {_commit.get()});
}

CommitResult<void> CommitBuilder::commit(JobSystem& jobsystem) {
    buildAllPending(jobsystem);

    return _graph->commit(*this, jobsystem);
}

CommitResult<void> CommitBuilder::rebaseAndCommit(JobSystem& jobsystem) {
    buildAllPending(jobsystem);

    return _graph->rebaseAndCommit(*this, jobsystem);
}
