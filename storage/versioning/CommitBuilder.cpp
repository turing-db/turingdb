#include "CommitBuilder.h"

#include <unordered_map>
#include <range/v3/view/enumerate.hpp>

#include "BioAssert.h"
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

CommitResult<std::unique_ptr<Commit>> CommitBuilder::flushWriteBuffer(JobSystem& jobsystem) {
    // Ensure this CommitBuilder is in a fresh state
    msgbioassert(
        isEmpty(),
        fmt::format("CommitBuilder must be empty (has {}) to flush write buffer.",
                    dpCount())
            .c_str());
    msgbioassert(pendingCount() == 0,
                 fmt::format("CommitBuilder must have one datapart builders (has {}) to "
                             "flush write buffer.",
                             pendingCount())
                     .c_str());

    // We create a single datapart when flushing the buffer, to ensure it is synced with
    // the metadata provided when rebasing main
    _builders.clear();
    DataPartBuilder& dpBuilder = newBuilder();
    CommitWriteBuffer& wb = writeBuffer();

    // TODO: Remove this by using a mapping of NodeID = offset + _firstNodeID
    std::unordered_map<CommitWriteBuffer::PendingNodeOffset, NodeID> tempIDMap;

    // Add pending nodes to be created to the current DataPartBuilder
    for (const auto& [offset, node] : wb.pendingNodes() | rv::enumerate) {
        NodeID nodeID = dpBuilder.addPendingNode(node);
        tempIDMap[offset] = nodeID;
    }

    // Add pending edges to be created to the current DataPartBuilder
    for (const auto& edge : wb.pendingEdges()) {
        dpBuilder.addPendingEdge(wb, edge, tempIDMap);
    }

    // Build this commit
    return build(jobsystem);
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
    // this->newBuilder();
}
