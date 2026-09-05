#include "CommitBuilder.h"

#include <range/v3/view/enumerate.hpp>

#include "ID.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"
#include "reader/GraphReader.h"
#include "Graph.h"
#include "spdlog/spdlog.h"
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
                                                      const Commit* prevCommit) {
    const GraphView view = GraphView(&prevCommit->data());
    auto* ptr = new CommitBuilder(controller, change, view);

    ptr->initialize(prevCommit);
    return std::unique_ptr<CommitBuilder> {ptr};
}

std::unique_ptr<CommitBuilder> CommitBuilder::prepareMerge(VersionController& controller,
                                                           Change* change,
                                                           const Commit* prevCommit) {
    const GraphView view = GraphView {&prevCommit->data()};
    auto* ptr = new CommitBuilder {controller, change, view};

    ptr->initializeMerge(prevCommit);
    return std::unique_ptr<CommitBuilder> {ptr};
}

CommitHash CommitBuilder::hash() const {
    return _commitData->hash();
}

GraphView CommitBuilder::viewGraph() const {
    return GraphView {_commitData.get()};
}

GraphReader CommitBuilder::readGraph() const {
    return viewGraph().read();
}

void CommitBuilder::appendBuilder(std::unique_ptr<DataPartBuilder> builder) {
    std::unique_lock<std::mutex> lock(_mutex);
    _builders.emplace_back(std::move(builder));
}

DataPartBuilder& CommitBuilder::newBuilder() {
    std::unique_lock<std::mutex> lock {_mutex};

    const GraphView view = GraphView(_commitData.get());
    const size_t partIndex = view.dataparts().size() + _builders.size();
    auto& builder = _builders.emplace_back(DataPartBuilder::prepare(*_metadataBuilder,
                                                                    view.read().getTotalNodesAllocated(),
                                                                    view.read().getTotalEdgesAllocated(),
                                                                    partIndex));

    return *builder;
}

template <SupportedType P>
WeakArc<Index> CommitBuilder::newNodePropertyIndex(std::string_view indexName,
                                                   PropertyTypeID ptID) {
    return _controller->createNodePropertyIndex<P>(indexName, ptID);
}

template <SupportedType P>
WeakArc<Index> CommitBuilder::newEdgePropertyIndex(std::string_view indexName,
                                                   PropertyTypeID ptID) {
    return _controller->createEdgePropertyIndex<P>(indexName, ptID);
}

CommitResult<void> CommitBuilder::buildAllPending(JobSystem& jobsystem) {
    Profile profile("CommitBuilder::buildAllPending");

    std::unique_lock<std::mutex> lock(_mutex);

    const GraphView view {_commitData.get()};

    CommitHistoryBuilder historyBuilder(_commitData->_history);
    for (const auto& builder : _builders) {
        auto part = _controller->createDataPart(builder->_firstNodeID, builder->_firstEdgeID);

        if (auto res = part->load(view, jobsystem, *builder); !res) {
            return res;
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

    const size_t numCommitDataParts = _commit->history().commitDataparts().size();
    size_t numNodesAdded = 0;
    size_t numEdgesAdded = 0;

    for (size_t i = 0; i < numCommitDataParts; ++i) {
        const DataPart* part = _commit->data().commitDataparts()[i].get();
        numNodesAdded += part->getNodeContainerSize();
        numEdgesAdded += part->getEdgeContainerSize();
    }

    _commit->_numDataParts = numCommitDataParts;
    _commit->_numNodes = numNodesAdded;
    _commit->_numEdges = numEdgesAdded;

    return std::move(_commit);
}

void CommitBuilder::flushWriteBuffer([[maybe_unused]] JobSystem& jobsystem) {
    CommitWriteBuffer& wb = writeBuffer();
    Tombstones& tombstones = _commitData->_tombstones;

    bioassert(_commit->history().journal().empty(), "Journal must be empty on flush.");

    // At this point, any conflict checking should have already been done in @ref
    // Change::rebase, so all writes are valid.

    if (wb.containsDeletes()) {
        wb.applyDeletions(tombstones);
    }

    if (wb.containsCreates()) {
        // We create a single datapart when flushing the buffer,
        // to ensure it is synced with the metadata provided when rebasing main
        DataPartBuilder& dpBuilder = newBuilder();
        wb.buildPending(dpBuilder, tombstones);
    }

    if (wb.containsUpdates()) {
        // Create a new datapart: ensures updates apply to newly created entities
        DataPartBuilder& dpBuilder = newBuilder();

        // Update the firstXID fields so that this builder knows about the new CREATES
        const size_t numCreatedNodes = wb.numPendingNodes();
        const size_t numCreatedEdges = wb.numPendingEdges();

        dpBuilder._firstNodeID += numCreatedNodes;
        dpBuilder._firstEdgeID += numCreatedEdges;

        wb.applyUpdates(dpBuilder);
    }

    {
        CommitHistory& history = _commitData->history();
        CommitHistoryBuilder historyBuilder {history};

        // Add the indexes that this commit creates
        for (const WeakArc<Index>& index : wb.pendingIndexes()) {
            historyBuilder.addValidIndex(index);
        }

        const CommitJournal* ourJournal = history._journal.get();
        auto& ourIndexes = history._validIndexes;
        const auto& nodePropUpdates = ourJournal->nodePropertyWriteSet();
        const auto& edgePropUpdates = ourJournal->edgePropertyWriteSet();

        const CommitWriteBuffer::DroppedIndexes& droppedIndexes = wb.droppedIndexes();

        const auto invalidated = [&](const WeakArc<Index>& index) -> bool {
            const bool isNode = index->isNodeIndex();
            const PropertyTypeID indexedProp = index->property();
            const WriteSet<PropertyTypeID>& propUpdates =
                isNode ? nodePropUpdates : edgePropUpdates;
            const bool propertyInvalidated = propUpdates.contains(indexedProp);

            const auto findIt = std::ranges::find(droppedIndexes, index);
            const auto dropped = findIt != end(droppedIndexes);

            return propertyInvalidated | dropped;
        };

        // By default we carry over the previous commit's indexes. Remove any that have
        // become invalidated due to the writes (SETs, CREATEs) of this commit, or dropped
        // by a DROP INDEX command
        std::erase_if(ourIndexes, invalidated);
    }

    wb.setFlushed();
}

CommitBuilder::CommitBuilder(VersionController& controller, Change* change, const GraphView& view)
    : _controller(&controller),
    _change(change),
    _view(view)
{
}

void CommitBuilder::initialize(const Commit* prevCommit) {
    Profile profile ("CommitBuilder::initialize");

    const auto reader = _view.read();

    // The first ID of this commit will be one more than the max ID in the graph
    _firstNodeID = reader.getTotalNodesAllocated();
    _firstEdgeID = reader.getTotalEdgesAllocated();

    // Update the 'next' ID values for use when creating dataparts
    // NOTE: In the case of @ref Change::submit, these values will be resynced to be the
    // next ID in the graph on main at the time of submission.
    _nextNodeID = reader.getTotalNodesAllocated();
    _nextEdgeID = reader.getTotalEdgesAllocated();

    // Create new commit data
    _commitData = _controller->createCommitData(CommitHash::create());
    _commit = Commit::createNextCommit(_controller, _commitData, prevCommit);

    // Create metadata builder
    _metadataBuilder = MetadataBuilder::create(_view.metadata(), &_commitData->_metadata);

    CommitHistory& history = _commitData->_history;
    history._journal = CommitJournal::emptyJournal();
    bioassert(_commitData->_history._journal, "Invalid journal");

    // Create the write buffer for this commit
    _writeBuffer = std::make_unique<CommitWriteBuffer>(commitData().history().journal(), _view);
    // Copy tombstones from previous commit
    _commitData->_tombstones = _view.tombstones();
}

void CommitBuilder::initializeMerge(const Commit* prevCommit) {
    Profile profile ("CommitBuilder::initialize");

    _firstNodeID = 0;
    _firstEdgeID = 0;

    _nextNodeID = 0;
    _nextEdgeID = 0;

    // Create new commit data
    _commitData = _controller->createCommitData(CommitHash::create());
    _commit = Commit::createMergeCommit(_controller, _commitData, prevCommit);

    // Create metadata builder
    _metadataBuilder = MetadataBuilder::create(_view.metadata(), &_commitData->_metadata);

    _commitData->_history._journal = CommitJournal::emptyJournal();

    // Create the write buffer for this commit
    _writeBuffer = std::make_unique<CommitWriteBuffer>(commitData().history().journal(), _view);
}

namespace db {
template WeakArc<Index> CommitBuilder::newNodePropertyIndex<types::Int64>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> CommitBuilder::newNodePropertyIndex<types::UInt64>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> CommitBuilder::newNodePropertyIndex<types::Double>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> CommitBuilder::newNodePropertyIndex<types::String>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> CommitBuilder::newNodePropertyIndex<types::Bool>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> CommitBuilder::newNodePropertyIndex<types::Embedding>(std::string_view indexName, PropertyTypeID ptID);

template WeakArc<Index> CommitBuilder::newEdgePropertyIndex<types::Int64>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> CommitBuilder::newEdgePropertyIndex<types::UInt64>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> CommitBuilder::newEdgePropertyIndex<types::Double>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> CommitBuilder::newEdgePropertyIndex<types::String>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> CommitBuilder::newEdgePropertyIndex<types::Bool>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> CommitBuilder::newEdgePropertyIndex<types::Embedding>(std::string_view indexName, PropertyTypeID ptID);
}
