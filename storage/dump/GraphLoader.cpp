#include "GraphLoader.h"

#include "BinaryDiskDecoder.h"
#include "DataPart.h"
#include "EdgeContainer.h"
#include "Graph.h"
#include "NodeContainer.h"
#include "Profiler.h"
#include "indexers/EdgeIndexer.h"
#include "indexers/StringPropertyIndexer.h"
#include "indexes/StringIndex.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "properties/PropertyContainer.h"
#include "properties/PropertyManager.h"
#include "versioning/Commit.h"
#include "versioning/CommitData.h"
#include "versioning/CommitHistory.h"
#include "versioning/CommitHistoryBuilder.h"
#include "versioning/CommitJournal.h"
#include "versioning/Tombstones.h"
#include "versioning/VersionController.h"
#include "versioning/WriteSet.h"

#include "BioAssert.h"

namespace db {

GraphLoader::GraphLoader(Graph* graph)
    : _graph(graph)
{
    // Lazy-load entry (CommitLoader::loadData) reuses an existing VersionController.
    // Wire up _partMap upfront when one exists; the full-graph entry overwrites it
    // in initVersionController.
    if (_graph->_versionController) {
        _partMap = &_graph->_versionController->getPartMap();
    }
}

GraphLoader::~GraphLoader() = default;

DumpResult<void> GraphLoader::load(Graph* graph, const fs::Path& graphDir) {
    Profile profile("GraphLoader::load");

    GraphLoader builder(graph);
    BinaryDiskDecoder decoder(&builder);
    return decoder.decodeGraph(graphDir);
}

// ─────────────────────────────────────────────────────────────────
// Graph
// ─────────────────────────────────────────────────────────────────

void GraphLoader::setGraphInfo(uint64_t graphID, std::string_view name) {
    _graph->_graphID = GraphID(graphID);
    _graph->_graphName = std::string {name};
}

void GraphLoader::initVersionController() {
    _graph->_versionController = std::make_unique<VersionController>(_graph);
    _partMap = &_graph->_versionController->getPartMap();
}

void GraphLoader::reserveCommits(uint64_t /*numCommits*/) {
    // Best-effort hint; VersionController doesn't expose reserve.
}

void GraphLoader::setNumCommits(uint64_t /*n*/) {
    // Informational; VersionController computes its own count from _commits.size().
}

void GraphLoader::addCommitSkeleton(uint64_t commitHash,
                                    uint64_t numNodes,
                                    uint64_t numEdges,
                                    uint64_t numCommitDataParts) {
    auto commit = std::make_unique<Commit>(_graph->_versionController.get(),
                                           CommitHash {commitHash},
                                           _prevCommit);
    commit->_numNodes = numNodes;
    commit->_numEdges = numEdges;
    commit->_numDataParts = numCommitDataParts;

    _prevCommit = commit.get();
    _graph->_versionController->addCommit(std::move(commit));
}

uint64_t GraphLoader::getHeadCommitHash() const {
    Commit* head = _graph->_versionController->_head.load();
    bioassert(head, "No head commit");
    return head->hash().get();
}

Commit* GraphLoader::findCommit(uint64_t commitHash) const {
    const auto offset = _graph->_versionController->getCommitIndex(CommitHash {commitHash});
    bioassert(offset.has_value(), "Commit not registered");
    return _graph->_versionController->_commits[offset.value()].get();
}

// ─────────────────────────────────────────────────────────────────
// Per-commit data
// ─────────────────────────────────────────────────────────────────

void GraphLoader::beginCommitData(uint64_t commitHash) {
    _commit = findCommit(commitHash);
    _commit->setCommitData(_graph->_versionController->createCommitData(_commit->hash()));
    _commitData = const_cast<CommitData*>(&_commit->data());

    _metadata = &_commitData->_metadata;
    _journal = _commitData->_history._journal.get();
    _tombstones = &_commitData->_tombstones;
    _historyBuilder = std::make_unique<CommitHistoryBuilder>(_commitData->_history);
}

void GraphLoader::endCommitData() {
    _commit = nullptr;
    _commitData = nullptr;
    _metadata = nullptr;
    _journal = nullptr;
    _tombstones = nullptr;
    _historyBuilder.reset();
}

uint64_t GraphLoader::addLabel(std::string_view name) {
    return _metadata->_labelMap.getOrCreate(std::string {name}).getValue();
}

uint64_t GraphLoader::addEdgeType(std::string_view name) {
    return _metadata->_edgeTypeMap.getOrCreate(std::string {name}).getValue();
}

uint64_t GraphLoader::addPropertyType(std::string_view name, ValueType vt) {
    const auto pt = _metadata->_propTypeMap.getOrCreate(std::string {name}, vt);
    return pt._id.getValue();
}

uint64_t GraphLoader::addLabelSet(const uint64_t* bits, size_t count) {
    bioassert(count == LabelSet::IntegerCount, "labelset bit count mismatch");
    LabelSet ls;
    auto* data = ls.data();
    for (size_t i = 0; i < count; i++) {
        data[i] = bits[i];
    }
    const auto handle = _metadata->_labelsetMap.getOrCreate(ls);
    return handle.getID().getValue();
}

void GraphLoader::addNodeWriteSet(const uint64_t* nodeIDs, size_t count) {
    auto& set = _journal->rawNodeWriteSet();
    set.reserve(set.size() + count);
    for (size_t i = 0; i < count; i++) {
        set.emplace_back(nodeIDs[i]);
    }
}

void GraphLoader::addEdgeWriteSet(const uint64_t* edgeIDs, size_t count) {
    auto& set = _journal->rawEdgeWriteSet();
    set.reserve(set.size() + count);
    for (size_t i = 0; i < count; i++) {
        set.emplace_back(edgeIDs[i]);
    }
}

void GraphLoader::addNodeTombstones(const uint64_t* nodeIDs, size_t count) {
    auto& ts = _tombstones->nodeTombstones();
    ts.reserve(count);
    for (size_t i = 0; i < count; i++) {
        ts.insert(NodeID {nodeIDs[i]});
    }
}

void GraphLoader::addEdgeTombstones(const uint64_t* edgeIDs, size_t count) {
    auto& ts = _tombstones->edgeTombstones();
    ts.reserve(count);
    for (size_t i = 0; i < count; i++) {
        ts.insert(EdgeID {edgeIDs[i]});
    }
}

// ─────────────────────────────────────────────────────────────────
// DataParts
// ─────────────────────────────────────────────────────────────────

bool GraphLoader::isDataPartLoaded(uint64_t partID) const {
    return _partMap->find(DataPartID {partID}) != _partMap->end();
}

void GraphLoader::attachExistingDataPart(uint64_t partID) {
    const auto it = _partMap->find(DataPartID {partID});
    bioassert(it != _partMap->end(), "Datapart not loaded yet");
    _historyBuilder->addDatapart(it->second);
}

void GraphLoader::beginDataPart(uint64_t partID,
                                uint64_t firstNodeID,
                                uint64_t firstEdgeID) {
    _currentPart = _graph->_versionController->createDataPart(NodeID {firstNodeID},
                                                              EdgeID {firstEdgeID},
                                                              DataPartID {partID});
    _partRaw = _currentPart.get();
    _partRaw->_firstNodeID = NodeID {firstNodeID};
    _partRaw->_firstEdgeID = EdgeID {firstEdgeID};
    _partRaw->_nodeProperties = std::make_unique<PropertyManager>();
    _partRaw->_edgeProperties = std::make_unique<PropertyManager>();
    _partRaw->_nodeStrPropIdx = std::make_unique<StringPropertyIndexer>();
    _partRaw->_edgeStrPropIdx = std::make_unique<StringPropertyIndexer>();
}

void GraphLoader::endDataPart() {
    if (_partRaw) {
        _partMap->emplace(_partRaw->getID(), _currentPart);
    }
    _partRaw = nullptr;
    _currentPart = WeakArc<DataPart> {};
}

void GraphLoader::attachDataPartToCommit(uint64_t partID) {
    const auto it = _partMap->find(DataPartID {partID});
    bioassert(it != _partMap->end(), "Datapart not loaded yet");
    _historyBuilder->addDatapart(it->second);
}

void GraphLoader::setCommitDataPartCount(uint64_t count) {
    _historyBuilder->setCommitDatapartCount(count);
}

void GraphLoader::finalizeDataPart() {
    _partRaw->_initialized = true;
}

// ─────────────────────────────────────────────────────────────────
// NodeContainer
// ─────────────────────────────────────────────────────────────────

void GraphLoader::beginNodeContainer(uint64_t firstNodeID, uint64_t nodeCount) {
    auto* nc = new NodeContainer(NodeID {firstNodeID}, nodeCount);
    nc->_nodes.resize(nodeCount);
    _partRaw->_nodes = std::unique_ptr<NodeContainer> {nc};
    _nodeRecordOffset = 0;
}

void GraphLoader::addNodeRange(uint64_t labelsetID, uint64_t firstNodeID, uint64_t count) {
    const auto handle = _metadata->_labelsetMap.getValue(
        LabelSetID {static_cast<LabelSetID::Type>(labelsetID)});
    bioassert(handle.has_value(), "labelset id not found");
    auto& r = _partRaw->_nodes->_ranges[handle.value()];
    r._first = NodeID {firstNodeID};
    r._count = count;
}

void GraphLoader::appendNodeRecords(const uint64_t* labelsetIDs, size_t count) {
    auto& records = _partRaw->_nodes->_nodes;
    for (size_t i = 0; i < count; i++) {
        const auto handle = _metadata->_labelsetMap.getValue(
            LabelSetID {static_cast<LabelSetID::Type>(labelsetIDs[i])});
        bioassert(handle.has_value(), "labelset id not found in record");
        records[_nodeRecordOffset + i]._labelset = handle.value();
    }
    _nodeRecordOffset += count;
}

void GraphLoader::endNodeContainer() {
    // No-op
}

void GraphLoader::initEmptyNodeContainer() {
    _partRaw->_nodes = std::unique_ptr<NodeContainer> {
        new NodeContainer(_partRaw->_firstNodeID, 0)};
}

// ─────────────────────────────────────────────────────────────────
// EdgeContainer
// ─────────────────────────────────────────────────────────────────

namespace {
    // Per-loader-instance scratch — lives in the loader, not in TLS, but
    // keeping a couple of helper buffers as members would clutter the header.
    // We use small lambdas + temporaries below instead.
}

void GraphLoader::beginEdgeContainer(uint64_t firstNodeID,
                                     uint64_t firstEdgeID,
                                     uint64_t edgeCount) {
    std::vector<EdgeRecord> outs(edgeCount);
    std::vector<EdgeRecord> ins(edgeCount);
    auto* ec = new EdgeContainer(NodeID {firstNodeID}, EdgeID {firstEdgeID},
                                 std::move(outs), std::move(ins));
    _partRaw->_edges = std::unique_ptr<EdgeContainer> {ec};
    _outEdgeOffset = 0;
    _inEdgeOffset = 0;
}

void GraphLoader::appendEdgeRecords(EdgeDirection dir,
                                    const EdgeRecordRaw* records,
                                    size_t count) {
    auto& target = (dir == EdgeDirection::Out)
                       ? _partRaw->_edges->_outEdges
                       : _partRaw->_edges->_inEdges;
    size_t& offset = (dir == EdgeDirection::Out) ? _outEdgeOffset : _inEdgeOffset;
    for (size_t i = 0; i < count; i++) {
        auto& dst = target[offset + i];
        dst._edgeID = EdgeID {records[i]._edgeID};
        dst._nodeID = NodeID {records[i]._nodeID};
        dst._otherID = NodeID {records[i]._otherID};
        dst._edgeTypeID = EdgeTypeID {records[i]._edgeTypeID};
    }
    offset += count;
}

void GraphLoader::endEdgeContainer() {
    // No-op
}

void GraphLoader::initEmptyEdgeContainer() {
    auto* ec = new EdgeContainer(_partRaw->_firstNodeID, _partRaw->_firstEdgeID, {}, {});
    _partRaw->_edges = std::unique_ptr<EdgeContainer> {ec};
}

// ─────────────────────────────────────────────────────────────────
// EdgeIndexer
// ─────────────────────────────────────────────────────────────────

void GraphLoader::beginEdgeIndexer(uint64_t firstNodeID,
                                   uint64_t firstEdgeID,
                                   uint64_t coreNodeCount,
                                   uint64_t patchNodeCount) {
    auto* idx = new EdgeIndexer(*_partRaw->_edges);
    idx->_firstNodeID = NodeID {firstNodeID};
    idx->_firstEdgeID = EdgeID {firstEdgeID};
    idx->_nodes.resize(coreNodeCount + patchNodeCount);
    _partRaw->_edgeIndexer = std::unique_ptr<EdgeIndexer> {idx};
    _edgeIndexerNodeOffset = 0;
    _edgeIndexerPatchCount = patchNodeCount;
}

void GraphLoader::appendNodeEdgeRanges(const NodeEdgeRangesRaw* ranges, size_t count) {
    auto& nodes = _partRaw->_edgeIndexer->_nodes;
    for (size_t i = 0; i < count; i++) {
        auto& n = nodes[_edgeIndexerNodeOffset + i];
        n._outRange._first = ranges[i]._outFirst;
        n._outRange._count = ranges[i]._outCount;
        n._inRange._first = ranges[i]._inFirst;
        n._inRange._count = ranges[i]._inCount;
    }
    _edgeIndexerNodeOffset += count;
}

void GraphLoader::finalizeEdgeIndexerPatchNodes() {
    auto& idx = *_partRaw->_edgeIndexer;
    const size_t total = idx._nodes.size();
    const size_t patchCount = _edgeIndexerPatchCount;
    const size_t coreCount = total - patchCount;
    idx._patchNodes = {idx._nodes.data(), patchCount};
    idx._coreNodes = {idx._nodes.data() + patchCount, coreCount};

    const auto& outs = idx._edges->getOuts();
    const auto& ins = idx._edges->getIns();
    for (size_t i = 0; i < patchCount; i++) {
        const auto& data = idx._patchNodes[i];
        NodeID nodeID;
        if (data._outRange._count != 0) {
            nodeID = outs[data._outRange._first]._nodeID;
        } else if (data._inRange._count != 0) {
            nodeID = ins[data._inRange._first]._nodeID;
        } else {
            throw TuringException("Patch node without edges");
        }
        idx._patchNodeOffsets[nodeID] = i;
    }
}

void GraphLoader::appendEdgeIndexerSpan(EdgeDirection dir,
                                        uint64_t labelsetID,
                                        uint64_t offset,
                                        uint64_t count) {
    auto& idx = *_partRaw->_edgeIndexer;
    const auto handle = _metadata->_labelsetMap.getValue(
        LabelSetID {static_cast<LabelSetID::Type>(labelsetID)});
    bioassert(handle.has_value(), "labelset not found in edge-indexer span");

    const auto& source = (dir == EdgeDirection::Out)
                             ? idx._edges->getOuts()
                             : idx._edges->getIns();
    auto& spans = (dir == EdgeDirection::Out)
                      ? idx._outLabelSetSpans[handle.value()]
                      : idx._inLabelSetSpans[handle.value()];
    spans.emplace_back(source.data() + offset, count);
}

void GraphLoader::endEdgeIndexer() {
    // No-op
}

// ─────────────────────────────────────────────────────────────────
// Property indexer
// ─────────────────────────────────────────────────────────────────

void GraphLoader::appendPropertyIndexerEntry(EntityKind kind,
                                             uint64_t propTypeID,
                                             uint64_t labelsetID,
                                             const PropIndexRangeRaw* ranges,
                                             size_t count) {
    auto& mgr = currentPropManager(kind);
    auto& idxer = mgr._indexers.try_emplace(
                          PropertyTypeID {static_cast<PropertyTypeID::Type>(propTypeID)},
                          LabelSetPropertyIndexer {})
                      .first->second;

    const auto handle = _metadata->_labelsetMap.getValue(
        LabelSetID {static_cast<LabelSetID::Type>(labelsetID)});
    bioassert(handle.has_value(), "labelset not found in property-indexer entry");
    auto& info = idxer[handle.value()];
    info.resize(count);
    for (size_t i = 0; i < count; i++) {
        info[i]._offset = ranges[i]._offset;
        info[i]._count = ranges[i]._count;
    }
}

PropertyManager& GraphLoader::currentPropManager(EntityKind kind) {
    return (kind == EntityKind::Node) ? *_partRaw->_nodeProperties
                                      : *_partRaw->_edgeProperties;
}

StringPropertyIndexer& GraphLoader::currentStringIndexer(EntityKind kind) {
    return (kind == EntityKind::Node) ? *_partRaw->_nodeStrPropIdx
                                      : *_partRaw->_edgeStrPropIdx;
}

PropertyContainer& GraphLoader::currentPropertyContainer(EntityKind kind, uint64_t propTypeID) {
    auto& mgr = currentPropManager(kind);
    auto it = mgr._map.find(
        PropertyTypeID {static_cast<PropertyTypeID::Type>(propTypeID)});
    bioassert(it != mgr._map.end(), "Property container not registered");
    return *it->second;
}

// ─────────────────────────────────────────────────────────────────
// Property containers
// ─────────────────────────────────────────────────────────────────

void GraphLoader::registerPropertyContainer(EntityKind kind,
                                            uint64_t propTypeID,
                                            ValueType vt,
                                            uint64_t propCount,
                                            uint64_t embeddingDimension) {
    auto& mgr = currentPropManager(kind);
    PropertyTypeID ptID {static_cast<PropertyTypeID::Type>(propTypeID)};
    PropertyContainer* ptr = nullptr;
    switch (vt) {
        case ValueType::UInt64: {
            auto* c = new TypedPropertyContainer<types::UInt64>;
            c->_ids.reserve(propCount);
            c->_values.reserve(propCount);
            ptr = c;
            mgr._uint64s.emplace(ptID, ptr);
            break;
        }
        case ValueType::Int64: {
            auto* c = new TypedPropertyContainer<types::Int64>;
            c->_ids.reserve(propCount);
            c->_values.reserve(propCount);
            ptr = c;
            mgr._int64s.emplace(ptID, ptr);
            break;
        }
        case ValueType::Double: {
            auto* c = new TypedPropertyContainer<types::Double>;
            c->_ids.reserve(propCount);
            c->_values.reserve(propCount);
            ptr = c;
            mgr._doubles.emplace(ptID, ptr);
            break;
        }
        case ValueType::Bool: {
            auto* c = new TypedPropertyContainer<types::Bool>;
            c->_ids.reserve(propCount);
            c->_values.reserve(propCount);
            ptr = c;
            mgr._bools.emplace(ptID, ptr);
            break;
        }
        case ValueType::String: {
            auto* c = new TypedPropertyContainer<types::String>;
            c->_ids.reserve(propCount);
            ptr = c;
            mgr._strings.emplace(ptID, ptr);
            break;
        }
        case ValueType::Embedding: {
            auto* c = new TypedPropertyContainer<types::Embedding>(embeddingDimension);
            c->_ids.reserve(propCount);
            ptr = c;
            mgr._embeddings.emplace(ptID, ptr);
            break;
        }
        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw TuringException("Invalid value type");
    }
    mgr._map.emplace(ptID, std::unique_ptr<PropertyContainer> {ptr});
}

void GraphLoader::appendPropertyIDs(EntityKind kind,
                                    uint64_t propTypeID,
                                    const uint64_t* entityIDs,
                                    size_t count) {
    auto& container = currentPropertyContainer(kind, propTypeID);
    auto& ids = container._ids;
    for (size_t i = 0; i < count; i++) {
        ids.emplace_back(entityIDs[i]);
    }
}

void GraphLoader::appendUInt64PropertyValues(EntityKind kind,
                                             uint64_t propTypeID,
                                             const uint64_t* values,
                                             size_t count) {
    auto& container = currentPropertyContainer(kind, propTypeID).cast<types::UInt64>();
    for (size_t i = 0; i < count; i++) {
        container._values.push_back(values[i]);
    }
}

void GraphLoader::appendInt64PropertyValues(EntityKind kind,
                                            uint64_t propTypeID,
                                            const int64_t* values,
                                            size_t count) {
    auto& container = currentPropertyContainer(kind, propTypeID).cast<types::Int64>();
    for (size_t i = 0; i < count; i++) {
        container._values.push_back(values[i]);
    }
}

void GraphLoader::appendDoublePropertyValues(EntityKind kind,
                                             uint64_t propTypeID,
                                             const double* values,
                                             size_t count) {
    auto& container = currentPropertyContainer(kind, propTypeID).cast<types::Double>();
    for (size_t i = 0; i < count; i++) {
        container._values.push_back(values[i]);
    }
}

void GraphLoader::appendBoolPropertyValues(EntityKind kind,
                                           uint64_t propTypeID,
                                           const uint8_t* values,
                                           size_t count) {
    auto& container = currentPropertyContainer(kind, propTypeID).cast<types::Bool>();
    for (size_t i = 0; i < count; i++) {
        container._values.push_back(static_cast<bool>(values[i]));
    }
}

void GraphLoader::appendStringPropertyValue(EntityKind kind,
                                            uint64_t propTypeID,
                                            std::string_view value) {
    auto& container = currentPropertyContainer(kind, propTypeID).cast<types::String>();
    container._values.alloc(value);
}

void GraphLoader::appendEmbeddingPropertyValues(EntityKind kind,
                                                uint64_t propTypeID,
                                                const float* floats,
                                                size_t numEmbeddings,
                                                size_t dimension) {
    auto& container = currentPropertyContainer(kind, propTypeID).cast<types::Embedding>();
    for (size_t i = 0; i < numEmbeddings; i++) {
        const std::span<const float> view {floats + i * dimension, dimension};
        container._values.alloc(view);
    }
}

void GraphLoader::finalizePropertyContainer(EntityKind kind, uint64_t propTypeID) {
    auto& container = currentPropertyContainer(kind, propTypeID);
    const auto& ids = container._ids;

    switch (container.getValueType()) {
        case ValueType::UInt64: {
            auto& c = container.cast<types::UInt64>();
            c._entityIndexMap.reserve(ids.size());
            for (size_t i = 0; i < ids.size(); i++) c._entityIndexMap[ids[i]] = i;
            break;
        }
        case ValueType::Int64: {
            auto& c = container.cast<types::Int64>();
            c._entityIndexMap.reserve(ids.size());
            for (size_t i = 0; i < ids.size(); i++) c._entityIndexMap[ids[i]] = i;
            break;
        }
        case ValueType::Double: {
            auto& c = container.cast<types::Double>();
            c._entityIndexMap.reserve(ids.size());
            for (size_t i = 0; i < ids.size(); i++) c._entityIndexMap[ids[i]] = i;
            break;
        }
        case ValueType::Bool: {
            auto& c = container.cast<types::Bool>();
            c._entityIndexMap.reserve(ids.size());
            for (size_t i = 0; i < ids.size(); i++) c._entityIndexMap[ids[i]] = i;
            break;
        }
        case ValueType::String: {
            auto& c = container.cast<types::String>();
            c._entityIndexMap.reserve(ids.size());
            for (size_t i = 0; i < ids.size(); i++) c._entityIndexMap[ids[i]] = i;
            break;
        }
        case ValueType::Embedding: {
            auto& c = container.cast<types::Embedding>();
            c._entityIndexMap.reserve(ids.size());
            for (size_t i = 0; i < ids.size(); i++) c._entityIndexMap[ids[i]] = i;
            break;
        }
        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw TuringException("Invalid value type at finalize");
    }
}

// ─────────────────────────────────────────────────────────────────
// String prop indexer
// ─────────────────────────────────────────────────────────────────

void GraphLoader::beginStringPropIndex(EntityKind kind,
                                       uint64_t propTypeID,
                                       uint64_t numTreeNodes) {
    auto& idxer = currentStringIndexer(kind);
    idxer.addIndex(PropertyTypeID {static_cast<PropertyTypeID::Type>(propTypeID)},
                   std::make_unique<StringIndex>(numTreeNodes));
}

StringIndex& GraphLoader::currentStringIndex(EntityKind kind, uint64_t propTypeID) {
    auto& idxer = currentStringIndexer(kind);
    const auto& uniq = idxer.at(
        PropertyTypeID {static_cast<PropertyTypeID::Type>(propTypeID)});
    return *uniq;
}

void GraphLoader::setStringPropIndexChild(EntityKind kind,
                                          uint64_t propTypeID,
                                          uint64_t parentLocalID,
                                          uint64_t childSlot,
                                          uint64_t childLocalID) {
    auto& idx = currentStringIndex(kind, propTypeID);
    auto* parent = idx.getNode(parentLocalID);
    auto* child = idx.getNode(childLocalID);
    parent->setChild(child, childSlot);
}

void GraphLoader::appendStringPropIndexOwners(EntityKind kind,
                                              uint64_t propTypeID,
                                              uint64_t treeNodeLocalID,
                                              const uint64_t* ownerEntityIDs,
                                              size_t count) {
    auto& idx = currentStringIndex(kind, propTypeID);
    auto* node = idx.getNode(treeNodeLocalID);
    for (size_t i = 0; i < count; i++) {
        node->addOwner(EntityID {ownerEntityIDs[i]});
    }
}

void GraphLoader::endStringPropIndex(EntityKind /*kind*/, uint64_t /*propTypeID*/) {
    // No-op
}

void GraphLoader::finalizeStringPropIndexer(EntityKind kind) {
    currentStringIndexer(kind).setInitialised();
}

}
