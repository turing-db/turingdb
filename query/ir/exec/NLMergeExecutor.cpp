#include "NLMergeExecutor.h"

#include <stdint.h>

#include <algorithm>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <variant>
#include <vector>

#include <spdlog/fmt/bundled/format.h>

#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/GetPropertiesWithNullIterator.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"

#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

#include "NLExecutionContext.h"
#include "NLWriteProperties.h"
#include "NLWrittenValues.h"

#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

template <typename Property>
void appendMergeKey(const std::vector<Property>& properties, size_t row, std::string& key) {
    for (const Property& property : properties) {
        property._keyAppend(property._values, row, key);
    }
}

// The graph holds the values from before the change, so an entity the change updated is
// keyed by the value it wrote rather than by the one the graph holds
template <typename ID, typename T>
void fetchMergeProperty(const GraphView& view,
                        NLWrittenValues& written,
                        PropertyTypeID propertyTypeID,
                        const ColumnVector<ID>* ids,
                        Column* output) {
    auto* typed = static_cast<ColumnOptVector<typename T::Primitive>*>(output);

    GetPropertiesWithNullChunkWriter<ID, T> writer(view, propertyTypeID, ids);
    writer.setOutput(typed);
    writer.fill(ids->size());

    if (!written.hasUpdates()) {
        return;
    }

    auto& values = typed->getRaw();
    const auto& entities = ids->getRaw();
    for (size_t row = 0; row < entities.size(); row++) {
        const NLWrittenValues::Value* update = written.findUpdate(entities[row], propertyTypeID);
        if (update) {
            values[row] = written.read<T>(*update);
        }
    }
}

void fetchMergeNodeProperty(const GraphView& view,
                            NLWrittenValues& written,
                            const NLMergeScanProperty& property,
                            const ColumnNodeIDs* nodes) {
    const auto fetch = [&]<SupportedType T>() {
        fetchMergeProperty<NodeID, T>(view, written, property._propertyType._id, nodes, property._values);
    };

    ValueTypeDispatcher(property._propertyType._valueType).execute(fetch);
}

void fetchMergeEdgeProperty(const GraphView& view,
                            NLWrittenValues& written,
                            const NLMergeScanProperty& property,
                            const ColumnEdgeIDs* edges) {
    const auto fetch = [&]<SupportedType T>() {
        fetchMergeProperty<EdgeID, T>(view, written, property._propertyType._id, edges, property._values);
    };

    ValueTypeDispatcher(property._propertyType._valueType).execute(fetch);
}

// The value a pending entity was written with for one property, as the single row of the
// property's scratch column: null when it was written without one
void readPendingValue(NLWrittenValues& written,
                      const CommitWriteBuffer::UntypedProperties& values,
                      const NLMergeScanProperty& property) {
    const auto read = [&]<SupportedType T>() {
        auto* column = static_cast<ColumnOptVector<typename T::Primitive>*>(property._values);
        column->clear();

        for (const CommitWriteBuffer::UntypedProperty& value : values) {
            if (value.propertyID == property._propertyType._id) {
                column->push_back(written.read<T>(value.value));
                return;
            }
        }

        column->push_back(std::nullopt);
    };

    ValueTypeDispatcher(property._propertyType._valueType).execute(read);
}

// The key a pending entity's own values serialize into, which a row's asked-for values are
// compared against
void appendPendingKey(NLWrittenValues& written,
                      const NLMergeScanProperties& properties,
                      const CommitWriteBuffer::UntypedProperties& values,
                      std::string& key) {
    for (const NLMergeScanProperty& property : properties) {
        readPendingValue(written, values, property);
        property._keyAppend(property._values, 0, key);
    }
}

// A MERGE matches on every property its pattern names, and a null is equal to nothing: a
// row asking for one has neither an entity to match nor a value to write
void throwIfAnyValueIsNull(const CommitWriteBuffer::UntypedProperties& values,
                           std::string_view propertyName,
                           std::string_view entityKind) {
    for (const CommitWriteBuffer::UntypedProperty& value : values) {
        const bool isNull = std::visit([](const auto& held) { return !held.has_value(); }, value.value);

        if (isNull) {
            throw IRException(fmt::format("Cannot merge {} whose property '{}' is null", entityKind, propertyName));
        }
    }
}

// A row an OPTIONAL MATCH did not match holds an invalid ID, which names no node of the
// graph and none of the write buffer either: there is nothing to match the pattern against
// and nothing to hang what it would write off
void throwIfBoundNodeIsNull(const NLMergeData::Node& node, size_t rowCount) {
    const ColumnNodeIDs* bound = node._boundColumn;
    const ColumnMask* pending = node._boundPending;

    for (size_t row = 0; row < rowCount; row++) {
        const bool isPending = pending && (*pending)[row];

        if (!isPending && !(*bound)[row].isValid()) {
            throw IRException("Cannot merge a pattern on a null node");
        }
    }
}

}

NLMergeExecutor::NLMergeExecutor(NLExecutionContext* context, NLMergeData* data)
    : _context(context),
    _data(data),
    _work(data->getWorkingSet()),
    _writeBuffer(context->getWriteBuffer()),
    _view(context->getView()),
    _firstPendingNodeID(committedNodeCount(_view)),
    _firstPendingEdgeID(committedEdgeCount(_view))
{
}

NLMergeExecutor::~NLMergeExecutor() {
}

void NLMergeExecutor::run() {
    bioassert(_writeBuffer, "nl.merge requires an active write transaction");

    const Column* rowCarrier = _data->getRowCarrier();

    // A pattern reading no column at all stands on its own, and runs over the single
    // row its literals are - the row db.create_node writes without a cardinality
    const size_t rowCount = rowCarrier ? rowCarrier->size() : 1;

    clearResults();
    extractProperties(rowCount);

    for (const NLMergeData::Node& node : _data->nodes()) {
        if (node._boundColumn) {
            throwIfBoundNodeIsNull(node, rowCount);
        }
    }

    _work->_candidates.resize(_data->nodes().size());
    _work->_candidateKeys.resize(_data->nodes().size());

    for (size_t row = 0; row < rowCount; row++) {
        matchRow(row);
    }

    gatherCarriedColumns();
}

void NLMergeExecutor::clearResults() {
    // A bound node has no output of its own: its rows come back through the carry set
    for (const NLMergeData::Node& node : _data->nodes()) {
        if (node._output) {
            node._output->clear();
            node._outputPending->clear();
        }
    }

    for (const NLMergeData::Hop& hop : _data->hops()) {
        hop._output->clear();
        hop._outputPending->clear();
    }

    _data->getCreated()->clear();
    _data->getIndices()->clear();
}

void NLMergeExecutor::extractProperties(size_t rowCount) {
    const std::vector<NLMergeData::Node>& nodes = _data->nodes();
    const std::vector<NLMergeData::Hop>& hops = _data->hops();

    _work->_nodeProperties.resize(nodes.size());
    for (size_t nodeIndex = 0; nodeIndex < nodes.size(); nodeIndex++) {
        const std::vector<NLMergeProperty>& properties = nodes[nodeIndex]._properties;
        // The extractor clears each buffer it fills, so a step reuses what the last one
        // allocated
        NLMergeWorkingSet::PropertiesPerRow& extracted = _work->_nodeProperties[nodeIndex];
        extracted.resize(properties.size());

        for (size_t index = 0; index < properties.size(); index++) {
            const NLMergeProperty& property = properties[index];

            extractColumnProperties(property._values, rowCount, property._propertyType, extracted[index]);
            throwIfAnyValueIsNull(extracted[index], property._name, "a node");
        }
    }

    _work->_hopProperties.resize(hops.size());
    for (size_t hopIndex = 0; hopIndex < hops.size(); hopIndex++) {
        const std::vector<NLMergeProperty>& properties = hops[hopIndex]._properties;
        NLMergeWorkingSet::PropertiesPerRow& extracted = _work->_hopProperties[hopIndex];
        extracted.resize(properties.size());

        for (size_t index = 0; index < properties.size(); index++) {
            const NLMergeProperty& property = properties[index];

            extractColumnProperties(property._values, rowCount, property._propertyType, extracted[index]);
            throwIfAnyValueIsNull(extracted[index], property._name, "an edge");
        }
    }
}

// One scan of the spec's label set with one property fetch per key property per chunk,
// rather than a lookup per row of the merge
void NLMergeExecutor::buildNodeIndex(NLMergeNodeIndex* index) {
    index->markBuilt();

    if (!index->isMatchable()) {
        return;
    }

    const LabelSetHandle labelset(index->getLabels());
    const std::vector<NLMergeScanProperty>& scanProperties = index->scanProperties();
    ColumnNodeIDs* nodes = index->getScanNodes();

    ScanNodesByLabelChunkWriter scan(*_view, labelset);
    scan.setNodeIDs(nodes);

    NLWrittenValues& written = _context->getWrittenValues();
    written.indexUpdates(_writeBuffer);

    std::string key;
    while (scan.isValid()) {
        scan.fill(_context->getChunkSize());

        const size_t rowCount = nodes->size();
        if (rowCount == 0) {
            continue;
        }

        for (const NLMergeScanProperty& property : scanProperties) {
            fetchMergeNodeProperty(*_view, written, property, nodes);
        }

        for (size_t row = 0; row < rowCount; row++) {
            key.clear();
            appendMergeKey(scanProperties, row, key);

            index->add(key, {._id=(*nodes)[row].getValue(), ._pending=false});
        }
    }

    index->setNextNodeUpdate(_writeBuffer->updatedNodes().size());
}

void NLMergeExecutor::collectCandidates(size_t row) {
    const std::vector<NLMergeData::Node>& nodes = _data->nodes();

    for (size_t nodeIndex = 0; nodeIndex < nodes.size(); nodeIndex++) {
        const NLMergeData::Node& node = nodes[nodeIndex];
        std::vector<NLMergeRef>& candidates = _work->_candidates[nodeIndex];
        std::unordered_set<uint64_t>& keys = _work->_candidateKeys[nodeIndex];
        candidates.clear();
        keys.clear();

        if (node._boundColumn) {
            const uint64_t boundID = (*node._boundColumn)[row].getValue();
            const bool marked = node._boundPending && (*node._boundPending)[row];
            const bool namesWritten = boundID >= _firstPendingNodeID
                                      && boundID - _firstPendingNodeID < _writeBuffer->numPendingNodes();
            const bool pending = marked || namesWritten;
            const uint64_t id = pending ? boundID - _firstPendingNodeID : boundID;

            candidates.push_back({._id=id, ._pending=pending});
            keys.insert(candidates.back().asKey());
        } else {
            NLMergeNodeIndex* index = node._index;
            if (!index->isBuilt()) {
                buildNodeIndex(index);
            }

            rekeyUpdatedNodes(index);
            absorbPendingNodes(index);

            std::string& key = _work->_key;
            key.clear();
            appendMergeKey(node._properties, row, key);

            for (const NLMergeRef& candidate : index->find(key)) {
                const bool holds = !isDeletedNode(candidate)
                                   && (!index->hasChanged(candidate) || holdsTheKey(index, candidate, key));

                if (holds && keys.insert(candidate.asKey()).second) {
                    candidates.push_back(candidate);
                }
            }
        }
    }
}

// The updates since the index last looked, to the graph's nodes and to the ones this query
// wrote: a node whose key values one of them changed is keyed again under its new values.
// A node the index has not taken in yet is read as it stands when it is.
void NLMergeExecutor::rekeyUpdatedNodes(NLMergeNodeIndex* index) {
    const NLMergeScanProperties& keyProperties = index->writtenProperties();
    const auto isKeyProperty = [&keyProperties](PropertyTypeID property) {
        return std::ranges::any_of(keyProperties, [property](const NLMergeScanProperty& keyProperty) {
            return keyProperty._propertyType._id == property;
        });
    };

    const CommitWriteBuffer::UpdatedNodes& updates = _writeBuffer->updatedNodes();
    for (size_t position = index->getNextNodeUpdate(); position < updates.size(); position++) {
        const CommitWriteBuffer::NodeUpdate& update = updates[position];

        if (isKeyProperty(update._updatedValue.propertyID)) {
            rekeyNode(index, {._id=update._idToUpdate.getValue(), ._pending=false});
        }
    }

    index->setNextNodeUpdate(updates.size());

    const std::vector<NLWrittenValues::PendingNodeUpdate>& pendingUpdates = _context->getWrittenValues().pendingNodeUpdates();
    for (size_t position = index->getNextPendingNodeUpdate(); position < pendingUpdates.size(); position++) {
        const NLWrittenValues::PendingNodeUpdate& update = pendingUpdates[position];
        const bool takenIn = update._offset < index->getNextPendingNode();

        if (takenIn && isKeyProperty(update._property)) {
            rekeyNode(index, {._id=update._offset, ._pending=true});
        }
    }

    index->setNextPendingNodeUpdate(pendingUpdates.size());
}

void NLMergeExecutor::rekeyNode(NLMergeNodeIndex* index, const NLMergeRef& node) {
    const LabelSetHandle labelset(index->getWriteLabels());

    const LabelSetHandle nodeLabels = node._pending
                                      ? _writeBuffer->getPendingNode(node._id).labelsetHandle
                                      : _view->read().getNodeLabelSet(NodeID(node._id));
    if (!nodeLabels.hasAtLeastLabels(labelset)) {
        return;
    }

    std::string& key = _work->_scanKey;
    key.clear();
    appendCurrentKey(index, node, key);

    index->add(key, node);
    index->markChanged(node);
}

void NLMergeExecutor::appendCurrentKey(NLMergeNodeIndex* index, const NLMergeRef& node, std::string& key) {
    const NLMergeScanProperties& keyProperties = index->writtenProperties();
    NLWrittenValues& written = _context->getWrittenValues();

    if (node._pending) {
        appendPendingKey(written, keyProperties, _writeBuffer->getPendingNode(node._id).properties, key);
        return;
    }

    ColumnNodeIDs* nodes = index->getScanNodes();
    nodes->clear();
    nodes->push_back(NodeID(node._id));

    written.indexUpdates(_writeBuffer);

    for (const NLMergeScanProperty& property : keyProperties) {
        fetchMergeNodeProperty(*_view, written, property, nodes);
        property._keyAppend(property._values, 0, key);
    }
}

bool NLMergeExecutor::holdsTheKey(NLMergeNodeIndex* index, const NLMergeRef& node, const std::string& key) {
    std::string& currentKey = _work->_scanKey;
    currentKey.clear();
    appendCurrentKey(index, node, currentKey);

    return currentKey == key;
}

bool NLMergeExecutor::isDeletedNode(const NLMergeRef& node) const {
    if (node._pending) {
        return _writeBuffer->deletedPendingNodes().contains(node._id);
    }

    return _view->isDeleted(NodeID(node._id));
}

// A node this query wrote, whichever clause wrote it, is in no graph the index scanned: it
// is taken in once the write is done, so a later row or merge binds it rather than
// writing a second copy
void NLMergeExecutor::absorbPendingNodes(NLMergeNodeIndex* index) {
    const size_t pendingCount = _writeBuffer->numPendingNodes();
    const size_t firstOffset = std::max(index->getNextPendingNode(), _context->getFirstQueryNode());
    if (firstOffset >= pendingCount) {
        return;
    }

    const LabelSetHandle labelset(index->getWriteLabels());
    const CommitWriteBuffer::DeletedPendingEntities& deleted = _writeBuffer->deletedPendingNodes();
    NLWrittenValues& written = _context->getWrittenValues();
    std::string& key = _work->_scanKey;

    for (size_t offset = firstOffset; offset < pendingCount; offset++) {
        const CommitWriteBuffer::PendingNode& node = _writeBuffer->getPendingNode(offset);
        if (deleted.contains(offset) || !node.labelsetHandle.hasAtLeastLabels(labelset)) {
            continue;
        }

        key.clear();
        appendPendingKey(written, index->writtenProperties(), node.properties, key);

        index->add(key, {._id=offset, ._pending=true});
    }

    index->setNextPendingNode(pendingCount);
}

void NLMergeExecutor::absorbPendingEdges() {
    NLMergePendingEdges* pendingEdges = _data->getPendingEdges();

    const size_t pendingCount = _writeBuffer->numPendingEdges();
    const size_t firstOffset = std::max(pendingEdges->getNextPendingEdge(), _context->getFirstQueryEdge());
    if (firstOffset >= pendingCount) {
        return;
    }

    const CommitWriteBuffer::DeletedPendingEntities& deleted = _writeBuffer->deletedPendingEdges();

    for (size_t offset = firstOffset; offset < pendingCount; offset++) {
        if (deleted.contains(offset)) {
            continue;
        }

        const CommitWriteBuffer::PendingEdge& edge = _writeBuffer->getPendingEdge(offset);
        pendingEdges->add(asMergeRef(edge.src), asMergeRef(edge.tgt), edge.edgeType, offset);
    }

    pendingEdges->setNextPendingEdge(pendingCount);
}

bool NLMergeExecutor::holdsTheHopValues(const NLMergeData::Hop& hop, uint64_t offset) {
    std::string& key = _work->_scanKey;
    key.clear();

    const CommitWriteBuffer::PendingEdge& edge = _writeBuffer->getPendingEdge(offset);
    appendPendingKey(_context->getWrittenValues(), hop._writtenProperties, edge.properties, key);

    return key == _work->_hopKey;
}

void NLMergeExecutor::matchRow(size_t row) {
    collectCandidates(row);

    const std::vector<NLMergeData::Hop>& hops = _data->hops();
    const size_t nodeCount = _data->nodes().size();

    std::vector<NLMergePartialMatch>& matches = _work->_matches;
    matches.clear();
    for (const NLMergeRef& candidate : _work->_candidates.front()) {
        NLMergePartialMatch& match = matches.emplace_back();

        // The walk appends one node and one edge per hop, so the whole chain fits the
        // allocation the first node forces
        match._nodes.reserve(nodeCount);
        match._edges.reserve(hops.size());
        match._nodes.push_back(candidate);
    }

    for (size_t hopIndex = 0; hopIndex < hops.size() && !matches.empty(); hopIndex++) {
        buildHopKeys(hops[hopIndex], row);

        extendHop(hopIndex);
    }

    if (matches.empty()) {
        writeRow(row);
        return;
    }

    for (const NLMergePartialMatch& match : matches) {
        emitRow(match, row, /*created=*/false);
    }
}

// The values the hop asks for, which a candidate edge's own values are compared against
void NLMergeExecutor::buildHopKeys(const NLMergeData::Hop& hop, size_t row) {
    _work->_hopKey.clear();
    appendMergeKey(hop._properties, row, _work->_hopKey);
}

void NLMergeExecutor::extendHop(size_t hopIndex) {
    const NLMergeData::Hop& hop = _data->hops()[hopIndex];
    const std::unordered_set<uint64_t>& targets = _work->_candidateKeys[hopIndex + 1];

    absorbPendingEdges();
    const NLMergePendingEdges* pendingEdges = _data->getPendingEdges();

    collectGraphExtensions(hop, hopIndex);

    const bool followsOutgoing = hop._direction != NLMergeDirection::Backward;
    const bool followsIncoming = hop._direction != NLMergeDirection::Forward;

    std::vector<NLMergePartialMatch>& matches = _work->_matches;
    std::vector<NLMergePartialMatch>& extended = _work->_extended;

    extended.clear();
    for (const NLMergePartialMatch& match : matches) {
        const NLMergeRef source = match._nodes.back();

        // Cypher binds each relationship of a pattern to an edge of its own, so a hop
        // cannot walk back along one the match already holds
        const auto extendWith = [&](const NLMergeRef& edge, const NLMergeRef& target) {
            if (std::ranges::find(match._edges, edge) != end(match._edges)) {
                return;
            }

            NLMergePartialMatch& grown = extended.emplace_back(match);
            grown._edges.push_back(edge);
            grown._nodes.push_back(target);
        };

        const auto runIt = _work->_extensionRuns.find(source.asKey());
        if (runIt != end(_work->_extensionRuns)) {
            const auto [first, last] = runIt->second;
            for (size_t index = first; index < last; index++) {
                const NLMergeExtension& extension = _work->_extensions[index];
                extendWith(extension._edge, extension._target);
            }
        }

        // An undirected hop follows both ways round, and a self-loop is on both sides of
        // its own node: the outgoing pass has it, so the incoming one leaves it alone
        const auto extendWithPending = [&](std::span<const NLMergePendingEdges::Entry> entries,
                                           bool skipSelfLoops) {
            for (const NLMergePendingEdges::Entry& entry : entries) {
                if (skipSelfLoops && entry._other == source) {
                    continue;
                }

                const bool deleted = _writeBuffer->deletedPendingEdges().contains(entry._offset);
                const bool sameType = entry._edgeType == hop._writeEdgeType;
                const bool onACandidate = targets.contains(entry._other.asKey());

                if (!deleted && sameType && onACandidate && holdsTheHopValues(hop, entry._offset)) {
                    extendWith({._id=entry._offset, ._pending=true}, entry._other);
                }
            }
        };

        if (followsOutgoing) {
            extendWithPending(pendingEdges->outOf(source), /*skipSelfLoops=*/false);
        }

        if (followsIncoming) {
            extendWithPending(pendingEdges->into(source), /*skipSelfLoops=*/followsOutgoing);
        }
    }

    matches.swap(extended);
}

void NLMergeExecutor::collectGraphExtensions(const NLMergeData::Hop& hop, size_t hopIndex) {
    _work->_extensions.clear();
    _work->_extensionRuns.clear();

    // A type the schema does not have is on no committed edge, so only a pending one can
    // extend the match
    if (!hop._matchEdgeType.isValid()) {
        return;
    }

    ColumnNodeIDs* sources = hop._scanSources;
    sources->clear();

    // Two partial matches reaching the same node scan its edges once: the runs below are
    // keyed by source node, so a second copy of one would fold into its run and extend
    // every match that reached it a second time.
    std::unordered_set<uint64_t>& sourceKeys = _work->_scanSourceKeys;
    sourceKeys.clear();

    for (const NLMergePartialMatch& match : _work->_matches) {
        const NLMergeRef source = match._nodes.back();
        if (source._pending) {
            continue;
        }

        if (sourceKeys.insert(source.asKey()).second) {
            sources->push_back(NodeID(source._id));
        }
    }

    if (sources->empty()) {
        return;
    }

    const bool followsOutgoing = hop._direction != NLMergeDirection::Backward;
    const bool followsIncoming = hop._direction != NLMergeDirection::Forward;

    if (followsOutgoing) {
        collectDirectedExtensions(hop, hopIndex, /*outgoing=*/true);
    }

    if (followsIncoming) {
        collectDirectedExtensions(hop, hopIndex, /*outgoing=*/false);
    }

    dropExtensionsWithOtherProperties(hop);
    groupExtensionsBySource();
}

void NLMergeExecutor::collectDirectedExtensions(const NLMergeData::Hop& hop, size_t hopIndex, bool outgoing) {
    const std::unordered_set<uint64_t>& targets = _work->_candidateKeys[hopIndex + 1];
    const ColumnNodeIDs* sources = hop._scanSources;

    // An undirected hop scans both ways round, and a self-loop is an out-edge and an
    // in-edge of the one node: the outgoing pass has it already
    const bool undirected = hop._direction == NLMergeDirection::Undirected;
    const bool skipSelfLoops = undirected && !outgoing;

    const auto collect = [&](const EdgeRecord& record) {
        if (record._edgeTypeID != hop._matchEdgeType || _view->isDeleted(record._edgeID)) {
            return;
        }

        if (skipSelfLoops && record._otherID == record._nodeID) {
            return;
        }

        const NLMergeRef target {._id=record._otherID.getValue(), ._pending=false};
        if (!targets.contains(target.asKey())) {
            return;
        }

        _work->_extensions.push_back({._source={._id=record._nodeID.getValue(), ._pending=false},
                                      ._edge={._id=record._edgeID.getValue(), ._pending=false},
                                      ._target=target});
    };

    if (outgoing) {
        const GetOutEdgesRange outEdges(*_view, sources);
        for (const EdgeRecord& record : outEdges) {
            collect(record);
        }
    } else {
        const GetInEdgesRange inEdges(*_view, sources);
        for (const EdgeRecord& record : inEdges) {
            collect(record);
        }
    }
}

void NLMergeExecutor::dropExtensionsWithOtherProperties(const NLMergeData::Hop& hop) {
    const std::vector<NLMergeScanProperty>& scanProperties = hop._scanProperties;
    std::vector<NLMergeExtension>& extensions = _work->_extensions;
    if (scanProperties.empty() || extensions.empty()) {
        return;
    }

    ColumnEdgeIDs* edges = hop._scanEdges;
    edges->clear();
    for (const NLMergeExtension& extension : extensions) {
        edges->push_back(EdgeID(extension._edge._id));
    }

    NLWrittenValues& written = _context->getWrittenValues();
    written.indexUpdates(_writeBuffer);

    for (const NLMergeScanProperty& property : scanProperties) {
        fetchMergeEdgeProperty(*_view, written, property, edges);
    }

    std::vector<NLMergeExtension>& kept = _work->_keptExtensions;
    std::string& key = _work->_scanKey;

    kept.clear();
    for (size_t index = 0; index < extensions.size(); index++) {
        key.clear();
        appendMergeKey(scanProperties, index, key);

        if (key == _work->_hopKey) {
            kept.push_back(extensions[index]);
        }
    }

    extensions.swap(kept);
}

void NLMergeExecutor::groupExtensionsBySource() {
    std::vector<NLMergeExtension>& extensions = _work->_extensions;

    std::ranges::stable_sort(extensions, [](const NLMergeExtension& lhs, const NLMergeExtension& rhs) {
        return lhs._source.asKey() < rhs._source.asKey();
    });

    size_t first = 0;
    while (first < extensions.size()) {
        const uint64_t sourceKey = extensions[first]._source.asKey();

        size_t last = first + 1;
        while (last < extensions.size() && extensions[last]._source.asKey() == sourceKey) {
            last++;
        }

        _work->_extensionRuns[sourceKey] = {first, last};
        first = last;
    }
}

void NLMergeExecutor::emitRow(const NLMergePartialMatch& match, size_t row, bool created) {
    const std::vector<NLMergeData::Node>& nodes = _data->nodes();
    const std::vector<NLMergeData::Hop>& hops = _data->hops();

    for (size_t nodeIndex = 0; nodeIndex < nodes.size(); nodeIndex++) {
        const NLMergeData::Node& node = nodes[nodeIndex];
        if (!node._output) {
            continue;
        }

        const NLMergeRef& entity = match._nodes[nodeIndex];
        const uint64_t nodeID = entity._pending ? entity._id + _firstPendingNodeID : entity._id;

        node._output->push_back(NodeID(nodeID));
        node._outputPending->push_back(entity._pending);
    }

    for (size_t hopIndex = 0; hopIndex < hops.size(); hopIndex++) {
        const NLMergeRef& edge = match._edges[hopIndex];
        const uint64_t edgeID = edge._pending ? edge._id + _firstPendingEdgeID : edge._id;

        hops[hopIndex]._output->push_back(EdgeID(edgeID));
        hops[hopIndex]._outputPending->push_back(edge._pending);
    }

    _data->getCreated()->push_back(created);
    _data->getIndices()->push_back(row);
}

void NLMergeExecutor::writeRow(size_t row) {
    const std::vector<NLMergeData::Node>& nodes = _data->nodes();
    const std::vector<NLMergeData::Hop>& hops = _data->hops();

    NLMergePartialMatch written;
    written._nodes.reserve(nodes.size());
    written._edges.reserve(hops.size());

    for (size_t nodeIndex = 0; nodeIndex < nodes.size(); nodeIndex++) {
        const NLMergeData::Node& node = nodes[nodeIndex];

        if (node._boundColumn) {
            written._nodes.push_back(_work->_candidates[nodeIndex].front());
        } else {
            written._nodes.push_back(writeNode(node, nodeIndex, row));
        }
    }

    for (size_t hopIndex = 0; hopIndex < hops.size(); hopIndex++) {
        const NLMergeData::Hop& hop = hops[hopIndex];

        // An undirected hop is written pointing forward, the way Cypher writes it
        const bool backward = hop._direction == NLMergeDirection::Backward;
        const NLMergeRef& source = backward ? written._nodes[hopIndex + 1] : written._nodes[hopIndex];
        const NLMergeRef& target = backward ? written._nodes[hopIndex] : written._nodes[hopIndex + 1];

        written._edges.push_back(writeEdge(hop, hopIndex, row, source, target));
    }

    emitRow(written, row, /*created=*/true);
}

NLMergeRef NLMergeExecutor::writeNode(const NLMergeData::Node& node, size_t nodeIndex, size_t row) {
    const uint64_t offset = _writeBuffer->numPendingNodes();

    CommitWriteBuffer::PendingNode& pending = _writeBuffer->newPendingNode();
    pending.labelsetHandle = node._labelSetHandle;

    for (const CommitWriteBuffer::UntypedProperties& values : _work->_nodeProperties[nodeIndex]) {
        pending.properties.push_back(values[row]);
    }

    return {._id=offset, ._pending=true};
}

NLMergeRef NLMergeExecutor::writeEdge(const NLMergeData::Hop& hop,
                                  size_t hopIndex,
                                  size_t row,
                                  const NLMergeRef& source,
                                  const NLMergeRef& target) {
    const uint64_t offset = _writeBuffer->numPendingEdges();

    CommitWriteBuffer::PendingEdge& pending = _writeBuffer->newPendingEdge(asWriteBufferNode(source),
                                                                          asWriteBufferNode(target));
    pending.edgeType = hop._writeEdgeType;

    for (const CommitWriteBuffer::UntypedProperties& values : _work->_hopProperties[hopIndex]) {
        pending.properties.push_back(values[row]);
    }

    return {._id=offset, ._pending=true};
}

CommitWriteBuffer::ExistingOrPendingNode NLMergeExecutor::asWriteBufferNode(const NLMergeRef& ref) {
    if (ref._pending) {
        return CommitWriteBuffer::PendingNodeOffset(ref._id);
    }

    return NodeID(ref._id);
}

NLMergeRef NLMergeExecutor::asMergeRef(const CommitWriteBuffer::ExistingOrPendingNode& node) {
    if (const NodeID* committed = std::get_if<NodeID>(&node)) {
        return {._id=committed->getValue(), ._pending=false};
    }

    return {._id=std::get<CommitWriteBuffer::PendingNodeOffset>(node), ._pending=true};
}

void NLMergeExecutor::gatherCarriedColumns() {
    const ColumnVector<size_t>* indices = _data->getIndices();

    for (const NLCarriedColumn& carried : _data->carriedColumns()) {
        const NLGatherFunction gather = carried.getGatherFunc();
        gather(carried.getInput(), indices, carried.getOutput());
    }
}
