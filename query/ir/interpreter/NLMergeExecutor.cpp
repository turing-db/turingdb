#include "NLMergeExecutor.h"

#include <stdint.h>

#include <algorithm>
#include <span>
#include <string>
#include <unordered_set>
#include <vector>

#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/GetPropertiesWithNullIterator.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"

#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

#include "NLExecutionContext.h"
#include "NLWriteProperties.h"

#include "BioAssert.h"

using namespace db;

namespace {

template <typename Property>
void appendMergeKey(const std::vector<Property>& properties, size_t row, std::string& key) {
    for (const Property& property : properties) {
        property._keyAppend(property._values, row, key);
    }
}

template <typename ID, typename T>
void fetchMergeProperty(const GraphView& view,
                        PropertyTypeID propertyTypeID,
                        const ColumnVector<ID>* ids,
                        Column* output) {
    auto* typed = static_cast<ColumnOptVector<typename T::Primitive>*>(output);

    GetPropertiesWithNullChunkWriter<ID, T> writer(view, propertyTypeID, ids);
    writer.setOutput(typed);
    writer.fill(ids->size());
}

void fetchMergeNodeProperty(const GraphView& view,
                            const NLMergeScanProperty& property,
                            const ColumnNodeIDs* nodes) {
    const auto fetch = [&]<SupportedType T>() {
        fetchMergeProperty<NodeID, T>(view, property._propertyType._id, nodes, property._values);
    };

    ValueTypeDispatcher(property._propertyType._valueType).execute(fetch);
}

void fetchMergeEdgeProperty(const GraphView& view,
                            const NLMergeScanProperty& property,
                            const ColumnEdgeIDs* edges) {
    const auto fetch = [&]<SupportedType T>() {
        fetchMergeProperty<EdgeID, T>(view, property._propertyType._id, edges, property._values);
    };

    ValueTypeDispatcher(property._propertyType._valueType).execute(fetch);
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
            extractColumnProperties(properties[index]._values,
                                                rowCount,
                                                properties[index]._propertyType._id,
                                                extracted[index]);
        }
    }

    _work->_hopProperties.resize(hops.size());
    for (size_t hopIndex = 0; hopIndex < hops.size(); hopIndex++) {
        const std::vector<NLMergeProperty>& properties = hops[hopIndex]._properties;
        NLMergeWorkingSet::PropertiesPerRow& extracted = _work->_hopProperties[hopIndex];
        extracted.resize(properties.size());

        for (size_t index = 0; index < properties.size(); index++) {
            extractColumnProperties(properties[index]._values,
                                                rowCount,
                                                properties[index]._propertyType._id,
                                                extracted[index]);
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

    std::string key;
    while (scan.isValid()) {
        scan.fill(_context->getChunkSize());

        const size_t rowCount = nodes->size();
        if (rowCount == 0) {
            continue;
        }

        for (const NLMergeScanProperty& property : scanProperties) {
            fetchMergeNodeProperty(*_view, property, nodes);
        }

        for (size_t row = 0; row < rowCount; row++) {
            key.clear();
            appendMergeKey(scanProperties, row, key);

            index->add(key, {._id=(*nodes)[row].getValue(), ._pending=false});
        }
    }
}

void NLMergeExecutor::collectCandidates(size_t row) {
    const std::vector<NLMergeData::Node>& nodes = _data->nodes();

    for (size_t nodeIndex = 0; nodeIndex < nodes.size(); nodeIndex++) {
        const NLMergeData::Node& node = nodes[nodeIndex];
        std::vector<NLMergeRef>& candidates = _work->_candidates[nodeIndex];
        candidates.clear();

        if (node._boundColumn) {
            const bool pending = node._boundPending && (*node._boundPending)[row];
            const uint64_t boundID = (*node._boundColumn)[row].getValue();
            const uint64_t id = pending ? boundID - _firstPendingNodeID : boundID;

            candidates.push_back({._id=id, ._pending=pending});
        } else {
            NLMergeNodeIndex* index = node._index;
            if (!index->isBuilt()) {
                buildNodeIndex(index);
            }

            std::string& key = _work->_key;
            key.clear();
            appendMergeKey(node._properties, row, key);

            const std::span<const NLMergeRef> committed = index->find(key);
            candidates.insert(candidates.end(), committed.begin(), committed.end());

            key.insert(0, node._signature);
            const std::span<const NLMergeRef> pending = _data->getPendingNodes()->find(key);
            candidates.insert(candidates.end(), pending.begin(), pending.end());
        }

        std::unordered_set<uint64_t>& keys = _work->_candidateKeys[nodeIndex];
        keys.clear();
        for (const NLMergeRef& candidate : candidates) {
            keys.insert(candidate.asKey());
        }
    }
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

void NLMergeExecutor::buildHopKeys(const NLMergeData::Hop& hop, size_t row) {
    // The values the hop asks for, which a candidate edge's own values are compared
    // against, and those same values behind the hop's signature - what the pending log
    // is keyed by, since it holds every hop this query wrote under one pair of endpoints
    _work->_hopKey.clear();
    appendMergeKey(hop._properties, row, _work->_hopKey);

    _work->_pendingHopKey.assign(hop._signature);
    _work->_pendingHopKey.append(_work->_hopKey);
}

void NLMergeExecutor::extendHop(size_t hopIndex) {
    const NLMergeData::Hop& hop = _data->hops()[hopIndex];
    const std::unordered_set<uint64_t>& targets = _work->_candidateKeys[hopIndex + 1];
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

                const bool sameType = entry._edgeType == hop._writeEdgeType;
                const bool sameProperties = entry._propertyKey == _work->_pendingHopKey;
                const bool onACandidate = targets.contains(entry._other.asKey());

                if (sameType && sameProperties && onACandidate) {
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
    const Tombstones& tombstones = _view->tombstones();
    const ColumnNodeIDs* sources = hop._scanSources;

    // An undirected hop scans both ways round, and a self-loop is an out-edge and an
    // in-edge of the one node: the outgoing pass has it already
    const bool undirected = hop._direction == NLMergeDirection::Undirected;
    const bool skipSelfLoops = undirected && !outgoing;

    const auto collect = [&](const EdgeRecord& record) {
        if (record._edgeTypeID != hop._matchEdgeType || tombstones.containsEdge(record._edgeID)) {
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

    for (const NLMergeScanProperty& property : scanProperties) {
        fetchMergeEdgeProperty(*_view, property, edges);
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

    const NLMergeRef ref {._id=offset, ._pending=true};

    // A later row asking for the same values binds this node rather than writing a
    // second one, which is what makes a merge over many rows idempotent
    std::string& key = _work->_key;
    key.assign(node._signature);
    appendMergeKey(node._properties, row, key);
    _data->getPendingNodes()->add(key, ref);

    return ref;
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

    // Keyed by its own hop's values, not by whichever hop the match reached before it
    // gave up: a later row looks each hop of the chain up under its own key
    buildHopKeys(hop, row);

    _data->getPendingEdges()->add(source, target, hop._writeEdgeType, offset, _work->_pendingHopKey);

    return {._id=offset, ._pending=true};
}

CommitWriteBuffer::ExistingOrPendingNode NLMergeExecutor::asWriteBufferNode(const NLMergeRef& ref) {
    if (ref._pending) {
        return CommitWriteBuffer::PendingNodeOffset(ref._id);
    }

    return NodeID(ref._id);
}

void NLMergeExecutor::gatherCarriedColumns() {
    const ColumnVector<size_t>* indices = _data->getIndices();

    for (const NLCarriedColumn& carried : _data->carriedColumns()) {
        const NLGatherFunction gather = carried.getGatherFunc();
        gather(carried.getInput(), indices, carried.getOutput());
    }
}
