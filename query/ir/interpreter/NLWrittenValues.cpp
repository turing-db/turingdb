#include "NLWrittenValues.h"

using namespace db;

NLWrittenValues::NLWrittenValues() {
}

NLWrittenValues::~NLWrittenValues() {
}

void NLWrittenValues::indexUpdates(const CommitWriteBuffer* writeBuffer) {
    _writeBuffer = writeBuffer;

    const CommitWriteBuffer::UpdatedNodes& nodes = writeBuffer->updatedNodes();
    for (; _indexedNodeUpdates < nodes.size(); _indexedNodeUpdates++) {
        const CommitWriteBuffer::NodeUpdate& update = nodes[_indexedNodeUpdates];
        const Key key {._entity=update._idToUpdate.getValue(),
                       ._property=update._updatedValue.propertyID.getValue()};

        _nodeUpdates[key] = _indexedNodeUpdates;
    }

    const CommitWriteBuffer::UpdatedEdges& edges = writeBuffer->updatedEdges();
    for (; _indexedEdgeUpdates < edges.size(); _indexedEdgeUpdates++) {
        const CommitWriteBuffer::EdgeUpdate& update = edges[_indexedEdgeUpdates];
        const Key key {._entity=update._idToUpdate.getValue(),
                       ._property=update._updatedValue.propertyID.getValue()};

        _edgeUpdates[key] = _indexedEdgeUpdates;
    }
}

const NLWrittenValues::Value* NLWrittenValues::findNodeUpdate(NodeID node, PropertyTypeID property) const {
    const Key key {._entity=node.getValue(), ._property=property.getValue()};

    const auto findIt = _nodeUpdates.find(key);
    if (findIt == end(_nodeUpdates)) {
        return nullptr;
    }

    return &_writeBuffer->updatedNodes()[findIt->second]._updatedValue.value;
}

const NLWrittenValues::Value* NLWrittenValues::findEdgeUpdate(EdgeID edge, PropertyTypeID property) const {
    const Key key {._entity=edge.getValue(), ._property=property.getValue()};

    const auto findIt = _edgeUpdates.find(key);
    if (findIt == end(_edgeUpdates)) {
        return nullptr;
    }

    return &_writeBuffer->updatedEdges()[findIt->second]._updatedValue.value;
}

const NLWrittenValues::Value& NLWrittenValues::retain(const Value& value) {
    return _retained.emplace_back(value);
}
