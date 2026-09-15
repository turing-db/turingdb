#include "NLPendingNodes.h"

#include "NLExecutionContext.h"
#include "NLWriteProperties.h"

using namespace db;

NLPendingNodeScan::NLPendingNodeScan(NLExecutionContext* context, ColumnNodeIDs* nodeIDs)
    : _writeBuffer(context->getWriteBuffer()),
    _nodeIDs(nodeIDs),
    _pendingNodeCount(_writeBuffer ? _writeBuffer->numPendingNodes() : 0)
{
    _node = context->getFirstQueryNode();

    if (_pendingNodeCount == 0) {
        return;
    }

    _firstPendingNodeID = committedNodeCount(context->getView());
}

NLPendingNodeScan::~NLPendingNodeScan() {
}

void NLPendingNodeScan::setLabelSet(const LabelSet& labelset) {
    _labelset = LabelSetHandle(labelset);
}

void NLPendingNodeScan::setProperty(PropertyTypeID propertyType,
                                    const CommitWriteBuffer::SupportedTypeVariant& value) {
    _propertyType = propertyType;
    _value = value;
}

void NLPendingNodeScan::fill(size_t maxCount) {
    _nodeIDs->clear();

    size_t remainingToMax = maxCount;

    while (remainingToMax > 0 && _node < _pendingNodeCount) {
        const size_t offset = _node;
        _node++;

        if (_writeBuffer->deletedPendingNodes().contains(offset)) {
            continue;
        }

        if (!matches(_writeBuffer->getPendingNode(offset))) {
            continue;
        }

        _nodeIDs->push_back(NodeID(_firstPendingNodeID + offset));

        remainingToMax--;
    }
}

bool NLPendingNodeScan::matches(const CommitWriteBuffer::PendingNode& node) const {
    if (_labelset.isValid() && !node.labelsetHandle.hasAtLeastLabels(_labelset)) {
        return false;
    }

    if (!_value) {
        return true;
    }

    for (const CommitWriteBuffer::UntypedProperty& property : node.properties) {
        const bool isTheProperty = property.propertyID == _propertyType;
        if (isTheProperty && property.value == *_value) {
            return true;
        }
    }

    return false;
}
