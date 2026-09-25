#include "NLPropertyValueScan.h"

#include <algorithm>
#include <type_traits>
#include <variant>

#include "NLExecutionContext.h"
#include "reader/GraphReader.h"
#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

using namespace db;

namespace {

// A value the change wrote is held as whatever type the row's own column carried, so it is
// converted to the type the schema holds the property as - which is what a fetch of it does
template <SupportedType T>
bool holdsValue(const CommitWriteBuffer::SupportedTypeVariant& written, const typename T::Primitive& value) {
    using Primitive = typename T::Primitive;

    const auto isValue = [&value](const auto& held) {
        using Inner = typename std::decay_t<decltype(held)>::value_type;

        if constexpr (std::is_convertible_v<const Inner&, Primitive>) {
            return held.has_value() && Primitive(*held) == value;
        } else {
            return false;
        }
    };

    return std::visit(isValue, written);
}

}

template <SupportedType T>
NLPropertyValueScan<T>::NLPropertyValueScan(NLExecutionContext* context,
                                            ColumnNodeIDs* nodeIDs,
                                            PropertyTypeID propertyType,
                                            const Primitive& value,
                                            const LabelSetHandle& labelset)
    : _graphNodes(*context->getView(), propertyType, value, labelset),
    _nodeIDs(nodeIDs)
{
    _graphNodes.setNodeIDs(nodeIDs);

    const CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    if (!writeBuffer) {
        return;
    }

    for (const CommitWriteBuffer::NodeUpdate& update : writeBuffer->updatedNodes()) {
        const CommitWriteBuffer::UntypedProperty& property = update._updatedValue;
        if (property.propertyID != propertyType) {
            continue;
        }

        const bool holds = holdsValue<T>(property.value, value);
        _updatedNodes[update._idToUpdate.getValue()] = holds ? UpdatedNode::HoldsValue : UpdatedNode::LostValue;
    }

    const GraphReader reader = context->getView()->read();

    for (const auto& [node, state] : _updatedNodes) {
        if (state != UpdatedNode::HoldsValue) {
            continue;
        }

        const NodeID nodeID {node};
        const bool carriesTheLabels = !labelset.isValid() || reader.getNodeLabelSet(nodeID).hasAtLeastLabels(labelset);
        if (carriesTheLabels) {
            _gainedNodes.push_back(nodeID);
        }
    }

    std::ranges::sort(_gainedNodes);
}

template <SupportedType T>
NLPropertyValueScan<T>::~NLPropertyValueScan() {
}

template <SupportedType T>
bool NLPropertyValueScan<T>::isValid() const {
    return _graphNodes.isValid() || _nextGainedNode < _gainedNodes.size();
}

template <SupportedType T>
void NLPropertyValueScan<T>::fill(size_t maxCount) {
    if (_graphNodes.isValid()) {
        _graphNodes.fill(maxCount);
        dropNodesThatLostTheValue();
        return;
    }

    _nodeIDs->clear();

    while (_nodeIDs->size() < maxCount && _nextGainedNode < _gainedNodes.size()) {
        const NodeID node = _gainedNodes[_nextGainedNode];
        _nextGainedNode++;

        if (_updatedNodes.at(node.getValue()) != UpdatedNode::FoundInGraph) {
            _nodeIDs->push_back(node);
        }
    }
}

template <SupportedType T>
void NLPropertyValueScan<T>::dropNodesThatLostTheValue() {
    if (_updatedNodes.empty()) {
        return;
    }

    std::vector<NodeID>& nodes = _nodeIDs->getRaw();
    size_t kept = 0;

    for (const NodeID node : nodes) {
        const auto findIt = _updatedNodes.find(node.getValue());
        if (findIt != end(_updatedNodes)) {
            if (findIt->second == UpdatedNode::LostValue) {
                continue;
            }

            findIt->second = UpdatedNode::FoundInGraph;
        }

        nodes[kept] = node;
        kept++;
    }

    nodes.resize(kept);
}

namespace db {

template class NLPropertyValueScan<types::Int64>;
template class NLPropertyValueScan<types::UInt64>;
template class NLPropertyValueScan<types::Double>;
template class NLPropertyValueScan<types::String>;
template class NLPropertyValueScan<types::Bool>;

}
