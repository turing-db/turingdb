#include "DataPartBuilder.h"

#include "Graph.h"
#include "ID.h"
#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"
#include "properties/PropertyManager.h"
#include "writers/MetadataBuilder.h"

using namespace db;

namespace {
// Functor for helping get the ValueType from an UntypedProperty
struct ValueTypeFromProperty {
    ValueType operator()(types::Int64::Primitive propValue) const {
        return types::Int64::_valueType;
    }
    ValueType operator()(types::UInt64::Primitive propValue) const {
        return types::UInt64::_valueType;
    }
    ValueType operator()(types::Double::Primitive propValue) const {
        return types::Double::_valueType;
    }
    ValueType operator()(std::string propValue) const {
        return types::String::_valueType;
    }
    ValueType operator()(types::Bool::Primitive propValue) const {
        return types::Bool::_valueType;
    }
};

}

DataPartBuilder::~DataPartBuilder() = default;

std::unique_ptr<DataPartBuilder> DataPartBuilder::prepare(MetadataBuilder& metadata,
                                                          const GraphView& view,
                                                          size_t partIndex) {
    const auto reader = view.read();
    auto* ptr = new DataPartBuilder();

    ptr->_view = view;
    ptr->_metadata = &metadata;
    ptr->_firstNodeID = reader.getNodeCount();
    ptr->_firstEdgeID = reader.getEdgeCount();
    ptr->_nextNodeID = ptr->_firstNodeID;
    ptr->_nextEdgeID = ptr->_firstEdgeID;
    ptr->_nodeProperties = std::make_unique<PropertyManager>();
    ptr->_edgeProperties = std::make_unique<PropertyManager>();
    ptr->_partIndex = partIndex;

    return std::unique_ptr<DataPartBuilder> {ptr};
}

NodeID DataPartBuilder::addNode(const LabelSetHandle& labelset) {
    if (!labelset.isStored()) {
        const LabelSet toBeStored = LabelSet::fromIntegers(labelset.integers());
        LabelSetHandle stored = _metadata->getOrCreateLabelSet(toBeStored);
        _coreNodeLabelSets.emplace_back(stored);
    } else {
        _coreNodeLabelSets.emplace_back(labelset);
    }

    return _nextNodeID++;
}

NodeID DataPartBuilder::addNode(const LabelSet& labelset) {
    LabelSetHandle ref = _metadata->getOrCreateLabelSet(labelset);
    _coreNodeLabelSets.emplace_back(ref);

    return _nextNodeID++;
}

template <SupportedType T>
void DataPartBuilder::addNodeProperty(NodeID nodeID,
                                      PropertyTypeID ptID,
                                      T::Primitive value) {
    if (!_nodeProperties->hasPropertyType(ptID)) {
        _nodeProperties->registerPropertyType<T>(ptID);
    }

    if (nodeID < _firstNodeID) {
        _patchNodeLabelSets.emplace(nodeID, LabelSetHandle {});
    }
    _nodeProperties->add<T>(ptID, nodeID.getValue(), std::move(value));
}

template <SupportedType T>
void DataPartBuilder::addEdgeProperty(const EdgeRecord& edge,
                                      PropertyTypeID ptID,
                                      T::Primitive value) {
    if (!_edgeProperties->hasPropertyType(ptID)) {
        _edgeProperties->registerPropertyType<T>(ptID);
    }
    if (edge._edgeID < _firstEdgeID) {
        _patchedEdges.emplace(edge._edgeID, &edge);
        _patchNodeLabelSets.emplace(edge._nodeID, LabelSetHandle {});
    }
    _edgeProperties->add<T>(ptID, edge._edgeID.getValue(), std::move(value));
}

const EdgeRecord& DataPartBuilder::addEdge(EdgeTypeID typeID, NodeID srcID, NodeID tgtID) {
    auto& edge = _edges.emplace_back();
    edge._edgeID = _nextEdgeID;
    edge._nodeID = srcID;
    edge._otherID = tgtID;
    edge._edgeTypeID = typeID;


    if (edge._nodeID < _firstNodeID) {
        _nodeHasPatchEdges.emplace(edge._nodeID);
        _patchNodeLabelSets.emplace(edge._nodeID, LabelSetHandle {});
        _outPatchEdgeCount += 1;
    }

    if (edge._otherID < _firstNodeID) {
        _nodeHasPatchEdges.emplace(edge._otherID);
        _patchNodeLabelSets.emplace(edge._otherID, LabelSetHandle {});
        _inPatchEdgeCount += 1;
    }

    ++_nextEdgeID;
    return edge;
}

NodeID DataPartBuilder::addPendingNode(CommitWriteBuffer::PendingNode& node) {
    using CWB = CommitWriteBuffer;
    // Build/get the LabelSet for this node
    LabelSet labelSet;
    for (const std::string& label : node.labelNames) {
        const auto labelID = _metadata->getOrCreateLabel(label);
        labelSet.set(labelID);
    }

    const NodeID nodeID = this->addNode(labelSet);

    // Adding node properties
    for (const CWB::UntypedProperty& prop : node.properties) {
        // Get the value type from the untyped property
        const ValueType propValueType = std::visit(ValueTypeFromProperty {}, prop.value);
        // Get the existing ID, or create a new property and get that ID
        const PropertyTypeID propID =
            _metadata->getOrCreatePropertyType(prop.propertyName, propValueType)._id;

        // Add this property to this node
        std::visit(
            [&](auto&& propValue) {
                using T = std::decay_t<decltype(propValue)>;

                if constexpr (std::is_same_v<T, types::Int64::Primitive>) {
                    this->addNodeProperty<types::Int64>(nodeID, propID, propValue);
                } else if constexpr (std::is_same_v<T, types::UInt64::Primitive>) {
                    this->addNodeProperty<types::UInt64>(nodeID, propID, propValue);
                } else if constexpr (std::is_same_v<T, types::Double::Primitive>) {
                    this->addNodeProperty<types::Double>(nodeID, propID, propValue);
                } else if constexpr (std::is_same_v<T, std::string>) {
                    this->addNodeProperty<types::String>(nodeID, propID, propValue);
                } else if constexpr (std::is_same_v<T, types::Bool::Primitive>) {
                    this->addNodeProperty<types::Bool>(nodeID, propID, propValue);
                }
            },
            prop.value);
    }

    return nodeID;
}

std::optional<EdgeID> DataPartBuilder::addPendingEdge(const CommitWriteBuffer& wb,
                                                      const CommitWriteBuffer::PendingEdge& edge,
                                                      const std::unordered_map<CommitWriteBuffer::PendingNodeOffset, NodeID>& tempNodeIDMap) {
    using CWB = CommitWriteBuffer;
    // If this edge has source or target which is a node in a previous datapart, check
    // if it has been deleted. NOTE: Deletes currently not implemented
    if (std::holds_alternative<NodeID>(edge.src)) {
        const NodeID srcID = std::get<NodeID>(edge.src);
        if (wb.deletedNodes().contains(srcID)) {
            return std::nullopt;
        }
    }
    if (std::holds_alternative<NodeID>(edge.tgt)) {
        const NodeID tgtID = std::get<NodeID>(edge.tgt);
        if (wb.deletedNodes().contains(tgtID)) {
            return std::nullopt;
        }
    }
    // Otherwise: source and target are either non-deleted existing nodes, or nodes
    // created in this commit
    const NodeID srcID =
        std::holds_alternative<NodeID>(edge.src)
            ? std::get<NodeID>(edge.src)
            : tempNodeIDMap.at(std::get<CommitWriteBuffer::PendingNodeOffset>(edge.src));

    const NodeID tgtID =
        std::holds_alternative<NodeID>(edge.tgt)
            ? std::get<NodeID>(edge.tgt)
            : tempNodeIDMap.at(std::get<CommitWriteBuffer::PendingNodeOffset>(edge.tgt));

    const std::string& edgeTypeName = edge.edgeLabelTypeName;
    const EdgeTypeID edgeTypeID = _metadata->getOrCreateEdgeType(edgeTypeName);

    const EdgeRecord newEdgeRecord = this->addEdge(edgeTypeID, srcID, tgtID);

    const EdgeID newEdgeID = newEdgeRecord._edgeID;

    for (const CWB::UntypedProperty& prop : edge.properties) {
        // Get the value type from the untyped property
        const ValueType propValueType = std::visit(ValueTypeFromProperty {}, prop.value);

        // Get the existing ID, or create a new property and get that ID
        const PropertyTypeID propID =
            _metadata->getOrCreatePropertyType(prop.propertyName, propValueType)._id;

        // Add this property to this edge in this builder
        std::visit(
            [&](const auto& propValue) {
                using T = std::decay_t<decltype(propValue)>;
                if constexpr (std::is_same_v<T, types::Int64::Primitive>) {
                    this->addEdgeProperty<types::Int64>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, propID, propValue);
                } else if constexpr (std::is_same_v<T, types::UInt64::Primitive>) {
                    this->addEdgeProperty<types::UInt64>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, propID, propValue);
                } else if constexpr (std::is_same_v<T, types::Double::Primitive>) {
                    this->addEdgeProperty<types::Double>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, propID, propValue);
                } else if constexpr (std::is_same_v<T, std::string>) {
                    this->addEdgeProperty<types::String>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, propID, propValue);
                } else if constexpr (std::is_same_v<T, types::Bool::Primitive>) {
                    this->addEdgeProperty<types::Bool>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, propID, propValue);
                }
            },
            prop.value);
    }

    return {};
}

#define INSTANTIATE(PType)                                                   \
    template void DataPartBuilder::addNodeProperty<PType>(NodeID,            \
                                                          PropertyTypeID,    \
                                                          PType::Primitive); \
    template void DataPartBuilder::addEdgeProperty<PType>(const EdgeRecord&, \
                                                          PropertyTypeID,    \
                                                          PType::Primitive);

INSTANTIATE(types::Int64);
INSTANTIATE(types::UInt64);
INSTANTIATE(types::Double);
INSTANTIATE(types::String);
INSTANTIATE(types::Bool);
