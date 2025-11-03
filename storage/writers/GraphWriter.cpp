#include "GraphWriter.h"

#include "Graph.h"
#include "JobSystem.h"
#include "spdlog/spdlog.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Change.h"
#include "DataPartBuilder.h"
#include "writers/MetadataBuilder.h"
#include "versioning/Transaction.h"

using namespace db;

GraphWriter::GraphWriter(Graph* graph)
    : _graph(graph),
    _jobSystem(JobSystem::create())
{

    if (_graph) {
        _change = _graph->newChange();
        _commitBuilder = _change->access().getTip();
        _dataPartBuilder = &_commitBuilder->getCurrentBuilder();
    }
}

GraphWriter::~GraphWriter() {
}

bool GraphWriter::commit() {
    if (auto res = _change->access().commit(*_jobSystem); !res) {
        spdlog::error("Could not commit changes: {}", res.error().fmtMessage());
        return false;
    }

    _commitBuilder = _change->access().getTip();
    _dataPartBuilder = &_commitBuilder->getCurrentBuilder();

    return true;
}

bool GraphWriter::submit() {
    if (auto res = _change->access().submit(*_jobSystem); !res) {
        spdlog::error("Could not submit changes: {}", res.error().fmtMessage());
        return false;
    }

    if (!_graph) {
        return false;
    }

    if (_graph) {
        _change = _graph->newChange();
        _commitBuilder = _change->access().getTip();
        _dataPartBuilder = &_commitBuilder->getCurrentBuilder();
    }

    return true;
}

PendingCommitWriteTx GraphWriter::openWriteTransaction() {
    return _change->openWriteTransaction();
}

void GraphWriter::setName(const std::string& name) {
    _graph->_graphName = name;
}

NodeID GraphWriter::addNode(std::initializer_list<std::string_view> labels) {
    if (!_dataPartBuilder) {
        return {};
    }

    auto& metadata = _commitBuilder->metadata();

    LabelSet labelset;
    for (auto label : labels) {
        const LabelID id = metadata.getOrCreateLabel(std::string {label});
        labelset.set(id);
    }

    return _dataPartBuilder->addNode(labelset);
}

NodeID GraphWriter::addNode(std::initializer_list<LabelID> labels) {
    if (!_dataPartBuilder) {
        return {};
    }

    LabelSet labelset;
    for (auto label : labels) {
        labelset.set(label);
    }

    return _dataPartBuilder->addNode(labelset);
}

NodeID GraphWriter::addNode(const LabelSet& labelset) {
    if (!_dataPartBuilder) {
        return {};
    }

    return _dataPartBuilder->addNode(labelset);
}

NodeID GraphWriter::addNode(const LabelSetHandle& labelset) {
    if (!_dataPartBuilder) {
        return {};
    }

    return _dataPartBuilder->addNode(labelset);
}

void GraphWriter::deleteNode(NodeID id) {
    if (!_commitBuilder) {
        return;
    }

    _commitBuilder->writeBuffer().addDeletedNode(id);
    _commitBuilder->writeBuffer().addHangingEdges(openWriteTransaction().viewGraph());
}

void GraphWriter::deleteNodes(const std::vector<NodeID>& ids) {
    if (!_commitBuilder) {
        return;
    }

    _commitBuilder->writeBuffer().addDeletedNodes(ids);
    _commitBuilder->writeBuffer().addHangingEdges(openWriteTransaction().viewGraph());
}

EdgeRecord GraphWriter::addEdge(std::string_view edgeType, NodeID src, NodeID tgt) {
    if (!_dataPartBuilder) {
        return {};
    }

    auto& metadata = _commitBuilder->metadata();

    const EdgeTypeID edgeTypeID = metadata.getOrCreateEdgeType(std::string {edgeType});

    return _dataPartBuilder->addEdge(edgeTypeID, src, tgt);
}

EdgeRecord GraphWriter::addEdge(EdgeTypeID edgeType, NodeID src, NodeID tgt) {
    if (!_dataPartBuilder) {
        return {};
    }

    return _dataPartBuilder->addEdge(edgeType, src, tgt);
}

void GraphWriter::deleteEdge(EdgeID id) {
    if (!_commitBuilder) {
        return;
    }

    _commitBuilder->writeBuffer().addDeletedEdge(id);
}

void GraphWriter::deleteEdges(const std::vector<EdgeID>& ids) {
    if (!_commitBuilder) {
        return;
    }

    _commitBuilder->writeBuffer().addDeletedEdges(ids);
}

template <SupportedType T>
void GraphWriter::addNodeProperty(NodeID nodeID, std::string_view propertyTypeName, T::Primitive&& value) {
    if (!_dataPartBuilder) {
        return;
    }

    auto& metadata = _commitBuilder->metadata();
    const PropertyType propertyType = metadata.getOrCreatePropertyType(std::string {propertyTypeName}, T::_valueType);

    _dataPartBuilder->addNodeProperty<T>(nodeID, propertyType._id, std::move(value));
}

template <SupportedType T>
void GraphWriter::addNodeProperty(NodeID nodeID, PropertyType propertyType, T::Primitive&& value) {
    if (!_dataPartBuilder) {
        return;
    }

    _dataPartBuilder->addNodeProperty<T>(nodeID, propertyType._id, std::move(value));
}

template <SupportedType T>
void GraphWriter::addEdgeProperty(const EdgeRecord& edge, std::string_view propertyTypeName, T::Primitive&& value) {
    if (!_dataPartBuilder) {
        return;
    }

    auto& metadata = _commitBuilder->metadata();
    const PropertyType propertyType = metadata.getOrCreatePropertyType(std::string {propertyTypeName}, T::_valueType);

    _dataPartBuilder->addEdgeProperty<T>(edge, propertyType._id, std::move(value));
}

template <SupportedType T>
void GraphWriter::addEdgeProperty(const EdgeRecord& edge, PropertyType propertyType, T::Primitive&& value) {
    if (!_dataPartBuilder) {
        return;
    }

    _dataPartBuilder->addEdgeProperty<T>(edge, propertyType._id, std::move(value));
}

PropertyType GraphWriter::addPropertyType(const std::string& propTypeName, ValueType valueType) {
    auto& metadata = _commitBuilder->metadata();
    return metadata.getOrCreatePropertyType(propTypeName, valueType);
}

template void GraphWriter::addNodeProperty<types::Int64>(NodeID, std::string_view, types::Int64::Primitive&&);
template void GraphWriter::addNodeProperty<types::UInt64>(NodeID, std::string_view, types::UInt64::Primitive&&);
template void GraphWriter::addNodeProperty<types::Double>(NodeID, std::string_view, types::Double::Primitive&&);
template void GraphWriter::addNodeProperty<types::String>(NodeID, std::string_view, types::String::Primitive&&);
template void GraphWriter::addNodeProperty<types::Bool>(NodeID, std::string_view, types::Bool::Primitive&&);

template void GraphWriter::addNodeProperty<types::Int64>(NodeID, PropertyType, types::Int64::Primitive&&);
template void GraphWriter::addNodeProperty<types::UInt64>(NodeID, PropertyType, types::UInt64::Primitive&&);
template void GraphWriter::addNodeProperty<types::Double>(NodeID, PropertyType, types::Double::Primitive&&);
template void GraphWriter::addNodeProperty<types::String>(NodeID, PropertyType, types::String::Primitive&&);
template void GraphWriter::addNodeProperty<types::Bool>(NodeID, PropertyType, types::Bool::Primitive&&);

template void GraphWriter::addEdgeProperty<types::Int64>(const EdgeRecord&, std::string_view, types::Int64::Primitive&&);
template void GraphWriter::addEdgeProperty<types::UInt64>(const EdgeRecord&, std::string_view, types::UInt64::Primitive&&);
template void GraphWriter::addEdgeProperty<types::Double>(const EdgeRecord&, std::string_view, types::Double::Primitive&&);
template void GraphWriter::addEdgeProperty<types::String>(const EdgeRecord&, std::string_view, types::String::Primitive&&);
template void GraphWriter::addEdgeProperty<types::Bool>(const EdgeRecord&, std::string_view, types::Bool::Primitive&&);

template void GraphWriter::addEdgeProperty<types::Int64>(const EdgeRecord&, PropertyType, types::Int64::Primitive&&);
template void GraphWriter::addEdgeProperty<types::UInt64>(const EdgeRecord&, PropertyType, types::UInt64::Primitive&&);
template void GraphWriter::addEdgeProperty<types::Double>(const EdgeRecord&, PropertyType, types::Double::Primitive&&);
template void GraphWriter::addEdgeProperty<types::String>(const EdgeRecord&, PropertyType, types::String::Primitive&&);
template void GraphWriter::addEdgeProperty<types::Bool>(const EdgeRecord&, PropertyType, types::Bool::Primitive&&);
