#include "GraphWriter.h"

#include "Graph.h"
#include "JobSystem.h"
#include "versioning/CommitBuilder.h"
#include "DataPartBuilder.h"
#include "reader/GraphReader.h"

using namespace db;

GraphWriter::GraphWriter(Graph* graph)
    : _graph(graph),
      _jobSystem(JobSystem::create())
{

    if (_graph) {
        _tx = _graph->openWriteTransaction();
        _commitBuilder = _tx.prepareCommit();
        _dataPartBuilder = &_commitBuilder->newBuilder();
    }
}

GraphWriter::~GraphWriter() {
}

void GraphWriter::commit() {
    if (!_commitBuilder) {
        return;
    }

    _graph->commit(std::move(_commitBuilder), *_jobSystem);
    _tx = _graph->openWriteTransaction();
    _commitBuilder = _tx.prepareCommit();
    _dataPartBuilder = &_commitBuilder->newBuilder();
}

EntityID GraphWriter::addNode(std::initializer_list<std::string_view> labels) {
    if (!_dataPartBuilder) {
        return {};
    }

    auto reader = _tx.readGraph();

    auto& metadata = reader.getMetadata();
    auto& labelMap = metadata.labels();

    LabelSet labelset;
    for (auto label : labels) {
        const LabelID id = labelMap.getOrCreate(std::string {label});
        labelset.set(id);
    }

    const LabelSetID labelsetID = metadata.labelsets().getOrCreate(labelset);

    return _dataPartBuilder->addNode(labelsetID);
}

EntityID GraphWriter::addNode(std::initializer_list<LabelID> labels) {
    if (!_dataPartBuilder) {
        return {};
    }

    auto reader = _tx.readGraph();

    auto& metadata = reader.getMetadata();

    LabelSet labelset;
    for (auto label : labels) {
        labelset.set(label);
    }

    const LabelSetID labelsetID = metadata.labelsets().getOrCreate(labelset);

    return _dataPartBuilder->addNode(labelsetID);
}

EntityID GraphWriter::addNode(const LabelSet& labelset) {
    if (!_dataPartBuilder) {
        return {};
    }

    auto reader = _tx.readGraph();
    auto& metadata = reader.getMetadata();

    const LabelSetID labelsetID = metadata.labelsets().getOrCreate(labelset);

    return _dataPartBuilder->addNode(labelsetID);
}

EntityID GraphWriter::addNode(LabelSetID labelsetID) {
    if (!_dataPartBuilder) {
        return {};
    }

    return _dataPartBuilder->addNode(labelsetID);
}

EdgeRecord GraphWriter::addEdge(std::string_view edgeType, EntityID src, EntityID tgt) {
    if (!_dataPartBuilder) {
        return {};
    }

    auto reader = _tx.readGraph();
    auto& metadata = reader.getMetadata();

    const EdgeTypeID edgeTypeID = metadata.edgeTypes().getOrCreate(std::string {edgeType});

    return _dataPartBuilder->addEdge(edgeTypeID, src, tgt);
}

EdgeRecord GraphWriter::addEdge(EdgeTypeID edgeType, EntityID src, EntityID tgt) {
    if (!_dataPartBuilder) {
        return {};
    }

    return _dataPartBuilder->addEdge(edgeType, src, tgt);
}

template <SupportedType T>
void GraphWriter::addNodeProperty(EntityID nodeID, std::string_view propertyTypeName, T::Primitive&& value) {
    if (!_dataPartBuilder) {
        return;
    }

    auto reader = _tx.readGraph();
    auto& metadata = reader.getMetadata();
    const PropertyType propertyType = metadata.propTypes().getOrCreate(std::string {propertyTypeName}, T::_valueType);

    _dataPartBuilder->addNodeProperty<T>(nodeID, propertyType._id, std::move(value));
}

template <SupportedType T>
void GraphWriter::addNodeProperty(EntityID nodeID, PropertyType propertyType, T::Primitive&& value) {
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

    auto reader = _tx.readGraph();
    auto& metadata = reader.getMetadata();
    const PropertyType propertyType = metadata.propTypes().getOrCreate(std::string {propertyTypeName}, T::_valueType);

    _dataPartBuilder->addEdgeProperty<T>(edge, propertyType._id, std::move(value));
}

template <SupportedType T>
void GraphWriter::addEdgeProperty(const EdgeRecord& edge, PropertyType propertyType, T::Primitive&& value) {
    if (!_dataPartBuilder) {
        return;
    }

    _dataPartBuilder->addEdgeProperty<T>(edge, propertyType._id, std::move(value));
}

template void GraphWriter::addNodeProperty<types::Int64>(EntityID, std::string_view, types::Int64::Primitive&&);
template void GraphWriter::addNodeProperty<types::UInt64>(EntityID, std::string_view, types::UInt64::Primitive&&);
template void GraphWriter::addNodeProperty<types::Double>(EntityID, std::string_view, types::Double::Primitive&&);
template void GraphWriter::addNodeProperty<types::String>(EntityID, std::string_view, types::String::Primitive&&);
template void GraphWriter::addNodeProperty<types::Bool>(EntityID, std::string_view, types::Bool::Primitive&&);

template void GraphWriter::addNodeProperty<types::Int64>(EntityID, PropertyType, types::Int64::Primitive&&);
template void GraphWriter::addNodeProperty<types::UInt64>(EntityID, PropertyType, types::UInt64::Primitive&&);
template void GraphWriter::addNodeProperty<types::Double>(EntityID, PropertyType, types::Double::Primitive&&);
template void GraphWriter::addNodeProperty<types::String>(EntityID, PropertyType, types::String::Primitive&&);
template void GraphWriter::addNodeProperty<types::Bool>(EntityID, PropertyType, types::Bool::Primitive&&);

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
