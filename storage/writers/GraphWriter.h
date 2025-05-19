#pragma once

#include <initializer_list>

#include "EdgeRecord.h"
#include "EntityID.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/SupportedType.h"

namespace db {

class Graph;
class JobSystem;
class DataPartBuilder;
class Change;
class CommitBuilder;

class GraphWriter {
public:
    explicit GraphWriter(Graph* graph);
    ~GraphWriter();

    GraphWriter(const GraphWriter&) = delete;
    GraphWriter(GraphWriter&&) = delete;
    GraphWriter& operator=(const GraphWriter&) = delete;
    GraphWriter& operator=(GraphWriter&&) = delete;

    bool commit();
    bool submit();

    EntityID addNode(std::initializer_list<std::string_view> labels);
    EntityID addNode(std::initializer_list<LabelID> labels);
    EntityID addNode(const LabelSet& labelset);
    EntityID addNode(const LabelSetHandle& labelset);

    EdgeRecord addEdge(std::string_view edgeType, EntityID src, EntityID tgt);
    EdgeRecord addEdge(EdgeTypeID edgeType, EntityID src, EntityID tgt);

    template<SupportedType T>
    void addNodeProperty(EntityID nodeID, std::string_view propertyTypeName, T::Primitive&& value);

    template<SupportedType T>
    void addNodeProperty(EntityID nodeID, PropertyType propertyType, T::Primitive&& value);

    template<SupportedType T>
    void addEdgeProperty(const EdgeRecord& edge, std::string_view propertyTypeName, T::Primitive&& value);

    template<SupportedType T>
    void addEdgeProperty(const EdgeRecord& edge, PropertyType propertyType, T::Primitive&& value);

private:
    Graph* _graph {nullptr};
    std::unique_ptr<Change> _change;
    CommitBuilder* _commitBuilder {nullptr};
    DataPartBuilder* _dataPartBuilder {nullptr};
    std::unique_ptr<JobSystem> _jobSystem;
};

}
