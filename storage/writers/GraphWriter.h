#pragma once

#include <initializer_list>

#include "EdgeRecord.h"
#include "EntityID.h"
#include "labels/LabelSet.h"
#include "types/SupportedType.h"
#include "versioning/Transaction.h"

namespace db {

class Graph;
class JobSystem;
class DataPartBuilder;

class GraphWriter {
public:
    explicit GraphWriter(Graph* graph);
    ~GraphWriter();

    GraphWriter(const GraphWriter&) = delete;
    GraphWriter(GraphWriter&&) = delete;
    GraphWriter& operator=(const GraphWriter&) = delete;
    GraphWriter& operator=(GraphWriter&&) = delete;

    void commit();

    EntityID addNode(std::initializer_list<std::string_view> labels);
    EntityID addNode(std::initializer_list<LabelID> labels);
    EntityID addNode(const LabelSet& labelset);
    EntityID addNode(LabelSetID labelsetID);

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
    WriteTransaction _tx;
    std::unique_ptr<CommitBuilder> _commitBuilder;
    DataPartBuilder* _dataPartBuilder = nullptr;
    std::unique_ptr<JobSystem> _jobSystem;
};

}
