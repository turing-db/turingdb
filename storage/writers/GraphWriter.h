#pragma once

#include <initializer_list>

#include "EdgeRecord.h"
#include "ID.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"

namespace db {

class Graph;
class JobSystem;
class DataPartBuilder;
class Change;
class CommitBuilder;
class PendingCommitWriteTx;

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

    PendingCommitWriteTx openWriteTransaction();

    void setName(const std::string& name);

    NodeID addNode(std::initializer_list<std::string_view> labels);
    NodeID addNode(std::initializer_list<LabelID> labels);
    NodeID addNode(const LabelSet& labelset);
    NodeID addNode(const LabelSetHandle& labelset);

    void deleteNode(NodeID id);
    void deleteNodes(const std::vector<NodeID>& ids);

    EdgeRecord addEdge(std::string_view edgeType, NodeID src, NodeID tgt);
    EdgeRecord addEdge(EdgeTypeID edgeType, NodeID src, NodeID tgt);

    void deleteEdge(EdgeID id);
    void deleteEdges(const std::vector<EdgeID>& ids);

    template <SupportedType T>
    void addNodeProperty(NodeID nodeID, std::string_view propertyTypeName, T::Primitive&& value);

    template <SupportedType T>
    void addNodeProperty(NodeID nodeID, PropertyType propertyType, T::Primitive&& value);

    template <SupportedType T>
    void addEdgeProperty(const EdgeRecord& edge, std::string_view propertyTypeName, T::Primitive&& value);

    template <SupportedType T>
    void addEdgeProperty(const EdgeRecord& edge, PropertyType propertyType, T::Primitive&& value);

    PropertyType addPropertyType(const std::string& propTypeName, ValueType valueType);

private:
    Graph* _graph {nullptr};
    std::unique_ptr<Change> _change;
    CommitBuilder* _commitBuilder {nullptr};
    DataPartBuilder* _dataPartBuilder {nullptr};
    std::unique_ptr<JobSystem> _jobSystem;
};

}
