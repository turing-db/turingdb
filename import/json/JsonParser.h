#pragma once

#include "versioning/Transaction.h"
#include "writers/DataPartBuilder.h"

namespace db {

class Graph;
class PropertyTypeMap;
class LabelMap;
class EdgeTypeMap;
class IDMapper;
class JobSystem;

struct GraphStats {
    size_t nodeCount = 0;
    size_t edgeCount = 0;
};

class JsonParser {
public:
    enum class FileType {
        Neo4j_4_Stats = 0,
        Neo4j_4_NodeProperties,
        Neo4j_4_EdgeProperties,
        Neo4j_4_Nodes,
        Neo4j_4_Edges,
    };

    explicit JsonParser(Graph* graph);
    JsonParser(const JsonParser&) = delete;
    JsonParser(JsonParser&&) = delete;
    JsonParser& operator=(const JsonParser&) = delete;
    JsonParser& operator=(JsonParser&&) = delete;
    ~JsonParser();

    void buildPending(JobSystem& jobSystem);
    GraphStats parseStats(const std::string& data);
    bool parseNodeLabels(const std::string& data);
    bool parseNodeLabelSets(const std::string& data);
    bool parseNodeProperties(const std::string& data);
    bool parseEdgeTypes(const std::string& data);
    bool parseEdgeProperties(const std::string& data);
    bool parseNodes(const std::string& data, DataPartBuilder&);
    bool parseEdges(const std::string& data, DataPartBuilder&);

    DataPartBuilder& newDataBuffer();
    void commit(Graph& graph, JobSystem& jobSystem);

private:
    Graph* _graph {nullptr};
    WriteTransaction _transaction;
    std::unique_ptr<CommitBuilder> _commitBuilder;
    GraphMetadata* _graphMetadata {nullptr};
    std::unique_ptr<IDMapper> _nodeIDMapper;
};

}
