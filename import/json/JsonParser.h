#pragma once

#include <stddef.h>
#include <string>
#include <memory>

namespace db {

class PropertyTypeMap;
class LabelMap;
class EdgeTypeMap;
class IDMapper;
class JobSystem;
class ChangeAccessor;

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

    JsonParser();
    ~JsonParser();

    JsonParser(const JsonParser&) = delete;
    JsonParser(JsonParser&&) = delete;
    JsonParser& operator=(const JsonParser&) = delete;
    JsonParser& operator=(JsonParser&&) = delete;

    GraphStats parseStats(const std::string& data);
    bool parseNodeLabels(ChangeAccessor& change, const std::string& data);
    bool parseNodeLabelSets(ChangeAccessor& change, const std::string& data);
    bool parseNodeProperties(ChangeAccessor& change, const std::string& data);
    bool parseEdgeTypes(ChangeAccessor& change, const std::string& data);
    bool parseEdgeProperties(ChangeAccessor& change, const std::string& data);
    bool parseNodes(ChangeAccessor& change, const std::string& data);
    bool parseEdges(ChangeAccessor& change, const std::string& data);

private:
    std::unique_ptr<IDMapper> _nodeIDMapper;
};

}
