#pragma once

#include "JsonParsingStats.h"
#include "StringRef.h"
#include "DBIndex.h"

#include <unordered_map>

namespace db {

class DB;
class Writeback;
class Network;
class NodeType;

}

class JsonParser;

class Neo4j4JsonParser {
private:
    Neo4j4JsonParser(db::DB* db, JsonParsingStats& stats);
    ~Neo4j4JsonParser();

    bool parseStats(const std::string& data);
    bool parseNodeProperties(const std::string& data);
    bool parseNodes(const std::string& data, const std::string& networkName);
    bool parseEdgeProperties(const std::string& data);
    bool parseEdges(const std::string& data);

    void setReducedOutput(bool value) { _reducedOutput = value; }

private:
    friend JsonParser;

    db::DB* _db {nullptr};
    JsonParsingStats& _stats;
    db::Writeback* _wb {nullptr};
    std::unordered_map<size_t, db::DBIndex> _nodeIdMap;
    bool _reducedOutput = false;

    db::Network* getOrCreateNetwork(const std::string& netName);
    db::NodeType* getOrCreateNodeType(db::StringRef name);
};
