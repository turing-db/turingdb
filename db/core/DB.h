// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <map>

#include "StringIndex.h"
#include "StringRef.h"
#include "DBIndex.h"

namespace db {

class Network;
class NodeType;
class EdgeType;
class DBNetworkRange;
class DBNodeTypeRange;
class DBEdgeTypeRange;
class Writeback;
class DBLoader;
class DBDumper;

class DB {
public:
    friend DBNetworkRange;
    friend DBNodeTypeRange;
    friend DBEdgeTypeRange;
    friend Writeback;
    friend DBLoader;
    friend DBDumper;

    ~DB();

    static DB* create();

    StringRef getString(const std::string& str);

    NodeType* getNodeType(StringRef name) const;
    EdgeType* getEdgeType(StringRef name) const;

    Network* getNetwork(StringRef name) const;

private:
    using Networks = std::map<StringRef, Network*>;
    using NodeTypes = std::map<StringRef, NodeType*>;
    using EdgeTypes = std::map<StringRef, EdgeType*>;

    StringIndex _strIndex;
    Networks _networks;
    NodeTypes _nodeTypes;
    EdgeTypes _edgeTypes;

    DBIndex::ID _nextFreeNetID {0};
    DBIndex::ID _nextFreeNodeID {0};
    DBIndex::ID _nextFreeEdgeID {0};
    DBIndex::ID _nextFreeNodeTypeID {0};
    DBIndex::ID _nextFreeEdgeTypeID {0};
    DBIndex::ID _nextFreePropertyTypeID {0};

    DB();
    void addNetwork(Network* net);
    void addNodeType(NodeType* nodeType);
    void addEdgeType(EdgeType* edgeType);
    DBIndex allocNetworkIndex();
    DBIndex allocNodeIndex();
    DBIndex allocEdgeIndex();
    DBIndex allocNodeTypeIndex();
    DBIndex allocEdgeTypeIndex();
    DBIndex allocPropertyTypeIndex();
};

}
