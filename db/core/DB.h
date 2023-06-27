// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <map>

#include "StringIndex.h"
#include "StringRef.h"
#include "DBIndex.h"
#include "Range.h"
#include "Comparator.h"

namespace db {

class Network;
class NodeType;
class EdgeType;
class Writeback;
class DBLoader;
class DBDumper;

class DB {
public:
    friend Writeback;
    friend DBLoader;
    friend DBDumper;
    friend DBComparator;
    using Networks = std::map<StringRef, Network*>;
    using NodeTypes = std::map<StringRef, NodeType*>;
    using EdgeTypes = std::map<StringRef, EdgeType*>;
    using NetworkRange = STLIndexRange<Networks>;
    using NodeTypeRange = STLIndexRange<NodeTypes>;
    using EdgeTypeRange = STLIndexRange<EdgeTypes>;

    ~DB();

    static DB* create();

    StringRef getString(const std::string& str);

    NetworkRange networks() const;
    NodeTypeRange nodeTypes() const;
    EdgeTypeRange edgeTypes() const;

    NodeType* getNodeType(StringRef name) const;
    EdgeType* getEdgeType(StringRef name) const;

    Network* getNetwork(StringRef name) const;

private:
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
    DBIndex allocPropertyTypeIndexFromExisting(DBIndex::ID id);
};

}
