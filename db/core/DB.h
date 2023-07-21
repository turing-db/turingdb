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
class EntityDumper;
class Node;
class Edge;

class DB {
public:
    friend Writeback;
    friend EntityDumper;
    friend DBLoader;
    friend DBDumper;
    friend DBComparator;
    using Networks = std::map<StringRef, Network*>;
    using NodeTypes = std::map<StringRef, NodeType*>;
    using EdgeTypes = std::map<StringRef, EdgeType*>;
    using Nodes =  std::map<DBIndex, Node*>;
    using Edges =  std::map<DBIndex, Edge*>;
    using NetworkRange = STLIndexRange<Networks>;
    using NodeTypeRange = STLIndexRange<NodeTypes>;
    using EdgeTypeRange = STLIndexRange<EdgeTypes>;
    using NodeRange = STLRange<Nodes>;
    using EdgeRange = STLRange<Edges>;

    ~DB();

    static DB* create();

    StringRef getString(const std::string& str);
    StringRef lookupString(const std::string& str) const;

    NetworkRange networks() const;
    NodeTypeRange nodeTypes() const;
    EdgeTypeRange edgeTypes() const;

    NodeType* getNodeType(StringRef name) const;
    NodeType* getNodeType(DBIndex id) const;
    EdgeType* getEdgeType(StringRef name) const;
    EdgeType* getEdgeType(DBIndex id) const;

    Node* getNode(DBIndex id) const;
    Edge* getEdge(DBIndex id) const;
    size_t getNodeCount() const;
    size_t getEdgeCount() const;
    NodeRange nodes() const;
    EdgeRange edges() const;

    Network* getNetwork(StringRef name) const;
    Network* getNetwork(DBIndex id) const;

private:
    StringIndex _strIndex;
    Networks _networks;
    NodeTypes _nodeTypes;
    EdgeTypes _edgeTypes;
    Nodes _nodes;
    Edges _edges;

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
    void addNode(Node* node);
    void addEdge(Edge* edge);
    DBIndex allocNetworkIndex();
    DBIndex allocNodeIndex();
    DBIndex allocEdgeIndex();
    DBIndex allocNodeTypeIndex();
    DBIndex allocEdgeTypeIndex();
    DBIndex allocPropertyTypeIndex();
    DBIndex allocPropertyTypeIndexFromExisting(DBIndex::ID id);
};

}
