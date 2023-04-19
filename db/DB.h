// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <map>

#include "StringIndex.h"
#include "StringRef.h"
#include "DBIndex.h"

namespace db {

class ValueType;
class Network;
class NodeType;
class ComponentType;
class EdgeType;
class DBNetworkRange;
class Writeback;

class DB {
public:
    friend DBNetworkRange;
    friend Writeback;

    ~DB();

    static DB* create();

    StringRef getString(const std::string& str);

    // Value types
    ValueType* getIntType() const { return _int; }
    ValueType* getUnsignedType() const { return _unsigned; }
    ValueType* getBoolType() const { return _bool; }
    ValueType* getDecimalType() const { return _decimal; }
    ValueType* getStringType() const { return _string; }

    NodeType* getNodeType(StringRef name) const;
    EdgeType* getEdgeType(StringRef name) const;
    ComponentType* getComponentType(StringRef name) const;

    Network* getNetwork(StringRef name) const;

private:
    using Networks = std::map<StringRef, Network*>;

    StringIndex _strIndex;
    DBIndex::ID _nextFreeNetID {0};
    Networks _networks;
    std::map<StringRef, NodeType*> _nodeTypes;
    std::map<StringRef, EdgeType*> _edgeTypes;
    std::map<StringRef, ComponentType*> _compTypes;

    ValueType* _int {nullptr};
    ValueType* _unsigned {nullptr};
    ValueType* _bool {nullptr};
    ValueType* _decimal {nullptr};
    ValueType* _string {nullptr};

    DB();
    DBIndex::ID allocNetworkID();
    void addNetwork(Network* net);
    void addNodeType(NodeType* nodeType);
    void addEdgeType(EdgeType* edgeType);
    void addComponentType(ComponentType* compType);
};

}
