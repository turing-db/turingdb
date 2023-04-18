// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <map>

#include "StringIndex.h"
#include "StringRef.h"

namespace db {

class ValueType;
class Network;
class NodeType;
class ComponentType;
class EdgeType;
class Writeback;

class DB {
public:
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
    StringIndex _strIndex;
    std::map<StringRef, Network*, StringRef::Comparator> _networks;
    std::map<StringRef, NodeType*, StringRef::Comparator> _nodeTypes;
    std::map<StringRef, EdgeType*, StringRef::Comparator> _edgeTypes;
    std::map<StringRef, ComponentType*, StringRef::Comparator> _compTypes;

    ValueType* _int {nullptr};
    ValueType* _unsigned {nullptr};
    ValueType* _bool {nullptr};
    ValueType* _decimal {nullptr};
    ValueType* _string {nullptr};

    DB();
    void addNetwork(Network* net);
    void addNodeType(NodeType* nodeType);
    void addEdgeType(EdgeType* edgeType);
    void addComponentType(ComponentType* compType);
};

}
