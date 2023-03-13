// Copyright 2023 Turing Biosystems Ltd.

#include "DB.h"

#include "Network.h"
#include "ValueType.h"
#include "Edge.h"
#include "NodeType.h"
#include "EdgeType.h"

using namespace db;

DB::DB()
{
    _int = new ValueType(ValueType::VK_INT);
    _unsigned = new ValueType(ValueType::VK_UNSIGNED);
    _bool = new ValueType(ValueType::VK_BOOL);
    _decimal = new ValueType(ValueType::VK_DECIMAL);
    _string = new ValueType(ValueType::VK_STRING);
}

DB::~DB() {
    delete _int;
    delete _unsigned;
    delete _bool;
    delete _decimal;
    delete _string;
}

DB* DB::create() {
    DB* db = new DB();
    return db;
}

StringRef DB::getString(const std::string& str) {
    return _strIndex.getString(str);
}

void DB::addNetwork(Network* net) {
    _networks.push_back(net);
}

void DB::addEdge(Edge* link) {
    _edges.push_back(link);
}

void DB::addNodeType(NodeType* nodeType) {
    _nodeTypes.push_back(nodeType);
}

void DB::addEdgeType(EdgeType* edgeType) {
    _edgeTypes.push_back(edgeType);
}
