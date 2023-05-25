// Copyright 2023 Turing Biosystems Ltd.

#include "DB.h"

#include "Network.h"
#include "NodeType.h"
#include "EdgeType.h"

using namespace db;

DB::DB()
{
}

DB::~DB() {
}

DB* DB::create() {
    DB* db = new DB();
    return db;
}

StringRef DB::getString(const std::string& str) {
    return _strIndex.getString(str);
}

DBIndex DB::allocNetworkIndex() {
    const DBIndex::ID netID = _nextFreeNetID;
    _nextFreeNetID++;
    return DBIndex(netID);
}

DBIndex DB::allocNodeIndex() {
    const DBIndex::ID nodeID = _nextFreeNodeID;
    _nextFreeNodeID++;
    return DBIndex(nodeID);
}

DBIndex DB::allocEdgeIndex() {
    const DBIndex::ID edgeID = _nextFreeEdgeID;
    _nextFreeEdgeID++;
    return DBIndex(edgeID);
}

DBIndex DB::allocNodeTypeIndex() {
    const DBIndex::ID nodeTypeID = _nextFreeNodeTypeID;
    _nextFreeNodeTypeID++;
    return DBIndex(nodeTypeID);
}

DBIndex DB::allocEdgeTypeIndex() {
    const DBIndex::ID edgeTypeID = _nextFreeEdgeTypeID;
    _nextFreeEdgeTypeID++;
    return DBIndex(edgeTypeID);
}

DBIndex DB::allocPropertyTypeIndex() {
    const DBIndex::ID propertyTypeID = _nextFreePropertyTypeID;
    _nextFreePropertyTypeID++;
    return DBIndex(propertyTypeID);
}

void DB::addNetwork(Network* net) {
    _networks[net->getName()] = net;
}

Network* DB::getNetwork(StringRef name) const {
    const auto foundIt = _networks.find(name);
    if (foundIt == _networks.end()) {
        return nullptr;
    }

    return foundIt->second;
}

NodeType* DB::getNodeType(StringRef name) const {
    const auto foundIt = _nodeTypes.find(name);
    if (foundIt == _nodeTypes.end()) {
        return nullptr;
    }

    return foundIt->second;
}

EdgeType* DB::getEdgeType(StringRef name) const {
    const auto foundIt = _edgeTypes.find(name);
    if (foundIt == _edgeTypes.end()) {
        return nullptr;
    }

    return foundIt->second;
}

void DB::addNodeType(NodeType* nodeType) {
    _nodeTypes[nodeType->getName()] = nodeType;
}

void DB::addEdgeType(EdgeType* edgeType) {
    _edgeTypes[edgeType->getName()] = edgeType;
}
