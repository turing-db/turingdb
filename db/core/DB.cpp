// Copyright 2023 Turing Biosystems Ltd.

#include "DB.h"

#include "Node.h"
#include "Edge.h"
#include "Network.h"
#include "NodeType.h"
#include "EdgeType.h"

using namespace db;

DB::DB()
{
}

DB::~DB() {
    for (const auto& netEntry : _networks) {
        delete netEntry.second;
    }
    _networks.clear();

    for (const auto& nodeTypeEntry : _nodeTypes) {
        delete nodeTypeEntry.second;
    }
    _nodeTypes.clear();

    for (const auto& edgeTypeEntry : _edgeTypes) {
        delete edgeTypeEntry.second;
    }
    _edgeTypes.clear();

    for (const auto& nodeEntry : _nodes) {
        delete nodeEntry.second;
    }

    for (const auto& edgeEntry : _edges) {
        delete edgeEntry.second;
    }
}

DB* DB::create() {
    DB* db = new DB();
    return db;
}

StringRef DB::getString(const std::string& str) {
    return _strIndex.getString(str);
}

StringRef DB::lookupString(const std::string& str) const {
    return _strIndex.lookupString(str);
}

DB::NetworkRange DB::networks() const {
    return NetworkRange(&_networks);
}

DB::NodeTypeRange DB::nodeTypes() const {
    return NodeTypeRange(&_nodeTypes);
}

DB::EdgeTypeRange DB::edgeTypes() const {
    return EdgeTypeRange(&_edgeTypes);
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

DBIndex DB::allocPropertyTypeIndexFromExisting(DBIndex::ID id) {
    if (id >= _nextFreePropertyTypeID) {
        _nextFreePropertyTypeID = id + 1;
    }
    return DBIndex(id);
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

Network* DB::getNetwork(DBIndex id) const {
    const StringRef netName = _strIndex.getString(id);
    return getNetwork(netName);
}

NodeType* DB::getNodeType(StringRef name) const {
    const auto foundIt = _nodeTypes.find(name);
    if (foundIt == _nodeTypes.end()) {
        return nullptr;
    }

    return foundIt->second;
}

NodeType* DB::getNodeType(DBIndex id) const {
    const StringRef ntName = _strIndex.getString(id);
    return getNodeType(ntName);
}

EdgeType* DB::getEdgeType(StringRef name) const {
    const auto foundIt = _edgeTypes.find(name);
    if (foundIt == _edgeTypes.end()) {
        return nullptr;
    }

    return foundIt->second;
}

EdgeType* DB::getEdgeType(DBIndex id) const {
    const StringRef etName = _strIndex.getString(id);
    return getEdgeType(etName);
}

Node* DB::getNode(DBIndex id) const {
    const auto foundIt = _nodes.find(id);
    if (foundIt == _nodes.end()) {
        return nullptr;
    }
    return foundIt->second;
}

Edge* DB::getEdge(DBIndex id) const {
    const auto foundIt = _edges.find(id);
    if (foundIt == _edges.end()) {
        return nullptr;
    }
    return foundIt->second;
}

size_t DB::getNodeCount() const {
    return _nodes.size();
}

size_t DB::getEdgeCount() const {
    return _edges.size();
}

DB::NodeRange DB::nodes() const {
    return NodeRange(&_nodes);
}

DB::EdgeRange DB::edges() const {
    return EdgeRange(&_edges);
}

void DB::addNodeType(NodeType* nodeType) {
    _nodeTypes[nodeType->getName()] = nodeType;
}

void DB::addEdgeType(EdgeType* edgeType) {
    _edgeTypes[edgeType->getName()] = edgeType;
}

void DB::addNode(Node* node) {
    _nodes.emplace(node->getIndex(), node);
}

void DB::addEdge(Edge* edge) {
    _edges[edge->getIndex()] = edge;
}
