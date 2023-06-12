#include "Writeback.h"

#include "DB.h"
#include "Network.h"
#include "Node.h"
#include "NodeType.h"
#include "Property.h"
#include "Edge.h"
#include "EdgeType.h"
#include "PropertyType.h"
#include "TypeCheck.h"

using namespace db;

Writeback::Writeback(DB* db)
    : _db(db)
{
}

Writeback::~Writeback() {
}

Network* Writeback::createNetwork(StringRef name) {
    if (_db->getNetwork(name)) {
        return nullptr;
    }

    const DBIndex netIndex = _db->allocNetworkIndex();
    Network* net = new Network(netIndex, name);
    _db->addNetwork(net);
    return net;
}

Node* Writeback::createNode(Network* net, NodeType* type) {
    return createNode(net, type, StringRef());
}

Node* Writeback::createNode(Network* net, NodeType* type, StringRef name) {
    if (!net || !type) {
        return nullptr;
    }

    const DBIndex nodeIndex = _db->allocNodeIndex();
    Node* node = new Node(nodeIndex, type, net);
    node->setName(name);
    net->addNode(node);

    return node;
}

Edge* Writeback::createEdge(EdgeType* type, Node* source, Node* target) {
    if (!type || !source || !target) {
        return nullptr;
    }

    TypeCheck typeCheck(_db);
    if (!typeCheck.checkEdge(type, source, target)) {
        return nullptr;
    }

    const DBIndex edgeIndex = _db->allocEdgeIndex();
    Edge* edge = new Edge(edgeIndex, type, source, target);

    source->addOutEdge(edge);
    target->addInEdge(edge);

    Network* sourceNet = source->getNetwork();
    Network* targetNet = target->getNetwork();
    sourceNet->addEdge(edge);
    if (sourceNet != targetNet) {
        targetNet->addEdge(edge);
    }
    
    return edge;
}

NodeType* Writeback::createNodeType(StringRef name) {
    if (_db->getNodeType(name)) {
        return nullptr;
    }

    const DBIndex typeIndex = _db->allocNodeTypeIndex();
    NodeType* nodeType = new NodeType(typeIndex, name);
    _db->addNodeType(nodeType);

    return nodeType;
}

EdgeType* Writeback::createEdgeType(StringRef name, NodeType* source, NodeType* target) {
    return createEdgeType(name, std::vector{source}, std::vector{target});
}

EdgeType* Writeback::createEdgeType(StringRef name,
                                    const NodeTypes& sources,
                                    const NodeTypes& targets) {
    if (_db->getEdgeType(name)) {
        return nullptr;
    }

    if (sources.empty() || targets.empty()) {
        return nullptr;
    }

    const DBIndex typeIndex = _db->allocEdgeTypeIndex();
    EdgeType* edgeType = new EdgeType(typeIndex, name, sources, targets);
    _db->addEdgeType(edgeType);

    return edgeType;
}

PropertyType* Writeback::addPropertyType(NodeType* nodeType,
                                         StringRef name,
                                         ValueType type) {
    return addPropertyTypeBase(nodeType, name, type);
}

PropertyType* Writeback::addPropertyType(EdgeType* edgeType,
                                         StringRef name,
                                         ValueType type) {
    return addPropertyTypeBase(edgeType, name, type);
}

PropertyType* Writeback::addPropertyTypeBase(DBEntityType* dbType,
                                             StringRef name,
                                             ValueType type) {
    if (!dbType || !type.isValid()) {
        return nullptr;
    }

    if (dbType->getPropertyType(name)) {
        return nullptr;
    }

    const DBIndex propertyTypeIndex = _db->allocPropertyTypeIndex();
    PropertyType* propType = new PropertyType(propertyTypeIndex, dbType, name, type);
    dbType->addPropertyType(propType);
    return propType;
}

bool Writeback::addSourceNodeType(EdgeType* edgeType, NodeType* nodeType) {
    if (!edgeType) {
        return false;
    }

    if (edgeType->hasSourceType(nodeType)) {
        return false;
    }

    edgeType->addSourceType(nodeType);
    return true;
}

bool Writeback::addTargetNodeType(EdgeType* edgeType, NodeType* nodeType) {
    if (!edgeType) {
        return false;
    }

    if (edgeType->hasTargetType(nodeType)) {
        return false;
    }

    edgeType->addTargetType(nodeType);
    return true;
}

bool Writeback::setProperty(DBEntity* entity, const Property& prop) {
    if (!entity) {
        return false;
    }

    if (!prop.isValid()) {
        return false;
    }

    TypeCheck typeCheck(_db);
    if (!typeCheck.checkEntityProperty(entity, prop)) {
        return false;
    }

    entity->addProperty(prop);
    return true;
}
